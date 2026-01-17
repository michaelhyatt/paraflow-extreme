//! pf-transform CLI - Test and debug Rhai transforms.
//!
//! This tool enables testing transforms using stdin/stdout without
//! requiring the full worker pipeline.
//!
//! # Usage
//!
//! ```bash
//! # Basic transform: read NDJSON from stdin, apply script, output to stdout
//! cat data.ndjson | pf-transform --script 'record.new_field = 42; record'
//!
//! # Transform with script file
//! cat data.ndjson | pf-transform --script-file ./transforms/enrich.rhai
//!
//! # With enrichment tables
//! cat data.ndjson | pf-transform \
//!   --script-file ./transforms/enrich.rhai \
//!   --enrichment geo_ip:cidr:./tables/geo.csv \
//!   --enrichment users:exact:./tables/users.csv
//!
//! # Validate script syntax without processing data
//! pf-transform --script-file ./transform.rhai --validate
//! ```

use anyhow::{Context, Result, bail};
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::json::ReaderBuilder;
use clap::Parser;
use pf_enrichment::{EnrichmentRegistry, EnrichmentTableConfig, MatchType};
use pf_traits::Transform;
use pf_transform::{ErrorPolicy, RhaiTransform, TransformConfig};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{Level, debug};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
#[command(name = "pf-transform")]
#[command(about = "Test and debug Rhai transforms")]
#[command(version)]
struct Cli {
    /// Inline Rhai script
    #[arg(short = 's', long)]
    script: Option<String>,

    /// Path to Rhai script file
    #[arg(short = 'S', long)]
    script_file: Option<PathBuf>,

    /// Enrichment table: name:type:path (can be repeated)
    /// type is 'exact' or 'cidr'
    #[arg(short = 'e', long, value_parser = parse_enrichment_spec)]
    enrichment: Vec<EnrichmentTableConfig>,

    /// Error policy: drop, fail, passthrough
    #[arg(long, default_value = "drop", value_parser = parse_error_policy)]
    error_policy: ErrorPolicy,

    /// Process only first N records
    #[arg(short = 'n', long)]
    limit: Option<usize>,

    /// Validate script syntax and exit
    #[arg(long)]
    validate: bool,

    /// Print transform statistics to stderr
    #[arg(long)]
    stats: bool,

    /// Pretty-print JSON output (for debugging)
    #[arg(long)]
    pretty: bool,

    /// Verbose output
    #[arg(short = 'v', long)]
    verbose: bool,
}

/// Statistics collected during transformation.
#[derive(Debug, Default)]
struct TransformStats {
    records_in: usize,
    records_out: usize,
    records_filtered: usize,
    records_errored: usize,
}

fn parse_enrichment_spec(s: &str) -> Result<EnrichmentTableConfig, String> {
    let parts: Vec<&str> = s.splitn(3, ':').collect();
    if parts.len() != 3 {
        return Err(format!(
            "Invalid enrichment spec: '{}'. Expected format: name:type:path",
            s
        ));
    }

    let name = parts[0].to_string();
    let match_type = match parts[1].to_lowercase().as_str() {
        "exact" => MatchType::Exact,
        "cidr" => MatchType::Cidr,
        other => {
            return Err(format!(
                "Invalid match type: '{}'. Use 'exact' or 'cidr'",
                other
            ));
        }
    };
    let source = parts[2].to_string();

    // Infer key field from first column (will be overridden during load)
    let key_field = match match_type {
        MatchType::Cidr => "cidr".to_string(),
        MatchType::Exact => name.clone(),
    };

    Ok(EnrichmentTableConfig {
        name,
        source,
        match_type,
        key_field,
    })
}

fn parse_error_policy(s: &str) -> Result<ErrorPolicy, String> {
    match s.to_lowercase().as_str() {
        "drop" => Ok(ErrorPolicy::Drop),
        "fail" => Ok(ErrorPolicy::Fail),
        "passthrough" => Ok(ErrorPolicy::Passthrough),
        other => Err(format!(
            "Invalid error policy: '{}'. Use 'drop', 'fail', or 'passthrough'",
            other
        )),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Setup logging
    let level = if cli.verbose {
        Level::DEBUG
    } else {
        Level::WARN
    };
    let subscriber = FmtSubscriber::builder()
        .with_max_level(level)
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber).context("Failed to set logger")?;

    // Get script content
    let script = match (&cli.script, &cli.script_file) {
        (Some(s), None) => s.clone(),
        (None, Some(path)) => tokio::fs::read_to_string(path)
            .await
            .context(format!("Failed to read script file: {}", path.display()))?,
        (Some(_), Some(_)) => bail!("Cannot specify both --script and --script-file"),
        (None, None) => bail!("Specify either --script or --script-file"),
    };

    // Validate only mode
    if cli.validate {
        let engine = rhai::Engine::new();
        match engine.compile(&script) {
            Ok(_) => {
                eprintln!("Script syntax OK");
                return Ok(());
            }
            Err(e) => {
                eprintln!("Script syntax error: {}", e);
                std::process::exit(1);
            }
        }
    }

    // Load enrichment tables
    let enrichment = if !cli.enrichment.is_empty() {
        debug!(tables = cli.enrichment.len(), "Loading enrichment tables");

        // Create S3 client if any tables are from S3
        let needs_s3 = cli.enrichment.iter().any(|t| t.source.starts_with("s3://"));
        let s3_client = if needs_s3 {
            let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
            Some(aws_sdk_s3::Client::new(&config))
        } else {
            None
        };

        // Fix key fields by reading CSV headers
        let mut configs = Vec::new();
        for mut config in cli.enrichment {
            // Load first line to get actual key field
            if !config.source.starts_with("s3://") {
                let content = tokio::fs::read_to_string(&config.source)
                    .await
                    .context(format!("Failed to read: {}", config.source))?;
                if let Some(first_line) = content.lines().next() {
                    let headers: Vec<&str> = first_line.split(',').collect();
                    if !headers.is_empty() {
                        config.key_field = headers[0].to_string();
                    }
                }
            }
            configs.push(config);
        }

        Some(Arc::new(
            EnrichmentRegistry::load(&configs, s3_client.as_ref())
                .await
                .context("Failed to load enrichment tables")?,
        ))
    } else {
        None
    };

    // Create transform
    let config = TransformConfig {
        script: Some(script),
        script_file: None,
        error_policy: cli.error_policy,
    };
    let transform =
        RhaiTransform::new(&config, enrichment).context("Failed to create transform")?;

    // Process input
    let mut stats = TransformStats::default();
    let stdin = std::io::stdin();
    let reader = BufReader::new(stdin.lock());
    let stdout = std::io::stdout();
    let mut writer = BufWriter::new(stdout.lock());

    for (i, line_result) in reader.lines().enumerate() {
        if let Some(limit) = cli.limit {
            if i >= limit {
                break;
            }
        }

        let line = line_result.context("Failed to read line")?;
        if line.trim().is_empty() {
            continue;
        }

        stats.records_in += 1;

        // Parse JSON into RecordBatch (single row)
        let batch = match json_line_to_batch(&line) {
            Ok(b) => b,
            Err(e) => {
                debug!(line = i + 1, error = %e, "Failed to parse JSON");
                stats.records_errored += 1;
                match cli.error_policy {
                    ErrorPolicy::Drop => continue,
                    ErrorPolicy::Fail => bail!("Failed to parse line {}: {}", i + 1, e),
                    ErrorPolicy::Passthrough => {
                        writeln!(writer, "{}", line)?;
                        stats.records_out += 1;
                        continue;
                    }
                }
            }
        };

        // Apply transform
        match transform.apply(batch) {
            Ok(result) => {
                if result.num_rows() == 0 {
                    stats.records_filtered += 1;
                } else {
                    // Convert back to JSON
                    let json = batch_to_json(&result, cli.pretty)?;
                    writeln!(writer, "{}", json)?;
                    stats.records_out += 1;
                }
            }
            Err(e) => {
                debug!(line = i + 1, error = %e, "Transform error");
                stats.records_errored += 1;
                match cli.error_policy {
                    ErrorPolicy::Drop => continue,
                    ErrorPolicy::Fail => bail!("Transform failed at line {}: {}", i + 1, e),
                    ErrorPolicy::Passthrough => {
                        writeln!(writer, "{}", line)?;
                        stats.records_out += 1;
                    }
                }
            }
        }
    }

    writer.flush()?;

    // Print stats
    if cli.stats {
        eprintln!();
        eprintln!("Statistics:");
        eprintln!("  Records in:        {}", stats.records_in);
        eprintln!("  Records out:       {}", stats.records_out);
        eprintln!("  Records filtered:  {}", stats.records_filtered);
        eprintln!("  Records errored:   {}", stats.records_errored);
    }

    Ok(())
}

/// Parses a JSON line into a single-row RecordBatch.
fn json_line_to_batch(line: &str) -> Result<Arc<RecordBatch>> {
    // Parse JSON to determine schema
    let value: serde_json::Value = serde_json::from_str(line).context("Invalid JSON")?;

    let obj = value.as_object().context("Expected JSON object")?;

    // Build schema from JSON keys
    let fields: Vec<Field> = obj
        .iter()
        .map(|(k, v)| {
            let data_type = match v {
                serde_json::Value::Bool(_) => DataType::Boolean,
                serde_json::Value::Number(n) if n.is_i64() => DataType::Int64,
                serde_json::Value::Number(_) => DataType::Float64,
                serde_json::Value::String(_) => DataType::Utf8,
                _ => DataType::Utf8, // Arrays and objects as JSON strings
            };
            Field::new(k, data_type, true)
        })
        .collect();

    let schema = Arc::new(Schema::new(fields));

    // Use Arrow JSON reader
    let reader = ReaderBuilder::new(schema.clone())
        .with_batch_size(1)
        .build(std::io::Cursor::new(line.as_bytes()))?;

    if let Some(batch_result) = reader.into_iter().next() {
        let batch = batch_result?;
        return Ok(Arc::new(batch));
    }

    bail!("No data in JSON line")
}

/// Converts a RecordBatch back to JSON.
fn batch_to_json(batch: &RecordBatch, pretty: bool) -> Result<String> {
    // Use Arrow's JSON writer
    let mut buf = Vec::new();
    {
        let mut writer = arrow::json::LineDelimitedWriter::new(&mut buf);
        writer.write(batch)?;
        writer.finish()?;
    }

    let json_str = String::from_utf8(buf)?;
    let json_str = json_str.trim();

    if pretty {
        // Parse and re-format with indentation
        let value: serde_json::Value = serde_json::from_str(json_str)?;
        Ok(serde_json::to_string_pretty(&value)?)
    } else {
        Ok(json_str.to_string())
    }
}
