//! pf-metrics - OpenTelemetry metrics integration for paraflow-extreme.
//!
//! Provides a simple interface for emitting metrics with job_id dimension
//! support. Metrics can be exported to any OpenTelemetry-compatible backend
//! such as Prometheus, CloudWatch, or Datadog.
//!
//! # Example
//!
//! ```ignore
//! use pf_metrics::{MetricsConfig, MetricsRecorder};
//!
//! // Initialize metrics with job_id
//! let config = MetricsConfig::new("my-job-id")
//!     .with_service_name("pf-worker");
//! let recorder = MetricsRecorder::new(config)?;
//!
//! // Record metrics
//! recorder.record_files_processed(10);
//! recorder.record_records_processed(50000);
//! recorder.record_bytes_read(1024 * 1024);
//! ```

use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::{MeterProviderBuilder, PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::runtime;
use opentelemetry_sdk::Resource;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Configuration for metrics collection.
#[derive(Debug, Clone)]
pub struct MetricsConfig {
    /// Job identifier - added as a dimension to all metrics.
    pub job_id: String,

    /// Service name for metrics attribution.
    pub service_name: String,

    /// OTLP endpoint for exporting metrics (optional).
    /// If not set, metrics are collected but not exported.
    pub otlp_endpoint: Option<String>,

    /// Export interval in seconds.
    pub export_interval_secs: u64,
}

impl MetricsConfig {
    /// Create a new metrics configuration with a job ID.
    pub fn new(job_id: impl Into<String>) -> Self {
        Self {
            job_id: job_id.into(),
            service_name: "paraflow".to_string(),
            otlp_endpoint: None,
            export_interval_secs: 10,
        }
    }

    /// Set the service name.
    pub fn with_service_name(mut self, name: impl Into<String>) -> Self {
        self.service_name = name.into();
        self
    }

    /// Set the OTLP endpoint for metric export.
    pub fn with_otlp_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.otlp_endpoint = Some(endpoint.into());
        self
    }

    /// Set the export interval in seconds.
    pub fn with_export_interval(mut self, interval_secs: u64) -> Self {
        self.export_interval_secs = interval_secs;
        self
    }
}

/// Metrics recorder for paraflow worker operations.
///
/// All metrics are automatically tagged with the configured job_id dimension.
pub struct MetricsRecorder {
    /// The job ID dimension added to all metrics.
    job_id: String,

    /// Common attributes for all metrics.
    common_attributes: Vec<KeyValue>,

    /// Counter for files processed.
    files_processed: Counter<u64>,

    /// Counter for files failed.
    files_failed: Counter<u64>,

    /// Counter for records processed.
    records_processed: Counter<u64>,

    /// Counter for records failed.
    records_failed: Counter<u64>,

    /// Counter for bytes read.
    bytes_read: Counter<u64>,

    /// Counter for bytes written.
    bytes_written: Counter<u64>,

    /// Histogram for file processing duration in milliseconds.
    file_processing_duration_ms: Histogram<f64>,

    /// Meter provider (kept alive for the duration of metrics collection).
    _meter_provider: Option<SdkMeterProvider>,
}

impl MetricsRecorder {
    /// Create a new metrics recorder with the given configuration.
    ///
    /// If an OTLP endpoint is configured, metrics will be exported periodically.
    /// Otherwise, metrics are collected but not exported (useful for testing).
    pub fn new(config: MetricsConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        info!(
            job_id = %config.job_id,
            service_name = %config.service_name,
            otlp_endpoint = ?config.otlp_endpoint,
            "Initializing metrics recorder"
        );

        // Build resource with service info
        let resource = Resource::new(vec![
            KeyValue::new("service.name", config.service_name.clone()),
        ]);

        // Build meter provider with optional OTLP exporter
        let meter_provider = if let Some(endpoint) = &config.otlp_endpoint {
            let exporter = opentelemetry_otlp::MetricExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .build()?;

            let reader = PeriodicReader::builder(exporter, runtime::Tokio)
                .with_interval(Duration::from_secs(config.export_interval_secs))
                .build();

            let provider = MeterProviderBuilder::default()
                .with_resource(resource)
                .with_reader(reader)
                .build();

            global::set_meter_provider(provider.clone());

            Some(provider)
        } else {
            debug!("No OTLP endpoint configured, metrics will not be exported");
            None
        };

        // Get meter (uses global provider if set, or a no-op meter)
        let meter = global::meter("paraflow");

        // Create common attributes with job_id
        let common_attributes = vec![
            KeyValue::new("job_id", config.job_id.clone()),
            KeyValue::new("service", config.service_name),
        ];

        // Create counters
        let files_processed = meter
            .u64_counter("paraflow.files.processed")
            .with_description("Total number of files processed")
            .with_unit("files")
            .build();

        let files_failed = meter
            .u64_counter("paraflow.files.failed")
            .with_description("Total number of files that failed processing")
            .with_unit("files")
            .build();

        let records_processed = meter
            .u64_counter("paraflow.records.processed")
            .with_description("Total number of records processed")
            .with_unit("records")
            .build();

        let records_failed = meter
            .u64_counter("paraflow.records.failed")
            .with_description("Total number of records that failed processing")
            .with_unit("records")
            .build();

        let bytes_read = meter
            .u64_counter("paraflow.bytes.read")
            .with_description("Total bytes read from source files")
            .with_unit("bytes")
            .build();

        let bytes_written = meter
            .u64_counter("paraflow.bytes.written")
            .with_description("Total bytes written to destination")
            .with_unit("bytes")
            .build();

        let file_processing_duration_ms = meter
            .f64_histogram("paraflow.file.processing.duration")
            .with_description("File processing duration in milliseconds")
            .with_unit("ms")
            .build();

        Ok(Self {
            job_id: config.job_id,
            common_attributes,
            files_processed,
            files_failed,
            records_processed,
            records_failed,
            bytes_read,
            bytes_written,
            file_processing_duration_ms,
            _meter_provider: meter_provider,
        })
    }

    /// Get the job ID.
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    /// Record files processed.
    pub fn record_files_processed(&self, count: u64) {
        self.files_processed.add(count, &self.common_attributes);
    }

    /// Record files failed.
    pub fn record_files_failed(&self, count: u64) {
        self.files_failed.add(count, &self.common_attributes);
    }

    /// Record records processed.
    pub fn record_records_processed(&self, count: u64) {
        self.records_processed.add(count, &self.common_attributes);
    }

    /// Record records failed.
    pub fn record_records_failed(&self, count: u64) {
        self.records_failed.add(count, &self.common_attributes);
    }

    /// Record bytes read.
    pub fn record_bytes_read(&self, bytes: u64) {
        self.bytes_read.add(bytes, &self.common_attributes);
    }

    /// Record bytes written.
    pub fn record_bytes_written(&self, bytes: u64) {
        self.bytes_written.add(bytes, &self.common_attributes);
    }

    /// Record file processing duration in milliseconds.
    pub fn record_file_processing_duration_ms(&self, duration_ms: f64) {
        self.file_processing_duration_ms
            .record(duration_ms, &self.common_attributes);
    }

    /// Record file processing duration from a Duration.
    pub fn record_file_processing_duration(&self, duration: Duration) {
        self.record_file_processing_duration_ms(duration.as_secs_f64() * 1000.0);
    }

    /// Add additional attributes to metrics.
    /// Returns a new attribute set with the common attributes plus the provided ones.
    pub fn with_attributes(&self, extra: &[KeyValue]) -> Vec<KeyValue> {
        let mut attrs = self.common_attributes.clone();
        attrs.extend_from_slice(extra);
        attrs
    }

    /// Shutdown the metrics recorder and flush any pending exports.
    pub fn shutdown(self) {
        if let Some(provider) = self._meter_provider {
            if let Err(e) = provider.shutdown() {
                warn!(error = %e, "Error shutting down meter provider");
            }
        }
        info!("Metrics recorder shutdown complete");
    }
}

/// Builder for creating a no-op metrics recorder (for testing or when metrics are disabled).
pub fn noop_recorder(job_id: impl Into<String>) -> MetricsRecorder {
    MetricsRecorder::new(MetricsConfig::new(job_id))
        .expect("Creating noop recorder should not fail")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = MetricsConfig::new("test-job-123")
            .with_service_name("pf-worker")
            .with_export_interval(30);

        assert_eq!(config.job_id, "test-job-123");
        assert_eq!(config.service_name, "pf-worker");
        assert_eq!(config.export_interval_secs, 30);
        assert!(config.otlp_endpoint.is_none());
    }

    #[test]
    fn test_noop_recorder() {
        let recorder = noop_recorder("test-job");

        // Should not panic
        recorder.record_files_processed(10);
        recorder.record_records_processed(1000);
        recorder.record_bytes_read(1024);

        assert_eq!(recorder.job_id(), "test-job");
    }
}
