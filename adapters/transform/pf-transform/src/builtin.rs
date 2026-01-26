//! Built-in Rhai functions for transforms.

use rhai::{Dynamic, Engine};
use std::net::IpAddr;

/// Registers all built-in functions in the Rhai engine.
pub fn register_builtin_functions(engine: &mut Engine) {
    register_uuid_functions(engine);
    register_time_functions(engine);
    register_parsing_functions(engine);
    register_string_functions(engine);
    register_ip_functions(engine);
}

fn register_uuid_functions(engine: &mut Engine) {
    // UUID v4 generation
    engine.register_fn("uuid", || uuid::Uuid::new_v4().to_string());
}

fn register_time_functions(engine: &mut Engine) {
    // Current timestamp in ISO 8601 format
    engine.register_fn("timestamp", || chrono::Utc::now().to_rfc3339());

    // Unix timestamp in seconds
    engine.register_fn("unix_timestamp", || chrono::Utc::now().timestamp());

    // Unix timestamp in milliseconds
    engine.register_fn("unix_timestamp_ms", || {
        chrono::Utc::now().timestamp_millis()
    });
}

fn register_parsing_functions(engine: &mut Engine) {
    // Parse string to integer
    engine.register_fn("parse_int", |s: &str| -> Dynamic {
        s.parse::<i64>().map(Dynamic::from).unwrap_or(Dynamic::UNIT)
    });

    // Parse string to float
    engine.register_fn("parse_float", |s: &str| -> Dynamic {
        s.parse::<f64>().map(Dynamic::from).unwrap_or(Dynamic::UNIT)
    });

    // Parse string to boolean
    engine.register_fn("parse_bool", |s: &str| -> Dynamic {
        match s.to_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Dynamic::from(true),
            "false" | "0" | "no" | "off" => Dynamic::from(false),
            _ => Dynamic::UNIT,
        }
    });
}

fn register_string_functions(engine: &mut Engine) {
    // Convert to lowercase
    engine.register_fn("to_lowercase", |s: &str| s.to_lowercase());

    // Convert to uppercase
    engine.register_fn("to_uppercase", |s: &str| s.to_uppercase());

    // Trim whitespace
    engine.register_fn("trim", |s: &str| s.trim().to_string());

    // Trim start
    engine.register_fn("trim_start", |s: &str| s.trim_start().to_string());

    // Trim end
    engine.register_fn("trim_end", |s: &str| s.trim_end().to_string());

    // String contains
    engine.register_fn("str_contains", |s: &str, pattern: &str| s.contains(pattern));

    // String starts with
    engine.register_fn("starts_with", |s: &str, prefix: &str| s.starts_with(prefix));

    // String ends with
    engine.register_fn("ends_with", |s: &str, suffix: &str| s.ends_with(suffix));

    // Replace substring
    engine.register_fn("replace", |s: &str, from: &str, to: &str| {
        s.replace(from, to)
    });

    // Split string into array
    engine.register_fn("split", |s: &str, delimiter: &str| -> rhai::Array {
        s.split(delimiter)
            .map(|part| Dynamic::from(part.to_string()))
            .collect()
    });

    // Join array into string
    engine.register_fn("join", |arr: rhai::Array, delimiter: &str| -> String {
        arr.iter()
            .filter_map(|v| v.clone().into_string().ok())
            .collect::<Vec<_>>()
            .join(delimiter)
    });

    // Substring extraction
    engine.register_fn("substring", |s: &str, start: i64, len: i64| -> String {
        let start = start.max(0) as usize;
        let len = len.max(0) as usize;
        s.chars().skip(start).take(len).collect()
    });
}

fn register_ip_functions(engine: &mut Engine) {
    // Check if IP is private/loopback
    engine.register_fn("is_private_ip", |ip: &str| -> bool {
        ip.parse::<IpAddr>()
            .map(|addr| match addr {
                IpAddr::V4(v4) => v4.is_private() || v4.is_loopback() || v4.is_link_local(),
                IpAddr::V6(v6) => v6.is_loopback(),
            })
            .unwrap_or(false)
    });

    // Check if IP is valid
    engine.register_fn("is_valid_ip", |ip: &str| -> bool {
        ip.parse::<IpAddr>().is_ok()
    });

    // Check if IP is IPv4
    engine.register_fn("is_ipv4", |ip: &str| -> bool {
        ip.parse::<IpAddr>()
            .map(|addr| matches!(addr, IpAddr::V4(_)))
            .unwrap_or(false)
    });

    // Check if IP is IPv6
    engine.register_fn("is_ipv6", |ip: &str| -> bool {
        ip.parse::<IpAddr>()
            .map(|addr| matches!(addr, IpAddr::V6(_)))
            .unwrap_or(false)
    });

    // Normalize IP address (parse and re-format)
    engine.register_fn("normalize_ip", |ip: &str| -> Dynamic {
        ip.parse::<IpAddr>()
            .map(|addr| Dynamic::from(addr.to_string()))
            .unwrap_or(Dynamic::UNIT)
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_engine() -> Engine {
        let mut engine = Engine::new();
        register_builtin_functions(&mut engine);
        engine
    }

    #[test]
    fn test_uuid() {
        let engine = create_engine();
        let result: String = engine.eval("uuid()").unwrap();
        assert_eq!(result.len(), 36); // UUID format: 8-4-4-4-12
        assert!(result.contains('-'));
    }

    #[test]
    fn test_timestamp() {
        let engine = create_engine();
        let result: String = engine.eval("timestamp()").unwrap();
        assert!(result.contains('T')); // ISO 8601 format
        assert!(result.contains('Z') || result.contains('+'));
    }

    #[test]
    fn test_unix_timestamp() {
        let engine = create_engine();
        let result: i64 = engine.eval("unix_timestamp()").unwrap();
        assert!(result > 1700000000); // After 2023
    }

    #[test]
    fn test_parse_int() {
        let engine = create_engine();

        let result: i64 = engine.eval(r#"parse_int("42")"#).unwrap();
        assert_eq!(result, 42);

        let result: Dynamic = engine.eval(r#"parse_int("not-a-number")"#).unwrap();
        assert!(result.is_unit());
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_parse_float() {
        let engine = create_engine();

        let result: f64 = engine.eval(r#"parse_float("3.14")"#).unwrap();
        assert!((result - 3.14).abs() < 0.001);
    }

    #[test]
    fn test_parse_bool() {
        let engine = create_engine();

        let result: bool = engine.eval(r#"parse_bool("true")"#).unwrap();
        assert!(result);

        let result: bool = engine.eval(r#"parse_bool("0")"#).unwrap();
        assert!(!result);

        let result: Dynamic = engine.eval(r#"parse_bool("maybe")"#).unwrap();
        assert!(result.is_unit());
    }

    #[test]
    fn test_string_functions() {
        let engine = create_engine();

        let result: String = engine.eval(r#"to_lowercase("HELLO")"#).unwrap();
        assert_eq!(result, "hello");

        let result: String = engine.eval(r#"to_uppercase("hello")"#).unwrap();
        assert_eq!(result, "HELLO");

        let result: String = engine.eval(r#"trim("  hello  ")"#).unwrap();
        assert_eq!(result, "hello");

        let result: bool = engine
            .eval(r#"str_contains("hello world", "world")"#)
            .unwrap();
        assert!(result);

        let result: bool = engine.eval(r#"starts_with("hello", "hel")"#).unwrap();
        assert!(result);

        let result: String = engine.eval(r#"replace("hello", "l", "L")"#).unwrap();
        assert_eq!(result, "heLLo");

        let result: rhai::Array = engine.eval(r#"split("a,b,c", ",")"#).unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_substring() {
        let engine = create_engine();

        let result: String = engine.eval(r#"substring("hello world", 0, 5)"#).unwrap();
        assert_eq!(result, "hello");

        let result: String = engine.eval(r#"substring("hello", 6, 5)"#).unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_is_private_ip() {
        let engine = create_engine();

        let result: bool = engine.eval(r#"is_private_ip("10.0.0.1")"#).unwrap();
        assert!(result);

        let result: bool = engine.eval(r#"is_private_ip("192.168.1.1")"#).unwrap();
        assert!(result);

        let result: bool = engine.eval(r#"is_private_ip("127.0.0.1")"#).unwrap();
        assert!(result);

        let result: bool = engine.eval(r#"is_private_ip("8.8.8.8")"#).unwrap();
        assert!(!result);

        let result: bool = engine.eval(r#"is_private_ip("not-an-ip")"#).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_is_valid_ip() {
        let engine = create_engine();

        let result: bool = engine.eval(r#"is_valid_ip("192.168.1.1")"#).unwrap();
        assert!(result);

        let result: bool = engine.eval(r#"is_valid_ip("::1")"#).unwrap();
        assert!(result);

        let result: bool = engine.eval(r#"is_valid_ip("not-an-ip")"#).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_ip_version() {
        let engine = create_engine();

        let result: bool = engine.eval(r#"is_ipv4("192.168.1.1")"#).unwrap();
        assert!(result);

        let result: bool = engine.eval(r#"is_ipv6("192.168.1.1")"#).unwrap();
        assert!(!result);

        let result: bool = engine.eval(r#"is_ipv6("::1")"#).unwrap();
        assert!(result);
    }

    #[test]
    fn test_normalize_ip() {
        let engine = create_engine();

        let result: String = engine.eval(r#"normalize_ip("::1")"#).unwrap();
        assert_eq!(result, "::1");

        let result: Dynamic = engine.eval(r#"normalize_ip("not-an-ip")"#).unwrap();
        assert!(result.is_unit());
    }
}
