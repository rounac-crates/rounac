//! CAL configuration
//!
//! This CAL is configured using a TOML file, with a complete reference seen
//! below.
//!
//! # Complete configuration reference:
//! ```toml
//! # Optional system UUID (random v4 if unspecified).
//! system_uuid = "00000000-0000-0000-0000-000000000000"
//!
//! # Configurations that apply to all services.
//! [services]
//! # Optional default network for services.
//! default_network = "rabbit"
//! default_wire_format = "xml"
//!
//! # Configuration for "service1" service.
//! [services.service1]
//! # Optional service UUID (random v4 if unspecified).
//! service_uuid = "00000000-0000-4000-8000-0123456789AB"
//! # Optional if services.default_network exists, otherwise required.
//! # Specifies the "networks" sub-table this service should use.
//! network = "rabbitmq"
//! # Optional if services.default_wire_format exists, otherwise required.
//! wire_format = "xml"
//!
//! # Configuration for "rabbitmq" network.
//! [networks.rabbitmq]
//! # Required network kind, which defines remaining parameters.
//! kind = "amqp"
//! ## AMQP-specific parameters
//! # Required hostname of broker.
//! host = "localhost"
//! # Required port of broker
//! port = 5672
//! # Required credentials to access broker.
//! username = "guest"
//! password = "guest"
//! # Optional non-empty exchange name to segregate traffic on this ASB.
//! exchange = "rounac"
//! # Optional boolean to specify exchange durability. Defaults to true.
//! durable_exchange = true
//!
//! # A null network always succeeds but does nothing.
//! [networks.blackhole]
//! kind = "null"
//!
//! [qos.blink_and_miss_it]
//! # Optional buffer size. Defaults to 100.
//! buffer = 1
//! # Optional reliability. Accepts "reliable" (default) or "best_effort".
//! reliability = "best_effort"
//! # Optional expiration. Accepts positive number with optional suffix.
//! # Suffixes: ns, us, ms, s, min, h
//! expiration = "5s" # "5 s" also works
//! # Optional time-based filter. Reduces frequency of read messages but does
//! # not necessarily reduce frequency of messages received from the network.
//! time_based_filter = "1 s" # See expiration comment for format.
//! ```

use serde::{Deserialize, Serialize};
use std::{collections::HashMap, str::FromStr, time::Duration};
use toml::Table;
use uuid::Uuid;

/// Full ASB configuration.
///
/// # Usage
/// This type implements [FromStr] so simply call `.parse()` on your config
/// string. See the module documentation for the configuration format.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct AsbConfig {
	pub(crate) system_uuid: Option<Uuid>,
	pub(crate) services: ServicesConfig,
	#[serde(default)]
	pub(crate) networks: HashMap<String, NetworkConfig>,
	#[serde(default)]
	pub(crate) qos: HashMap<String, QosSettings>,
}
impl FromStr for AsbConfig {
	type Err = toml::de::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		toml::from_str(s)
	}
}

/// Configuration for all services.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct ServicesConfig {
	pub(crate) default_network: Option<String>,
	pub(crate) default_wire_format: Option<WireFormat>,
	pub(crate) default_qos: Option<String>,
	#[serde(flatten)]
	pub(crate) service: HashMap<String, ServiceConfig>,
}

/// Configuration of a single service.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ServiceConfig {
	pub(crate) service_uuid: Option<Uuid>,
	pub(crate) network: Option<String>,
	pub(crate) wire_format: Option<WireFormat>,
}

/// Configuration of a single network.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct NetworkConfig {
	pub(crate) kind: NetworkKind,
	#[serde(flatten)]
	pub(crate) params: Table,
}

/// The kinds of networks supported by the configuration.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[non_exhaustive]
pub enum NetworkKind {
	/// AMQP 0-9-1
	#[serde(rename = "amqp", alias = "AMQP")]
	Amqp,
	/// The lack of any network. Useful for testing or quick config changes.
	#[serde(rename = "null", alias = "NULL")]
	Null,
}

/// The specific format to be used when (de)serializing messages.
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Serialize)]
#[non_exhaustive]
pub enum WireFormat {
	#[serde(rename = "xml", alias = "XML")]
	Xml,
}
impl FromStr for WireFormat {
	type Err = &'static str;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s.to_lowercase().as_str() {
			"xml" => Ok(WireFormat::Xml),
			_ => Err("unrecognized wire format"),
		}
	}
}

/// Quality-of-Service settings for the CAL.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(default)]
pub struct QosSettings {
	#[serde(with = "opt_duration_serde")]
	pub(crate) time_based_filter: Option<Duration>,
	pub(crate) reliability: ReliabilityQos,
	#[serde(with = "opt_duration_serde")]
	pub(crate) expiration: Option<Duration>,
	pub(crate) buffer: usize,
}
impl Default for QosSettings {
	fn default() -> Self {
		QosSettings {
			time_based_filter: None,
			reliability: ReliabilityQos::default(),
			expiration: None,
			buffer: 100,
		}
	}
}

/// Module to perform serde for config [Option<Duration>] types.
mod opt_duration_serde {
	use serde::{
		Deserializer, Serializer,
		de::{Error, Visitor},
	};
	use std::time::Duration;

	/// Get a duration value from a count and possible suffix. Assumes seconds if no suffix.
	fn duration_value_suffix(v: u64, suffix: Option<&str>) -> Option<Duration> {
		if let Some(suf) = suffix {
			match suf {
				"h" => Some(Duration::from_hours(v)),
				"min" => Some(Duration::from_mins(v)),
				"s" => Some(Duration::from_secs(v)),
				"ms" => Some(Duration::from_millis(v)),
				"us" => Some(Duration::from_micros(v)),
				"ns" => Some(Duration::from_nanos(v)),
				_ => None,
			}
		} else {
			Some(Duration::from_secs(v as u64))
		}
	}

	pub fn serialize<S: Serializer>(v: &Option<Duration>, ser: S) -> Result<S::Ok, S::Error> {
		if let Some(d) = v {
			let value = format!("{}ms", d.as_millis());
			ser.serialize_some(&value)
		} else {
			ser.serialize_none()
		}
	}

	pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Option<Duration>, D::Error> {
		struct StringVisitor;
		impl<'d> Visitor<'d> for StringVisitor {
			type Value = String;

			fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
				formatter.write_str("positive integer with optional unit suffix")
			}

			fn visit_str<E: Error>(self, s: &str) -> Result<Self::Value, E> {
				Ok(s.to_string())
			}
		}

		let de_string = de.deserialize_string(StringVisitor)?;
		let dur_str = de_string.trim();

		// By default "split" at end, i.e. no suffix.
		let split_point = match dur_str.find(['h', 'm', 'n', 's', 'u']) {
			Some(idx) => idx,
			None => dur_str.len(),
		};

		// Split into value and suffix
		let (val_str, suf_str) = dur_str.split_at(split_point);
		let suf = suf_str.trim();

		// Try parsing number
		let Ok(val) = val_str.trim().parse() else {
			return Err(D::Error::custom("invalid integer"));
		};

		if suf.is_empty() {
			// Safety: Without a suffix conversion always succeeds.
			Ok(Some(duration_value_suffix(val, None).unwrap()))
		} else {
			let d = duration_value_suffix(val, Some(suf))
				.ok_or(D::Error::custom("invalid duration suffix"))?;

			Ok(Some(d))
		}
	}
}

/// Reliability types for a CAL.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum ReliabilityQos {
	#[serde(rename = "reliable")]
	Reliable,
	#[serde(rename = "best_effort")]
	BestEffort,
}
impl Default for ReliabilityQos {
	fn default() -> Self {
		ReliabilityQos::Reliable
	}
}

#[cfg(test)]
mod test {
	use super::*;
	use uuid::Uuid;

	#[test]
	fn single_service_config() {
		const CONFIG: &str = r#"
		system_uuid = "00000000-0000-0000-0000-000000000000"

		[services.my_service]
		service_uuid = "00000000-0000-4000-8000-0123456789AB"
		network = "null"
		wire_format = "xml"
		"#;

		let mut services = HashMap::new();
		services.insert(
			"my_service".to_string(),
			ServiceConfig {
				service_uuid: Some(uuid::uuid!("00000000-0000-4000-8000-0123456789AB")),
				network: Some("null".to_string()),
				wire_format: Some(WireFormat::Xml),
			},
		);
		let expected = AsbConfig {
			system_uuid: Some(Uuid::nil()),
			services: ServicesConfig {
				default_network: None,
				default_wire_format: None,
				default_qos: None,
				service: services,
			},
			networks: HashMap::new(),
			qos: HashMap::new(),
		};

		let parsed: AsbConfig = CONFIG.parse().unwrap();
		assert_eq!(parsed, expected);
	}

	#[test]
	fn deserialize_wire_format() {
		#[derive(Debug, Deserialize, Serialize)]
		struct TestConfig {
			format: WireFormat,
		}
		const GOOD_CONFIG1: &str = r#"format = "xml""#;
		const GOOD_CONFIG2: &str = r#"format = "XML""#;
		const BAD_CONFIG1: &str = r#"format = "xMl""#;
		const BAD_CONFIG2: &str = r#"format = "XmL""#;

		// Test good deserialization
		toml::from_str::<TestConfig>(GOOD_CONFIG1).unwrap();
		toml::from_str::<TestConfig>(GOOD_CONFIG2).unwrap();

		// Test bad serialization
		toml::from_str::<TestConfig>(BAD_CONFIG1).unwrap_err();
		toml::from_str::<TestConfig>(BAD_CONFIG2).unwrap_err();
	}

	/// Test the deserialization of [Option<Duration>] with bad number.
	#[test]
	fn deserialize_opt_duration_bad_num() {
		#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
		struct MyDur {
			#[serde(with = "opt_duration_serde")]
			dur: Option<Duration>,
		}

		const DUR_STR1: &str = r#"dur = "-10""#;
		const DUR_STR2: &str = r#"dur = " 1 0 ""#;
		const DUR_STR3: &str = r#"dur = " 10 10""#;

		toml::from_str::<MyDur>(DUR_STR1).unwrap_err();
		toml::from_str::<MyDur>(DUR_STR2).unwrap_err();
		toml::from_str::<MyDur>(DUR_STR3).unwrap_err();
	}

	/// Test the deserialization of [Option<Duration>] with no suffix.
	#[test]
	fn deserialize_opt_duration_no() {
		#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
		struct MyDur {
			#[serde(with = "opt_duration_serde")]
			dur: Option<Duration>,
		}

		const DUR_STR1: &str = r#"dur = "10""#;
		const DUR_STR2: &str = r#"dur = " 10 ""#;
		let expected = MyDur {
			dur: Some(Duration::from_secs(10)),
		};

		assert_eq!(expected, toml::from_str(DUR_STR1).unwrap());
		assert_eq!(expected, toml::from_str(DUR_STR2).unwrap());
	}

	/// Test the deserialization of [Option<Duration>] with bad suffix.
	#[test]
	fn deserialize_opt_duration_bad_suffix() {
		#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
		struct MyDur {
			#[serde(with = "opt_duration_serde")]
			dur: Option<Duration>,
		}

		const DUR_STR1: &str = r#"dur = "10d""#;
		const DUR_STR2: &str = r#"dur = "10 d""#;

		toml::from_str::<MyDur>(DUR_STR1).unwrap_err();
		toml::from_str::<MyDur>(DUR_STR2).unwrap_err();
	}

	/// Test the deserialization of [Option<Duration>] with "ns" suffix.
	#[test]
	fn deserialize_opt_duration_ns() {
		#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
		struct MyDur {
			#[serde(with = "opt_duration_serde")]
			dur: Option<Duration>,
		}

		const DUR_STR1: &str = r#"dur = "10ns""#;
		const DUR_STR2: &str = r#"dur = "10 ns""#;
		let expected = MyDur {
			dur: Some(Duration::from_nanos(10)),
		};

		assert_eq!(expected, toml::from_str(DUR_STR1).unwrap());
		assert_eq!(expected, toml::from_str(DUR_STR2).unwrap());
	}

	/// Test the deserialization of [Option<Duration>] with "us" suffix.
	#[test]
	fn deserialize_opt_duration_us() {
		#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
		struct MyDur {
			#[serde(with = "opt_duration_serde")]
			dur: Option<Duration>,
		}

		const DUR_STR1: &str = r#"dur = "10us""#;
		const DUR_STR2: &str = r#"dur = "10 us""#;
		let expected = MyDur {
			dur: Some(Duration::from_micros(10)),
		};

		assert_eq!(expected, toml::from_str(DUR_STR1).unwrap());
		assert_eq!(expected, toml::from_str(DUR_STR2).unwrap());
	}

	/// Test the deserialization of [Option<Duration>] with "ms" suffix.
	#[test]
	fn deserialize_opt_duration_ms() {
		#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
		struct MyDur {
			#[serde(with = "opt_duration_serde")]
			dur: Option<Duration>,
		}

		const DUR_STR1: &str = r#"dur = "10ms""#;
		const DUR_STR2: &str = r#"dur = "10 ms""#;
		let expected = MyDur {
			dur: Some(Duration::from_millis(10)),
		};

		assert_eq!(expected, toml::from_str(DUR_STR1).unwrap());
		assert_eq!(expected, toml::from_str(DUR_STR2).unwrap());
	}

	/// Test the deserialization of [Option<Duration>] with "s" suffix.
	#[test]
	fn deserialize_opt_duration_s() {
		#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
		struct MyDur {
			#[serde(with = "opt_duration_serde")]
			dur: Option<Duration>,
		}

		const DUR_STR1: &str = r#"dur = "10s""#;
		const DUR_STR2: &str = r#"dur = "10 s""#;
		let expected = MyDur {
			dur: Some(Duration::from_secs(10)),
		};

		assert_eq!(expected, toml::from_str(DUR_STR1).unwrap());
		assert_eq!(expected, toml::from_str(DUR_STR2).unwrap());
	}

	/// Test the deserialization of [Option<Duration>] with "min" suffix.
	#[test]
	fn deserialize_opt_duration_min() {
		#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
		struct MyDur {
			#[serde(with = "opt_duration_serde")]
			dur: Option<Duration>,
		}

		const DUR_STR1: &str = r#"dur = "10min""#;
		const DUR_STR2: &str = r#"dur = "10 min""#;
		let expected = MyDur {
			dur: Some(Duration::from_mins(10)),
		};

		assert_eq!(expected, toml::from_str(DUR_STR1).unwrap());
		assert_eq!(expected, toml::from_str(DUR_STR2).unwrap());
	}

	/// Test the deserialization of [Option<Duration>] with "h" suffix.
	#[test]
	fn deserialize_opt_duration_h() {
		#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
		struct MyDur {
			#[serde(with = "opt_duration_serde")]
			dur: Option<Duration>,
		}

		const DUR_STR1: &str = r#"dur = "10h""#;
		const DUR_STR2: &str = r#"dur = "10 h""#;
		let expected = MyDur {
			dur: Some(Duration::from_hours(10)),
		};

		assert_eq!(expected, toml::from_str(DUR_STR1).unwrap());
		assert_eq!(expected, toml::from_str(DUR_STR2).unwrap());
	}
}
