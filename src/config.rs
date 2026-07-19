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
//! # AMQP-specific parameters
//! host = "localhost"
//! port = 5672
//! username = "guest"
//! password = "guest"
//!
//! # A null network always succeeds but does nothing.
//! [networks.blackhole]
//! kind = "null"
//! ```

use serde::{Deserialize, Serialize};
use std::{collections::HashMap, str::FromStr};
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
	pub(crate) networks: HashMap<String, NetworkConfig>,
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

		[networks]
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
				service: services,
			},
			networks: HashMap::new(),
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
}
