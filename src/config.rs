//! Configuration related utilities.

use serde::{Deserialize, Serialize};
use std::{collections::HashMap, str::FromStr};
use toml::Table;
use uuid::Uuid;

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct AsbConfig {
	pub(crate) system_uuid: Option<Uuid>,
	pub(crate) services: HashMap<String, ServiceConfig>,
	pub(crate) networks: HashMap<String, NetworkConfig>,
}
impl FromStr for AsbConfig {
	type Err = toml::de::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		toml::from_str(s)
	}
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ServiceConfig {
	pub(crate) service_uuid: Option<Uuid>,
	pub(crate) network: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct NetworkConfig {
	pub(crate) kind: NetworkKind,
	#[serde(flatten)]
	pub(crate) params: Table,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum NetworkKind {
	/// AMQP 0-9-1
	#[serde(rename = "amqp", alias = "AMQP")]
	Amqp,
	/// The lack of any network. Useful for testing or quick config changes.
	#[serde(rename = "null", alias = "NULL")]
	Null,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
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

		[networks]
		"#;

		let mut services = HashMap::new();
		services.insert(
			"my_service".to_string(),
			ServiceConfig {
				service_uuid: Some(uuid::uuid!("00000000-0000-4000-8000-0123456789AB")),
				network: "null".to_string(),
			},
		);
		let expected = AsbConfig {
			system_uuid: Some(Uuid::nil()),
			services,
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
