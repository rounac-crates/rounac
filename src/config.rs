//! Configuration related utilities.

use serde::{Deserialize, Serialize};
use std::{collections::HashMap, str::FromStr};
use uuid::Uuid;

#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct AsbConfig {
	system_id: Option<Uuid>,
	services: HashMap<String, ServiceConfig>,
}
impl FromStr for AsbConfig {
	type Err = toml::de::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		toml::from_str(s)
	}
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct ServiceConfig {
	service_id: String,
}

#[cfg(test)]
mod test {
	use super::*;
	use uuid::Uuid;

	#[test]
	fn single_service_config() {
		const CONFIG: &str = r#"
		system_id = "00000000-0000-0000-0000-000000000000"

		[services.my_service]
		service_id = "Bla"
		"#;

		let mut services = HashMap::new();
		services.insert(
			"my_service".to_string(),
			ServiceConfig {
				service_id: "Bla".to_string(),
			},
		);
		let expected = AsbConfig {
			system_id: Some(Uuid::nil()),
			services,
		};

		let parsed: AsbConfig = CONFIG.parse().unwrap();
		assert_eq!(parsed, expected);
	}
}
