//! Configuration related utilities.

use serde::{Deserialize, Serialize};
use std::{collections::HashMap, str::FromStr};
use uuid::Uuid;

#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct AsbConfig {
	pub(crate) system_uuid: Option<Uuid>,
	pub(crate) services: HashMap<String, ServiceConfig>,
}
impl FromStr for AsbConfig {
	type Err = toml::de::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		toml::from_str(s)
	}
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct ServiceConfig {
	pub(crate) service_uuid: Uuid,
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
		"#;

		let mut services = HashMap::new();
		services.insert(
			"my_service".to_string(),
			ServiceConfig {
				service_uuid: uuid::uuid!("00000000-0000-4000-8000-0123456789AB"),
			},
		);
		let expected = AsbConfig {
			system_uuid: Some(Uuid::nil()),
			services,
		};

		let parsed: AsbConfig = CONFIG.parse().unwrap();
		assert_eq!(parsed, expected);
	}
}
