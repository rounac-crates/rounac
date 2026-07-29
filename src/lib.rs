//! Rounac
//!
//! The Rust [OMS][1] [UCI][2] Not-A-CAL; pronounced "Runic".
//!
//! [1]: https://gitlab.com/open-arsenal/oms/standard
//! [2]: https://gitlab.com/open-arsenal/uci/standard

pub mod config;
pub mod error;
mod msg_serde;
mod networks;

pub use crate::error::{CalError, CalErrorKind};
use crate::networks::AsbConnection;
pub use crate::networks::{
	AsbListener, AsbReader, AsbWriter,
	utils::{AsbConnStatus, AsbStatusListener},
};

use config::AsbConfig;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Abstract Service Bus.
pub struct Asb {
	config: AsbConfig,
	service_name: String,
	system_uuid: Uuid,
	service_uuid: Uuid,
	//runtime: tokio::runtime::Runtime, // Maybe only use if async asb.
	/// Data specific to this ASB's network connection.
	connection: AsbConnection,
}
impl Asb {
	/// Get an initialized ASB for the client with the name `service_name`.
	pub fn new(service_name: &str, config: AsbConfig) -> Result<Self, CalError> {
		let Some(service_config) = config.services.service.get(service_name) else {
			return Err(CalError::config_err(format!(
				"Missing service config for {service_name}"
			)));
		};

		// Get system and service UUIDs from given config, otherwise generate one.
		let system_uuid = config.system_uuid.unwrap_or(Uuid::new_v4());
		let service_uuid = match service_config.service_uuid {
			Some(u) => u,
			None => Uuid::new_v4(),
		};

		// Get network from service config or the default
		let default_network = config.services.default_network.as_ref();
		let Some(network) = service_config.network.as_ref().or(default_network) else {
			return Err(CalError::config_err(format!(
				"Missing network config for service {service_name}"
			)));
		};

		let connection = AsbConnection::connect(network, &config)?;

		Ok(Asb {
			config,
			service_name: service_name.to_owned(),
			system_uuid,
			service_uuid,
			connection,
		})
	}

	/// Get the current status of this ASB.
	pub fn get_connection_status(&self) -> AsbConnStatus {
		self.connection.get_status()
	}

	/// Register a function to be called whenever the status of this ASB changes.
	pub fn add_status_listener(&self, fun: impl AsbStatusListener) -> u32 {
		self.connection.add_status_listener(fun)
	}

	/// Remove the listener identified with `id`, returning `true` if it exists.
	pub fn remove_status_listener(&self, id: u32) -> bool {
		self.connection.remove_status_listener(id)
	}

	/// Return the [Uuid] of the system this ASB resides on.
	pub fn get_system_uuid(&self) -> Uuid {
		self.system_uuid
	}

	/// Return the [Uuid] of the service that initialized this [Asb] object.
	pub fn get_service_uuid(&self) -> Uuid {
		self.service_uuid
	}

	/// Create a new [AsbReader] for the given [Topic].
	pub fn new_reader<T: for<'de> Deserialize<'de> + Send + Sync + 'static>(
		&self,
		topic: &str,
	) -> Result<AsbReader<T>, CalError> {
		Ok(self
			.connection
			.create_reader(topic, &self.config, &self.service_name)?)
	}

	/// Create a new [AsbWriter] for the given [Topic].
	pub fn new_writer<T: Serialize + 'static>(
		&self,
		topic: &str,
	) -> Result<AsbWriter<T>, CalError> {
		Ok(self
			.connection
			.create_writer(topic, &self.config, &self.service_name)?)
	}
}
