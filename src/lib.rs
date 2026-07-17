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

use config::AsbConfig;
use serde::{Deserialize, Serialize};
use std::{
	default::Default,
	marker::PhantomData,
	sync::{
		Arc, RwLock,
		atomic::{AtomicUsize, Ordering},
	},
	thread,
	time::Duration,
};
use uuid::Uuid;

/// Possible states of the ASB.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AsbConnStatus {
	// This should always be 0 so default [Asb] has a sensible value.
	Initializing = 0,
	Normal,
	Degraded,
	Inoperable,
	Failed,
}
impl TryFrom<usize> for AsbConnStatus {
	type Error = ();
	fn try_from(v: usize) -> Result<Self, Self::Error> {
		match v {
			x if x == AsbConnStatus::Initializing as usize => Ok(AsbConnStatus::Initializing),
			x if x == AsbConnStatus::Normal as usize => Ok(AsbConnStatus::Normal),
			x if x == AsbConnStatus::Degraded as usize => Ok(AsbConnStatus::Degraded),
			x if x == AsbConnStatus::Inoperable as usize => Ok(AsbConnStatus::Inoperable),
			x if x == AsbConnStatus::Failed as usize => Ok(AsbConnStatus::Failed),
			_ => Err(()),
		}
	}
}

/// Empty trait that simply groups the desired closure with [Send] and [Sync].
pub trait AsbStatusListener: Fn(AsbConnStatus) + Send + Sync {}
impl<T: Fn(AsbConnStatus) + Send + Sync> AsbStatusListener for T {}

/// Abstract Service Bus.
pub struct Asb {
	config: AsbConfig,
	system_uuid: Uuid,
	service_uuid: Uuid,
	/// The current status as an integer representing a variant of [AsbConnStatus].
	status: AtomicUsize,
	/// Vector of `(id, fn)` where `id` is a random number to remove `fn` later.
	status_listeners: RwLock<Vec<(u32, Arc<dyn AsbStatusListener>)>>,
	//runtime: tokio::runtime::Runtime, // Maybe only use if async asb.
	/// Data specific to this ASB's network connection.
	connection: AsbConnection,
}
impl Asb {
	/// Get an initialized ASB for the client with the name `service_name`.
	pub fn new(service_name: &str, config: AsbConfig) -> Result<Self, CalError> {
		let Some(service_config) = config.services.get(service_name) else {
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

		let connection = AsbConnection::connect(&service_config.network, &config)?;

		Ok(Asb {
			config,
			system_uuid,
			service_uuid,
			status: AtomicUsize::default(),
			status_listeners: RwLock::new(Vec::new()),
			connection,
		})
	}

	/// Get the current status of this ASB.
	pub fn get_connection_status(&self) -> AsbConnStatus {
		// Safety: Connection status will only ever be set through `set_connection_status()` which guarantees a valid value.
		self.status.load(Ordering::Relaxed).try_into().unwrap()
	}

	/// If `new_status` differs from current status, update status and notify listeners. Else ignore.
	fn set_connection_status(&self, new_status: AsbConnStatus) {
		if self.get_connection_status() != new_status {
			self.status.store(new_status as usize, Ordering::Relaxed);
			self.call_status_listeners(new_status);
		}
	}

	/// Register a function to be called whenever the status of this ASB changes.
	pub fn add_status_listener(&self, fun: Arc<dyn AsbStatusListener>) -> u32 {
		// Add function to listeners vec.
		let mut listeners = self.status_listeners.write().unwrap();
		let id = rand::random();
		let f = fun.clone();
		listeners.push((id, fun));

		// Call the function immediately with current status.
		let status = self.get_connection_status();
		thread::spawn(move || f(status));

		// Return ID to user so they can remove listener later
		id
	}

	/// Remove the listener identified with `id`, returning `true` if it exists.
	pub fn remove_status_listener(&self, id: u32) -> bool {
		let mut listeners = self.status_listeners.write().unwrap();
		if let Some(idx) = listeners.iter().position(|f| f.0 == id) {
			// Swap remove since order is not important.
			listeners.swap_remove(idx);

			true
		} else {
			false
		}
	}

	/// Create a new thread for each status listener and call them with `status`.
	fn call_status_listeners(&self, status: AsbConnStatus) {
		let listeners = self.status_listeners.read().unwrap();
		for listener in listeners.iter() {
			let f = listener.1.clone();
			thread::spawn(move || f(status));
		}
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
	pub fn new_reader<T: for<'de> Deserialize<'de> + Send + 'static>(
		&self,
		topic: &Topic<T>,
	) -> Result<AsbReader<T>, CalError> {
		Ok(AsbReader(
			self.connection.create_reader(topic, &self.config)?,
		))
	}

	/// Create a new [AsbWriter] for the given [Topic].
	pub fn new_writer<T: Serialize>(&self, topic: &Topic<T>) -> Result<AsbWriter<T>, CalError> {
		Ok(AsbWriter(
			self.connection.create_writer(topic, &self.config)?,
		))
	}
}

/// Reliability types for a CAL.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ReliabilityQos {
	Reliable,
	BestEffort,
}
impl Default for ReliabilityQos {
	fn default() -> Self {
		ReliabilityQos::BestEffort
	}
}

/// Quality-of-Service settings for the CAL.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct QosSettings {
	time_based_filter: Option<Duration>,
	reliability: ReliabilityQos,
	expiration: Option<Duration>,
	buffer: usize,
}
impl Default for QosSettings {
	fn default() -> Self {
		QosSettings {
			time_based_filter: None,
			reliability: ReliabilityQos::default(),
			expiration: None,
			buffer: 1,
		}
	}
}

/// CAL topic. A combination of name, QoS, and a message type.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct Topic<T> {
	name: String,
	pub qos: QosSettings,
	message_type: PhantomData<T>,
}
// TODO: Restrict T to be a valid type for sending (minimum serde, possibly specific message trait)
impl<T> Topic<T> {
	pub fn new(name: &str, qos: QosSettings) -> Result<Self, CalError> {
		// Restrict topic names to ASCII alphanumeric to minimize potential issues with ASB transports.
		if name.contains(|c: char| !c.is_ascii_alphanumeric()) {
			return Err(CalError::topic_err(format!(
				"Topic \"{name}\" contains non-alphanumeric characters."
			)));
		}

		Ok(Topic {
			name: name.to_string(),
			qos,
			message_type: PhantomData,
		})
	}

	/// Return current name as [str].
	pub fn name(&self) -> &str {
		&self.name
	}

	/// Set a new name for this topic. If `new_name` is invalid, then this topic is not modified.
	pub fn set_name(&mut self, new_name: &str) -> Result<(), CalError> {
		if new_name.contains(|c: char| !c.is_ascii_alphanumeric()) {
			return Err(CalError::topic_err(format!(
				"Topic \"{new_name}\" contains non-alphanumeric characters."
			)));
		}

		// Reuse string.
		self.name.clear();
		self.name.push_str(new_name);

		Ok(())
	}
}

pub struct AsbReader<T>(networks::AsbReader<T>);

pub struct AsbWriter<T>(networks::AsbWriter<T>);

#[cfg(test)]
mod test {
	use super::*;
	use crate::config::{NetworkConfig, NetworkKind, ServiceConfig};
	use std::collections::HashMap;
	use toml::Table;

	fn new_asb() -> Asb {
		let mut networks = HashMap::new();
		networks.insert(
			"null".to_string(),
			NetworkConfig {
				kind: NetworkKind::Null,
				params: Table::new(),
			},
		);

		let mut services = HashMap::new();
		services.insert(
			"my_service".to_string(),
			ServiceConfig {
				service_uuid: None,
				network: "null".to_string(),
			},
		);

		let config: AsbConfig = AsbConfig {
			system_uuid: None,
			networks,
			services,
		};

		Asb::new("my_service", config).unwrap()
	}

	/// Test that a status listener is correctly called for each status.
	#[test]
	fn status_listener() {
		use std::sync::atomic::{AtomicBool, AtomicUsize};

		// Create ASB and manually set the status to ensure consistency.
		let asb = new_asb();
		asb.status
			.store(AsbConnStatus::Initializing as usize, Ordering::Relaxed);

		// Variables for this thread
		let call_count = Arc::new(AtomicUsize::default());
		let init_hit = Arc::new(AtomicBool::default());
		let norm_hit = Arc::new(AtomicBool::default());
		let degr_hit = Arc::new(AtomicBool::default());
		let inop_hit = Arc::new(AtomicBool::default());
		let fail_hit = Arc::new(AtomicBool::default());

		// Variables for listener thread
		let count = call_count.clone();
		let init = init_hit.clone();
		let norm = norm_hit.clone();
		let degr = degr_hit.clone();
		let inop = inop_hit.clone();
		let fail = fail_hit.clone();

		// Add the listener.
		asb.add_status_listener(Arc::new(move |status| {
			count.fetch_add(1, Ordering::Relaxed);
			match status {
				AsbConnStatus::Initializing => init.store(true, Ordering::Relaxed),
				AsbConnStatus::Normal => norm.store(true, Ordering::Relaxed),
				AsbConnStatus::Degraded => degr.store(true, Ordering::Relaxed),
				AsbConnStatus::Inoperable => inop.store(true, Ordering::Relaxed),
				AsbConnStatus::Failed => fail.store(true, Ordering::Relaxed),
			};
		}));
		asb.set_connection_status(AsbConnStatus::Normal);
		asb.set_connection_status(AsbConnStatus::Degraded);
		asb.set_connection_status(AsbConnStatus::Inoperable);
		asb.set_connection_status(AsbConnStatus::Failed);

		// Ensure listener was called the correct number of times.
		while call_count.load(Ordering::Acquire) != 5 {
			std::hint::spin_loop();
		}

		// Check that every state was reached
		assert!(init_hit.load(Ordering::Acquire));
		assert!(norm_hit.load(Ordering::Acquire));
		assert!(degr_hit.load(Ordering::Acquire));
		assert!(inop_hit.load(Ordering::Acquire));
		assert!(fail_hit.load(Ordering::Acquire));
	}
}
