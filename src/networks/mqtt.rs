//! Types and utilities for MQTT transport.
//!

use super::utils::RingMaster;
use crate::{
	CalError,
	config::{NetworkConfig, NetworkKind, params::ParamTool},
};
use rumqttc::{AsyncClient, MqttOptions, QoS};
use std::{
	collections::HashMap,
	sync::{
		Arc, RwLock,
		atomic::{AtomicBool, AtomicU16, Ordering},
	},
	time::Instant,
};
use tokio::runtime::Handle;

pub(crate) fn get_mqtt_opts(network: &NetworkConfig) -> Result<MqttOptions, CalError> {
	// Verify this network is the correct type.
	if network.kind != NetworkKind::Mqtt {
		return Err(CalError::config_err("Expected network kind \"mqtt\"."));
	}

	let params = ParamTool(&network.params);

	// Get parameters
	let host = params.get_str("host")?.unwrap_or("localhost");
	let port = params.get_int("port")?.unwrap_or(1883);
	let client_id = params.get_str("client_id")?.unwrap_or_default();
	let user = params.get_str("username")?;
	let pass = params.get_str("password")?;

	// Both username and password must be present if either is.
	if user.is_some() && pass.is_none() || pass.is_some() && user.is_none() {
		return Err(CalError::config_err(
			"Expected \"username\" and \"password\", or neither.",
		));
	}

	let mut opts = MqttOptions::new(client_id, host, port as u16);
	if user.is_some() && pass.is_some() {
		opts.set_credentials(user.unwrap(), pass.unwrap());
	}

	Ok(opts)
}

pub(super) struct MqttAsb {
	pub rt_handle: Handle,
	pub client: AsyncClient,
	// Tuple of (ringmaster, reader_count)
	pub readers: RwLock<HashMap<String, (Arc<dyn RingMaster>, AtomicU16)>>,
	shutdown_fuse: AtomicBool,
}
impl MqttAsb {
	pub fn new(rt_handle: Handle, client: AsyncClient) -> Self {
		MqttAsb {
			rt_handle,
			client,
			readers: RwLock::new(HashMap::new()),
			shutdown_fuse: AtomicBool::default(),
		}
	}

	/// Publish to the ASB using the given parameters.
	pub fn publish<S, V>(&self, topic: S, qos: QoS, retain: bool, data: V) -> Result<(), CalError>
	where
		S: Into<String>,
		V: Into<Vec<u8>>,
	{
		if self.shutdown_fuse.load(Ordering::Acquire) {
			return Err(CalError::net_err("ASB has been shut down."));
		}

		self.rt_handle
			.block_on(self.client.publish(topic, qos, retain, data))
			.map_err(CalError::net_err)
	}

	pub fn get_clone_for(&self, topic: &str) -> Option<Arc<dyn RingMaster>> {
		self.readers.read().unwrap().get(topic).map(|v| v.0.clone())
	}

	/// Increment internal reader count, returning [Ok] if successful.
	pub fn add_reader(&self, topic: &str) -> Result<(), ()> {
		if let Some(t) = self.readers.read().unwrap().get(topic) {
			t.1.fetch_add(1, Ordering::Acquire);
			Ok(())
		} else {
			Err(())
		}
	}

	/// Decrement internal reader count, where return of `Ok(true)` indicates this
	/// was the last reader.
	pub fn del_reader(&self, topic: &str) -> Result<bool, ()> {
		// Get write lock to avoid a lock-unlock-lock situation when this is the last
		// reader. Reader deletion is not expected to occur frequently.
		let mut readers = self.readers.write().unwrap();
		if let Some(t) = readers.get(topic) {
			let v = t.1.fetch_sub(1, Ordering::Acquire);
			// If this was the last reader for the topic, remove entry
			if v == 0 {
				readers.remove(topic);

				Ok(true)
			} else {
				Ok(false)
			}
		} else {
			Err(())
		}
	}

	/// Passes `data` along to readers of the given `topic`.
	pub fn handle_msg(&self, topic: &str, data: &[u8]) {
		// If shutdown, don't even lock readers.
		if self.shutdown_fuse.load(Ordering::Acquire) {
			return;
		}

		let readers = self.readers.read().unwrap();
		if let Some(ring_master) = readers.get(topic) {
			ring_master.0.distribute_msg(Instant::now(), data);
		}
	}

	pub fn shutdown(&self) {
		let mut readers = self.readers.write().unwrap();
		for reader in readers.values() {
			// Shutdown readers
			reader.0.shutdown();
		}

		readers.clear();
		self.shutdown_fuse.store(true, Ordering::Release);
	}
}
