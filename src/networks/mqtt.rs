//! Types and utilities for MQTT transport.
//!

use super::utils::RingMaster;
use crate::CalError;
use rumqttc::{AsyncClient, QoS};
use std::{
	collections::HashMap,
	sync::{
		Arc, RwLock,
		atomic::{AtomicU16, Ordering},
	},
	time::Instant,
};
use tokio::runtime::Handle;

pub(super) struct MqttAsb {
	pub rt_handle: Handle,
	pub client: AsyncClient,
	// Tuple of (ringmaster, reader_count)
	pub readers: RwLock<HashMap<String, (Arc<dyn RingMaster>, AtomicU16)>>,
}
impl MqttAsb {
	pub fn new(rt_handle: Handle, client: AsyncClient) -> Self {
		MqttAsb {
			rt_handle,
			client,
			readers: RwLock::new(HashMap::new()),
		}
	}

	/// Publish to the ASB using the given parameters.
	pub fn publish<S, V>(&self, topic: S, qos: QoS, retain: bool, data: V) -> Result<(), CalError>
	where
		S: Into<String>,
		V: Into<Vec<u8>>,
	{
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
		let readers = self.readers.read().unwrap();
		if let Some(ring_master) = readers.get(topic) {
			ring_master.0.distribute_msg(Instant::now(), data);
		}
	}
}
