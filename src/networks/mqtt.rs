//! Types and utilities for MQTT transport.
//!

use super::utils::RingMaster;
use rumqttc::AsyncClient;
use std::{
	collections::HashMap,
	sync::{Arc, RwLock},
	time::Instant,
};
use tokio::runtime::Handle;

pub(super) struct MqttAsb {
	pub rt_handle: Handle,
	pub client: AsyncClient,
	pub readers: RwLock<HashMap<String, Arc<dyn RingMaster>>>,
}
impl MqttAsb {
	pub fn new(rt_handle: Handle, client: AsyncClient) -> Self {
		MqttAsb {
			rt_handle,
			client,
			readers: RwLock::new(HashMap::new()),
		}
	}

	/// Passes `data` along to readers of the given `topic`.
	pub fn handle_msg(&self, topic: &str, data: &[u8]) {
		let readers = self.readers.read().unwrap();
		if let Some(ring_master) = readers.get(topic) {
			ring_master.distribute_msg(Instant::now(), data);
		}
	}
}
