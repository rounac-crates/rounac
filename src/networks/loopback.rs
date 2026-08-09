//! Loopback network.
//!

use crate::{CalError, networks::utils::RingMaster};
use std::{
	collections::HashMap,
	sync::{
		Arc, RwLock,
		atomic::{AtomicBool, AtomicU16, Ordering},
	},
	time::Instant,
};

/// Loopback ASB that facilitates local-only message passing.
#[derive(Default)]
pub(super) struct LoopbackAsb {
	pub readers: RwLock<HashMap<String, (Arc<dyn RingMaster>, AtomicU16, String)>>,
	shutdown_fuse: AtomicBool,
}
impl LoopbackAsb {
	pub fn new() -> Self {
		LoopbackAsb::default()
	}

	/// Publish to readers on this ASB.
	pub fn publish(&self, topic: &str, data: &[u8]) -> Result<(), CalError> {
		if self.shutdown_fuse.load(Ordering::Acquire) {
			return Err(CalError::net_err("ASB has been shut down."));
		}

		self.handle_msg(topic, data);

		Ok(())
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
		let last_reader = {
			let mut readers = self.readers.write().unwrap();
			// If this is the last reader for the topic, remove entry
			let Some(t) = readers.get(topic) else {
				return Err(());
			};

			if t.1.fetch_sub(1, Ordering::Acquire) == 1 {
				// Shutdown the ringmaster just in case.
				t.0.shutdown();

				// SAFETY: `.get` already succeeded above.
				Some(readers.remove(topic).unwrap())
			} else {
				None
			}
		};

		Ok(last_reader.is_some())
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
