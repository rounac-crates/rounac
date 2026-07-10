//! Rounac
//!
//! The Rust [OMS][1] [UCI][2] Not-A-CAL; pronounced "Runic".
//!
//! [1]: https://gitlab.com/open-arsenal/oms/standard
//! [2]: https://gitlab.com/open-arsenal/uci/standard

use std::{
	default::Default,
	sync::{
		RwLock,
		atomic::{AtomicUsize, Ordering},
	},
	thread,
	time::Duration,
};
use uuid::Uuid;

struct CalError;

/// Possible states of the ASB.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AsbConnStatus {
	Initializing,
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

/// Abstract Service Bus.
struct Asb {
	system_uuid: Uuid,
	service_uuid: Uuid,
	/// The current status as an integer representing a variant of [AsbConnStatus].
	status: AtomicUsize,
	/// Vector of `(id, fn)` where `id` is a random number to remove `fn` later.
	status_listeners: RwLock<Vec<(u32, fn(AsbConnStatus))>>,
}
impl Asb {
	/// Get an initialized ASB for the client with the name `service_name`.
	fn new(service_name: &str) -> Result<Self, CalError> {
		Err(CalError)
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
	pub fn add_status_listener(&self, fun: fn(AsbConnStatus)) -> u32 {
		// Add function to listeners vec.
		let mut listeners = self.status_listeners.write().unwrap();
		let id = rand::random();
		listeners.push((id, fun));

		// Call the function immediately with current status.
		let f = fun.clone();
		let status = self.get_connection_status();
		thread::spawn(move || f(status));

		// Return ID to user so they can remove listener later
		id
	}

	/// Remove the listener identified with `id`, returning `true` if it exists.
	pub fn remove_status_listener(&self, id: u32) -> bool {
		let mut listeners = self.status_listeners.write().unwrap();
		if let Some(idx) = listeners.iter().position(|&f| f.0 == id) {
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
			let f = listener.1;
			thread::spawn(move || f(status));
		}
	}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReliabilityQos {
	Reliable,
	BestEffort,
}
impl Default for ReliabilityQos {
	fn default() -> Self {
		ReliabilityQos::BestEffort
	}
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct QosSettings {
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

struct Topic {}
