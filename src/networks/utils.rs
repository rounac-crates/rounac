//! General network utilities

use std::{
	cell::Cell,
	marker::PhantomData,
	sync::{
		Arc, RwLock,
		atomic::{AtomicUsize, Ordering},
	},
	thread,
};

/// The trait required to register a type with [Asb::add_status_listener].
pub trait AsbStatusListener: Send + Sync + 'static {
	/// Called immediately upon registration and on any subsequent status changes.
	fn on_status_change(&self, status: AsbConnStatus);
}
impl<T: Fn(AsbConnStatus) + Send + Sync + 'static> AsbStatusListener for T {
	fn on_status_change(&self, status: AsbConnStatus) {
		self(status)
	}
}

/// Possible states of the ASB.
///
/// The descriptions for each are taken from the OMS CAL specification
/// verbatim, with slight modifications for this implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AsbConnStatus {
	/// **THIS IS NEVER USED FOR ROUNAC**. [Asb::new] will not return if
	/// initialization is still occuring.
	// This should always be 0 so default [Asb] has a sensible value.
	// Should never be used since [Asb::new] won't return if not fully
	// initialized.
	Initializing = 0,
	/// CAL is functioning normally, where all functions can be used. All QoS
	/// settings, as applicable, are being met. Read and write methods will behave
	/// as normal.
	Normal,
	/// CAL is not functioning normally, where functions can be used but there may
	/// be limitations. Not all QoS settings are being satisfied. The CAL can send
	/// and receive but QoS settings are not being satisfied. Read and write
	/// methods will behave as normal.
	Degraded,
	/// CAL is unusable but may return to an operational state in the future. CAL
	/// is unable to send or receive messages and is attempting to recover. Read
	/// and write methods will return [CalError].
	Inoperable,
	/// CAL is unusable and return to an operational state is not possible.
	/// Identical to Inoperable state but recovery is not possible. Read and write
	/// methods will return [CalError].
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

pub struct StatusCallbackManager {
	/// The current status as an integer representing a variant of [AsbConnStatus].
	status: AtomicUsize,
	/// Vector of `(id, fn)` where `id` is a random number to remove `fn` later.
	status_listeners: RwLock<Vec<(u32, Arc<dyn AsbStatusListener>)>>,
}
impl StatusCallbackManager {
	pub fn new() -> Self {
		StatusCallbackManager {
			// For now, status always normal since connection errors if something fails.
			status: AtomicUsize::new(AsbConnStatus::Normal as usize),
			status_listeners: RwLock::new(Vec::new()),
		}
	}

	/// Get the current status value.
	pub fn get_status(&self) -> AsbConnStatus {
		// Safety: Connection status will only ever be set through `set_connection_status()` which guarantees a valid value.
		self.status.load(Ordering::Acquire).try_into().unwrap()
	}

	/// If `new_status` differs from current status, update status and notify listeners. Else ignore.
	pub(crate) fn set_status(&self, new_status: AsbConnStatus) {
		if self.get_status() != new_status {
			self.status.store(new_status as usize, Ordering::Release);
			self.call_status_listeners(new_status);
		}
	}

	/// Register a function to be called whenever the status changes.
	pub fn add_listener(&self, fun: impl AsbStatusListener) -> u32 {
		// Add function to listeners vec.
		let mut listeners = self.status_listeners.write().unwrap();
		let id = rand::random();
		let f = Arc::new(fun);
		listeners.push((id, f.clone()));

		// Call the function immediately with current status.
		let status = self.get_status();
		thread::spawn(move || f.on_status_change(status));

		// Return ID to user so they can remove listener later
		id
	}

	/// Remove the listener identified with `id`, returning `true` if it exists.
	pub fn remove_listener(&self, id: u32) -> bool {
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
	pub(crate) fn call_status_listeners(&self, status: AsbConnStatus) {
		let listeners = self.status_listeners.read().unwrap();
		for listener in listeners.iter() {
			let f = listener.1.clone();
			thread::spawn(move || f.on_status_change(status));
		}
	}
}

/// Posession counter.
///
/// Increments on [clone] and decrements on [drop].
// This should be [Send] but not [Sync]; PhantomData used to disallow Sync.
pub struct PossessCtr(Arc<AtomicUsize>, PhantomData<Cell<()>>);
impl PossessCtr {
	/// Get a new counter with an internal count of `1`.
	pub fn new() -> Self {
		PossessCtr(Arc::new(AtomicUsize::new(1)), PhantomData)
	}

	/// Returns `true` when this instance is the only one that exists.
	pub fn is_unique(&self) -> bool {
		self.0.load(Ordering::Acquire) == 1
	}
}
impl Clone for PossessCtr {
	fn clone(&self) -> Self {
		// Increment count then return with cloned [Arc].
		self.0.fetch_add(1, Ordering::Relaxed);
		PossessCtr(self.0.clone(), PhantomData)
	}
}
impl Drop for PossessCtr {
	fn drop(&mut self) {
		self.0.fetch_sub(1, Ordering::Release);
	}
}

#[cfg(test)]
mod possess_ctr_tests {
	use super::*;

	/// Test that a brand new possession counter is unique.
	#[test]
	fn new_is_unique() {
		let c = PossessCtr::new();

		assert!(c.is_unique());
	}

	/// Test that when more than 1 [PossessCtr] exist, neither is unique.
	#[test]
	fn clone_is_not_unique() {
		let c = PossessCtr::new();
		let c2 = c.clone();

		assert!(!c.is_unique());
		assert!(!c2.is_unique());
	}

	/// Test that when multiple [PossessCtr] exist then all but one are dropped,
	/// the one is unique again.
	#[test]
	fn unique_after_drop() {
		let c = PossessCtr::new();
		let c2 = c.clone();

		drop(c);

		assert!(c2.is_unique());
	}

	/// Empty test that will fail to compile if [PossessCtr] is not [Send].
	#[test]
	fn is_send() {
		fn assert_send<T: Send>() {}
		assert_send::<PossessCtr>();
	}
}

#[cfg(test)]
mod status_manager_tests {
	use super::*;

	/// Test that a status listener is correctly called for each status.
	#[test]
	fn status_listener() {
		use std::sync::atomic::{AtomicBool, AtomicUsize};

		// Create Status manager and set the status to ensure consistency.
		let mgr = StatusCallbackManager::new();
		mgr.set_status(AsbConnStatus::Initializing);

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
		mgr.add_listener(move |status| {
			match status {
				AsbConnStatus::Initializing => init.store(true, Ordering::Relaxed),
				AsbConnStatus::Normal => norm.store(true, Ordering::Relaxed),
				AsbConnStatus::Degraded => degr.store(true, Ordering::Relaxed),
				AsbConnStatus::Inoperable => inop.store(true, Ordering::Relaxed),
				AsbConnStatus::Failed => fail.store(true, Ordering::Relaxed),
			};
			count.fetch_add(1, Ordering::Relaxed);
		});
		mgr.set_status(AsbConnStatus::Normal);
		mgr.set_status(AsbConnStatus::Degraded);
		mgr.set_status(AsbConnStatus::Inoperable);
		mgr.set_status(AsbConnStatus::Failed);

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
