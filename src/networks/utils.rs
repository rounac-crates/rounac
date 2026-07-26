//! General network utilities

use std::{
	cell::Cell,
	marker::PhantomData,
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
};

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
