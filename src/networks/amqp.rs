//! AMQPRS related utilities

use crate::{
	config::{NetworkConfig, NetworkKind, QosSettings, ReliabilityQos, params::ParamTool},
	error::CalError,
	networks::utils::{AsbConnStatus, RingMaster, StatusCallbackManager},
};
use amqprs::{
	Ack, BasicProperties, Cancel, Close, CloseChannel, Deliver, Nack, Return,
	callbacks::{ChannelCallback, ConnectionCallback},
	channel::{BasicAckArguments, BasicCancelArguments, Channel},
	connection::{Connection, OpenConnectionArguments},
	consumer::AsyncConsumer,
	error::Error,
};
use async_trait::async_trait;
use std::{
	collections::HashMap,
	sync::{
		Arc, RwLock,
		atomic::{AtomicBool, AtomicU16, Ordering},
	},
	time::Instant,
};
use tokio::runtime::Handle;

/// Get the necessary config params to create AMQP connection for `net_name`.
pub fn open_args_for_net(network: &NetworkConfig) -> Result<OpenConnectionArguments, CalError> {
	// Verify this network is the correct type.
	if network.kind != NetworkKind::Amqp {
		return Err(CalError::config_err("Expected network kind \"amqp\"."));
	}

	let params = ParamTool(&network.params);

	// Get parameters
	let host = params.get_str("host")?.unwrap_or("localhost");
	let port = params.get_int("port")?.unwrap_or(5672);
	let user = params.get_str("username")?.unwrap_or("guest");
	let pass = params.get_str("password")?.unwrap_or("guest");

	Ok(OpenConnectionArguments::new(host, port as u16, user, pass))
}

pub(crate) struct AmqpAsb {
	pub rt_handle: Handle,
	pub conn: Connection,
	pub chan: Channel,
	pub exchange: Option<String>,
	// For message handling. `.2` is consumer tag.
	pub readers: RwLock<HashMap<String, (Arc<dyn RingMaster>, AtomicU16, String)>>,
	shutdown_fuse: AtomicBool,
}
impl AmqpAsb {
	pub fn new(
		rt_handle: Handle,
		conn: Connection,
		chan: Channel,
		exchange: Option<String>,
	) -> Self {
		AmqpAsb {
			rt_handle,
			conn,
			chan,
			exchange,
			readers: RwLock::new(HashMap::new()),
			shutdown_fuse: AtomicBool::default(),
		}
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

		if let Some(last) = last_reader {
			// Remove consumer
			let args = BasicCancelArguments {
				consumer_tag: last.2,
				no_wait: true,
			};
			_ = self.rt_handle.block_on(self.chan.basic_cancel(args));

			Ok(true)
		} else {
			Ok(false)
		}
	}

	/// Passes `data` along to readers of the given `topic`.
	pub fn handle_msg(&self, topic: &str, data: &[u8]) {
		// If shutdown, don't even lock readers
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

pub struct AmqpConsumer {
	pub qos: QosSettings,
	pub asb: Arc<AmqpAsb>,
	pub topic: String,
}

#[async_trait]
impl AsyncConsumer for AmqpConsumer {
	async fn consume(
		&mut self,
		chan: &Channel,
		deliver: Deliver,
		_: BasicProperties,
		data: Vec<u8>,
	) {
		// ACK first to broker knows we got the message.
		if self.qos.reliability == ReliabilityQos::Reliable {
			let ack_args = BasicAckArguments::new(deliver.delivery_tag(), false);

			// Try to ACK some number of times before giving up.
			const MAX_ACK_TRIES: usize = 2;
			for _ in 0..MAX_ACK_TRIES {
				if chan.basic_ack(ack_args.clone()).await.is_ok() {
					break;
				}
			}
		}

		// Pass along message for distribution.
		self.asb.handle_msg(&self.topic, &data);
	}
}

// TODO: If reconnection desired, `AmqpAsb` needs to be [RwLock]'d and [Arc]'d
//       plus track consumers (would require `dyn` over [AmqpConsumer]).
pub(crate) struct ConnCb {
	pub(crate) status_manager: Arc<StatusCallbackManager>,
}
#[async_trait]
impl ConnectionCallback for ConnCb {
	async fn close(&mut self, connection: &Connection, close: Close) -> Result<(), Error> {
		// TODO: Have a way to relay error condition to [AsbConnection].
		eprintln!(
			"ERROR: Connection({}) closed by server: {close}",
			connection.connection_name()
		);

		// If connection is closed, then ASB connection is in a failure state.
		// TODO: If reconnect logic is ever added for AMQP, change this to
		// `AsbConnStatus::Inoperable`.
		self.status_manager.set_status(AsbConnStatus::Failed);

		Ok(())
	}

	async fn blocked(&mut self, _: &Connection, _: String) {}

	async fn unblocked(&mut self, _: &Connection) {}

	async fn secret_updated(&mut self, _: &Connection) {}
}

// If desired to re-opening channels, see TODO on [ConnCb] for general
// refactors required.
pub(crate) struct ChanCb {
	pub(crate) status_manager: Arc<StatusCallbackManager>,
}
#[async_trait]
impl ChannelCallback for ChanCb {
	async fn close(&mut self, chan: &Channel, close_channel: CloseChannel) -> Result<(), Error> {
		// TODO: Have a way to relay error condition to [AsbConnection].
		eprintln!(
			"ERROR: Channel({}) closed by server: {close_channel}",
			chan.channel_id()
		);

		// If channel is closed, then ASB connection is active but reads/writes will
		// not work.
		self.status_manager.set_status(AsbConnStatus::Inoperable);

		Ok(())
	}

	async fn cancel(&mut self, chan: &Channel, cancel: Cancel) -> Result<(), Error> {
		// TODO: Have a way to relay error condition to [AsbReader] or [AsbConnection].
		eprintln!(
			"ERROR: Channel({}) consumer cancelled by server: {cancel:?}",
			chan.channel_id()
		);
		Ok(())
	}

	async fn flow(&mut self, _: &Channel, _: bool) -> Result<bool, Error> {
		Ok(true)
	}

	async fn publish_ack(&mut self, _: &Channel, _: Ack) {}

	async fn publish_nack(&mut self, _: &Channel, _: Nack) {
		// TODO: If topic QoS dictates reliable, figure out how to get writer to
		//       re-send if `nack.requeue` is false.
	}

	async fn publish_return(&mut self, _: &Channel, _: Return, _: BasicProperties, _: Vec<u8>) {}
}
