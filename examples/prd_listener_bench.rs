//! Position Report Detailed benchmark subscriber.

use rounac::{Asb, AsbListener};
use rounac_uci::v2_5::elements::PositionReportDetailed;
use std::{
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
	time::Duration,
};

const CONFIG: &str = r#"
[services.prd_benchmark_subscriber]
network = "rabbit"
wire_format = "xml"

[networks.rabbit]
kind = "amqp"
host = "localhost"
port = 5672
username = "guest"
password = "guest"
exchange = "rounac"
"#;

struct MsgCounter(AtomicUsize);
impl MsgCounter {
	fn new() -> Self {
		Self(AtomicUsize::new(0))
	}

	fn count(&self) -> usize {
		self.0.load(Ordering::Relaxed)
	}
}
impl AsbListener<PositionReportDetailed> for MsgCounter {
	fn on_msg(&self, msg: Arc<PositionReportDetailed>) {
		// Get the UCI schema version.
		let schema_ver = rounac_uci::v2_5::SCHEMA_VERSION.to_owned();

		// Check schema version to do something.
		if msg.message_header.schema_version != schema_ver {
			eprintln!("Status has mismatched schema version!!");
		}

		// Increment counter.
		self.0.fetch_add(1, Ordering::Relaxed);
	}
}

fn main() {
	// This must match the service name in the config to apply the configuration.
	const SVC_NAME: &str = "prd_benchmark_subscriber";

	// Load the configuration and create the ASB + reader.
	let config = CONFIG.parse().unwrap();
	let asb = Asb::new(SVC_NAME, config).unwrap();
	let reader = asb.new_reader::<PositionReportDetailed>("prd").unwrap();

	let ctr = Arc::new(MsgCounter::new());
	reader.add_listener(ctr.clone());

	// Loop and send a few status messages.
	let listen_time = Duration::from_mins(15);

	println!(
		"Listening for status messages for {}s.",
		listen_time.as_secs()
	);

	std::thread::sleep(listen_time);

	let count = ctr.count();
	println!("Received {count} messages.");
}
