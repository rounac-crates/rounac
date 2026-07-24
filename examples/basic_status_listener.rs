//! Basic status listener
//!
//! This example will demonstrate a basic listener for `ServiceStatus` messages
//! from an AMQP (RabbitMQ) network.

use rounac::{Asb, Topic};
use rounac_uci::v2_5::elements::ServiceStatus;
use std::time::Duration;

// Simple configuration that will utilize the `amqp` network type.
const CONFIG: &str = r#"
system_uuid = "00000000-0000-0000-0000-000000000000"

[services.basic_status_subscribe]
service_uuid = "00000000-0000-4000-8000-0123456789AB"
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

fn main() {
	// This must match the service name in the config to apply the configuration.
	const SVC_NAME: &str = "basic_status_subscribe";

	// Load the configuration and create the ASB + reader.
	let config = CONFIG.parse().unwrap();
	let asb = Asb::new(SVC_NAME, config).unwrap();
	let topic = Topic::<ServiceStatus>::new("status").unwrap();
	let mut reader = asb.new_reader(&topic).unwrap();

	// How long to listen for
	let listen_time = Duration::from_secs(10);

	// Get the UCI schema version.
	let schema_ver = rounac_uci::v2_5::SCHEMA_VERSION;

	// Prepare listener
	let l_id = reader.add_listener(move |msg| {
		println!("Received status from {}!", msg.message_data.service_id.uuid);

		// Check schema version in header just to do something.
		if msg.message_header.schema_version != schema_ver {
			eprintln!("Status has mismatched schema version!!");
		}
	});

	println!(
		"Listening for status messages for {}s.",
		listen_time.as_secs()
	);
	// Sleep for desired time since listener will run in the background.
	std::thread::sleep(listen_time);

	// Not necessary here but good habit.
	reader.remove_listener(l_id);
}
