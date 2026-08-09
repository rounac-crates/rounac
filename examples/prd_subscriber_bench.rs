//! Position Report Detailed benchmark subscriber.

use chrono::Utc;
use rounac::Asb;
use rounac_uci::v2_5::elements::PositionReportDetailed;
use std::time::{Duration, Instant};

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

[networks.artemis]
kind = "mqtt"
host = "localhost"
username = "artemis"
password = "artemis"
"#;

fn main() {
	// This must match the service name in the config to apply the configuration.
	const SVC_NAME: &str = "prd_benchmark_subscriber";

	// Load the configuration and create the ASB + reader.
	let config = CONFIG.parse().unwrap();
	let asb = Asb::new(SVC_NAME, config).unwrap();
	let reader = asb.new_reader::<PositionReportDetailed>("prd").unwrap();

	// Get the UCI schema version.
	let schema_ver = rounac_uci::v2_5::SCHEMA_VERSION.to_owned();

	// Loop and send a few status messages.
	let listen_time = Duration::from_mins(15);
	let start = Instant::now();
	let mut now = Duration::ZERO;
	let mut remaining = listen_time;
	let mut last_print = Duration::ZERO;

	// Measure receive delay based on header.
	let mut min_delay = Duration::MAX;
	let mut max_delay = Duration::ZERO;

	println!(
		"Listening for status messages for {}s.",
		listen_time.as_secs()
	);
	let mut count = 0;
	let mut read_total = 0;
	let mut last_count = 0;
	while !remaining.is_zero() {
		let read_start = Instant::now();
		match reader.read_timeout(remaining) {
			// Print some information and check schema if message received.
			Ok(Some(msg)) => {
				read_total += read_start.elapsed().as_nanos();
				// Check schema version in header just to do something.
				if msg.message_header.schema_version != schema_ver {
					eprintln!("Status has mismatched schema version!!");
				}

				// Check time delay
				let delay = (Utc::now() - msg.message_header.timestamp)
					.to_std()
					.unwrap();
				if delay < min_delay {
					min_delay = delay;
				}
				if delay > max_delay {
					max_delay = delay;
				}

				if now > (last_print + listen_time / 10) {
					let avg_read_time = read_total / (count - last_count);
					println!(
						"Time {}s - Count: {}, Min delay: {:12}ns, Max delay: {:12}ns, Avg read (last period): {}ns",
						now.as_secs(),
						count,
						min_delay.as_nanos(),
						max_delay.as_nanos(),
						avg_read_time
					);

					last_print = now;
					read_total = 0;
					last_count = count;
				}

				count += 1;
			}
			// No message no error just keep going.
			Ok(None) => {}
			// If error stop trying to receive.
			Err(e) => {
				eprintln!("Reader error: {e}");
				break;
			}
		};

		now = start.elapsed();
		remaining = listen_time.saturating_sub(now);
	}

	println!("Received {count} messages.");
	println!(
		"Min delay: {:12}ns, Max delay: {:12}ns",
		min_delay.as_nanos(),
		max_delay.as_nanos()
	);
}
