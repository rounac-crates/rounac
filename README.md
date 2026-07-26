# Rounac
The Rust [OMS][1] [UCI][2] Not-A-CAL; pronounced "Runic".

# Example
This is a basic example of subscribing to a topic, taken directly from the
`basic_status_subscribe` example in the repo but shortened slightly.

```rust
use rounac::{Asb, QosSettings, Topic};
use rounac_uci::v2_5::elements::ServiceStatus;
use std::time::{Duration, Instant};

// Minimal configuration for RabbitMQ.
const CONFIG: &str = r#"
[services.basic_status_subscribe]
network = "rabbit"
wire_format = "xml"

[networks.rabbit]
kind = "amqp"
host = "localhost"
port = 5672
username = "guest"
password = "guest"
"#;

fn main() {
    // Load the configuration and create the ASB + reader.
    let config = CONFIG.parse().unwrap();
    let asb = Asb::new("basic_status_subscribe", config).unwrap();
    let topic = Topic::<ServiceStatus>::new("status", QosSettings::default()).unwrap();
    let reader = asb.new_reader(&topic).unwrap();

    // Loop and send a few status messages.
    let listen_time = Duration::from_secs(10);
    let start = Instant::now();
    let mut now = Duration::ZERO;
    let mut remaining = listen_time;

    // Listen for `listen_time`.
    while !remaining.is_zero() {
        if let Ok(Some(msg)) = reader.read_timeout(remaining) {
            println!("Received status from {}!", msg.message_data.service_id.uuid);
        }

        now = start.elapsed();
        remaining = listen_time.saturating_sub(now);
    }
}
```

# Supported transports
## AMQP (in progress)
`amqprs`
## ZeroMQ (desired)
`zeromq` (Rust native) or `zmq` (libzmq wrapper).
## MQTT (desired)
`rumqttc`
## NATS (desired)
`async-nats`

# CAL spec certs
| CERT ID | Planned compliance | Reason | Implemented |
|---|---|---|---|
| `CAL-005179` | N/A | Not C++ | N |
| `CAL-005180` | N/A | Not Java | N |
| `CAL-016015` | Y | Yes but it may require a network/broker connection. | Y |
| `CAL-016024` | Y | Handled by message bindings | Y |
| `CAL-016027` | Y | Handled by message bindings | Y |
| `CAL-016028` | Y | Handled by message bindings | Y |
| `CAL-016029` | Y | Handled by message bindings | Y |
| `CAL-016477` | Y | Handled by `uuid` crate | Y |
| `CAL-016479` | N | Choosing to use UUIDv4 | N |
| `CAL-005181` | Y | Choosing to use UUIDv4 | Y |
| `CAL-005201` | Y | `Asb::new()` | Y |
| `CAL-005202` | Y | Each `Asb::new()` returns a wholly separate instance. | Y |
| `CAL-005203` | Y | System and service UUIDs available. Others not yet. | Partial |
| `CAL-005204` | Y | `Asb::new()` returns a `CalError` if there is an issue initializing. | Y |
| `CAL-005208` | Y | A topic can take any type so long as every reader/writer uses the same type. | Y |
| `CAL-005209` | Y | Client topics are mapped through various means to CAL topics. | Y |
| `CAL-005210` | Y | Depends on the specific transport used, but supported. | Partial |
| `CAL-016033` | Y | Handled by message bindings | Y |
| `CAL-016035` | Partial | Message bindings utilize enums for abstract due to no polymorphism. | Partial |
| `CAL-005254` | Y | Handled by message bindings | Y |
| `CAL-005264` | Y | Handled by message bindings | Y |
| `CAL-005267` | N | Choices must be initialized with a valid choice. `Default` is not implemented. | N |
| `CAL-016038` | N | Enums must be initialized with a valid variant. `Default` is not implemented. | N |
| `CAL-005275` | Y | Standard Rust scoping/lifetime. | Y |
| `CAL-005290` | Y | Standard `Option` semantics | Y |
| `CAL-005293` | Y | Standard `Option` semantics | Y |
| `CAL-005294` | Y | Standard `Option` semantics | Y |
| `CAL-005296` | Y | Standard `Option` semantics | Y |
| `CAL-005364` | Y | `Asb::create_writer()` | Y |
| `CAL-005368` | Y | `AsbWriter` is associated with the topic used to create it. | Y |
| `CAL-005369` | Y | `AsbWriter::write()` returns `CalError` is the write failed. | Y |
| `CAL-016043` | Y | See previous comment. | Y |
| `CAL-005374` | Y | `Asb::create_reader()` | Y |
| `CAL-005378` | Y | `AsbReader` is associated with the topic used to create it. | Y |
| `CAL-005379` | Y | `AsbReader::add_listener()` and `AsbReader::remove_listener()` | Y |
| `CAL-005380` | Y | `AsbReader::read()`, `AsbReader::read_timeout()`, `AsbReader::try_read()` | Y |
| `CAL-005391` | Y | Trivially achievable. | Y |
| `CAL-005392` | Y | Utilize `Arc` to save space and permit flexibility. | Y |
| `CAL-005394` | Y | `AsbReader` resources are all initialized and ready when created. | Y |
| `CAL-016044` | Y | `AsbReader` has background thread that stores messages in a buffer. | Y |
| `CAL-016045` | Y | Technically the message is removed first but every listener will get it. | Y |
| `CAL-016046` | Y | Listener callbacks get an immutable reference to the message. | Y |
| `CAL-005396` | Y | `AsbReader::remove_listener()` | Y |
| `CAL-016049` | Y | `AsbReader::read_timeout()` provides this functionality. | Y |
| `CAL-016050` | Y | Returns `CalError` with kind `Illegal` when this happens. | Y |
| `CAL-016052` | Y | Message is removed from buffer to give to CAL client. | Y |
| `CAL-005431` | Y | Messages received within interval may be acknowledged per QoS, but are dropped. | Y |
| `CAL-005434` | Y* | Broker/protocol specific, possibly configuration-dependent too. | Y* |
| `CAL-016076` | Y* | Broker/protocol specific, but messages are sent/received in the order given. | Y* |
| `CAL-005437` | Y | `AsbReader` will ignore and discard any messages past expiration. | Y |
| `CAL-015746` | Y | Reader/Writer use topic QoS currently, but QoS isn't yet configurable. | Partial |
| `CAL-005444` | Maybe | Buffer outgoing messages (if necessary). | N |
| `CAL-005445` | Y | Overwriting ring buffers are used, but only reader uses currently. | Partial |
| `CAL-016079` | Y | An overwriting ring buffer is used. | Y |
| `CAL-016366` | Y | All logic to call listeners on change exists, but nothing changes status. | Partial |


[1]: https://gitlab.com/open-arsenal/oms/standard
[2]: https://gitlab.com/open-arsenal/uci/standard
