# Rounac
The Rust [OMS][1] [UCI][2] Not-A-CAL; pronounced "Runic".

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
| `CAL-016015` | Y | Ensure multi-threading supported and all clients can communicate. | N |
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
| `CAL-005208` | Y | Take a type parameter when first creating the topic. | N |
| `CAL-005209` | Y | Client topics are mapped through various means to CAL topics. | Y |
| `CAL-005210` | Maybe | Per-topic QoS (is this even possible with standard pub-sub connections?) | N |
| `CAL-016033` | Y | Handled by message bindings | Y |
| `CAL-016035` | Partial | Message bindings utilize enums for abstract due to no polymorphism. | Partial |
| `CAL-005254` | Y | Handled by message bindings | Y |
| `CAL-005264` | Y | Handled by message bindings | Y |
| `CAL-005267` | N | Choice types must be initialized with a valid choice. `Default` is not implemented. | N |
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
| `CAL-005379` | Y | Either have a mode selector or separate constructors for callback vs polling. | N |
| `CAL-005380` | Y | `AsbReader::read()`, `AsbReader::read_timeout()`, `AsbReader::try_read()` | Y |
| `CAL-005391` | Y | Trivially achievable. | N |
| `CAL-005392` | Y | Utilize `Arc` to save space and permit flexibility. | N |
| `CAL-005394` | Y | `AsbReader` resources are all initialized and ready when created. | Y |
| `CAL-016044` | Y | `AsbReader` has background thread that stores messages in a buffer. | Y |
| `CAL-016045` | Y | May just remove immediately depending where messages are stored. | N |
| `CAL-016046` | Y | See `Arc` comment above. | N |
| `CAL-005396` | Y | Trivially achievable. | N |
| `CAL-016049` | Y | `AsbReader::read_timeout()` provides this functionality. | Y |
| `CAL-016050` | Y | Return custom error or avoid entirely with separate readers for each type. | N |
| `CAL-016052` | Y | Message is removed from buffer to give to CAL client. | Y |
| `CAL-005431` | Y | Trivially achievable. | N |
| `CAL-005434` | Maybe | This seems like a broker/protocol specific capability, so may not support for every connection type. | N |
| `CAL-016076` | Maybe | See previous comment. | N |
| `CAL-005437` | Y | Trivially achievable. | N |
| `CAL-015746` | Y | Reader/Writer use topic QoS currently, but QoS isn't yet configurable. | Partial |
| `CAL-005444` | Maybe | Buffer outgoing messages (if necessary). | N |
| `CAL-005445` | Maybe | If outgoing are buffered (see previous). | N |
| `CAL-016079` | Y | An overwriting ring buffer is used. | Y |
| `CAL-016366` | Y | All logic to call listeners on change exists, but nothing changes status. | Partial |


[1]: https://gitlab.com/open-arsenal/oms/standard
[2]: https://gitlab.com/open-arsenal/uci/standard
