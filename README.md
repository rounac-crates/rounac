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
| `CAL-016024` | Y | Handled by message bindings | N |
| `CAL-016027` | Y | Handled by message bindings | N |
| `CAL-016028` | Y | Handled by message bindings | N |
| `CAL-016029` | Y | Handled by message bindings | N |
| `CAL-016477` | Y | Handled by `uuid` crate | N |
| `CAL-016479` | N | Choosing to use UUIDv4 | N |
| `CAL-005181` | Y | Choosing to use UUIDv4 | N |
| `CAL-005201` | Y | Just implement `new()` or something | N |
| `CAL-005202` | Y | Make sure the primary ASB connection object doesn't conflict with itself. | N |
| `CAL-005203` | Y | Methods to return required values. | N |
| `CAL-005204` | Y | Make a custom result alias and error type. | N |
| `CAL-005208` | Y | Take a type parameter when first creating the topic. | N |
| `CAL-005210` | Maybe | Per-topic QoS (is this even possible with standard pub-sub connections?) | N |
| `CAL-016033` | Y | Handled by message bindings | N |
| `CAL-016035` | Y | Handled by message bindings | N |
| `CAL-005254` | Y | Handled by message bindings | N |
| `CAL-005264` | Y | Handled by message bindings | N |
| `CAL-005267` | N | Choice types must be initialized with a valid choice. `Default` is not implemented. | N |
| `CAL-016038` | N | Enums must be initialized with a valid variant. `Default` is not implemented. | N |
| `CAL-005275` | Y | Standard Rust scoping/lifetime. | N |
| `CAL-005290` | Y | Standard `Option` semantics | N |
| `CAL-005293` | Y | Standard `Option` semantics | N |
| `CAL-005294` | Y | Standard `Option` semantics | N |
| `CAL-005296` | Y | Standard `Option` semantics | N |
| `CAL-005364` | Y | Figure out what info topic needs and require as parameter for creating Writer. | N |
| `CAL-005368` | Y | See previous comment. | N |
| `CAL-005369` | Y | Return custom error type with write error (could just wrap io error). | N |
| `CAL-016043` | Y | See previous comment. | N |
| `CAL-005374` | Y | Figure out what info topic needs and require as parameter for creating Reader. | N |
| `CAL-005378` | Y | See previous comment. | N |
| `CAL-005379` | Y | Either have a mode selector or separate constructors for callback vs polling. | N |
| `CAL-005380` | Y | See previous comment. | N |
| `CAL-005391` | Y | Trivially achievable. | N |
| `CAL-005392` | Y | Utilize `Arc` to save space and permit flexibility. | N |
| `CAL-005394` | Y | Best effort will be made. | N |
| `CAL-016044` | Y | Utilize buffer either in reader or the listener/callback mechanism. Perhaps an overwriting ring buffer? | N |
| `CAL-016045` | Y | May just remove immediately depending where messages are stoed. | N |
| `CAL-016046` | Y | See `Arc` comment above. | N |
| `CAL-005396` | Y | Trivially achievable. | N |
| `CAL-016049` | Y | May be a tad annoying if forced to use tokio (timeout does exist though) | N |
| `CAL-016050` | Y | Return custom error or avoid entirely with separate readers for each type. | N |
| `CAL-016052` | Y | Trivially achievable. | N |
| `CAL-005431` | Y | Trivially achievable. | N |
| `CAL-005434` | Maybe | This seems like a broker/protocol specific capability, so may not support for every connection type. | N |
| `CAL-016076` | Maybe | See previous comment. | N |
| `CAL-005437` | Y | Trivially achievable. | N |
| `CAL-015746` | Y | Configuration option. | N |
| `CAL-005444` | Maybe | Buffer outgoing messages (if necessary). | N |
| `CAL-005445` | Maybe | If outgoing are buffered (see previous). | N |
| `CAL-016079` | Y | Ring buffer or something. | N |
| `CAL-016366` | Y | List of listeners called whenever status changes. | Y |


[1]: https://gitlab.com/open-arsenal/oms/standard
[2]: https://gitlab.com/open-arsenal/uci/standard
