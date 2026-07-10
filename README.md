# Rounac
The Rust [OMS][1] [UCI][2] Not-A-CAL; pronounced "Runic".

# CAL spec certs
| CERT ID | Planned compliance | Reason |
|---|---|---|
| `CAL-005179` | N/A | Not C++ |
| `CAL-005180` | N/A | Not Java |
| `CAL-016015` | Y | Ensure multi-threading supported and all clients can communicate. |
| `CAL-016024` | Y | Handled by message bindings |
| `CAL-016027` | Y | Handled by message bindings |
| `CAL-016028` | Y | Handled by message bindings |
| `CAL-016029` | Y | Handled by message bindings |
| `CAL-016477` | Y | Handled by `uuid` crate |
| `CAL-016479` | N | Choosing to use UUIDv4 |
| `CAL-005181` | Y | Choosing to use UUIDv4 |
| `CAL-005201` | Y | Just implement `new()` or something |
| `CAL-005202` | Y | Make sure the primary ASB connection object doesn't conflict with itself. |
| `CAL-005203` | Y | Methods to return required values. |
| `CAL-005204` | Y | Make a custom result alias and error type. |
| `CAL-005208` | Y | Take a type parameter when first creating the topic. |
| `CAL-005210` | Maybe | Per-topic QoS (is this even possible with standard pub-sub connections?) |
| `CAL-016033` | Y | Handled by message bindings |
| `CAL-016035` | Y | Handled by message bindings |
| `CAL-005254` | Y | Handled by message bindings |
| `CAL-005264` | Y | Handled by message bindings |
| `CAL-005267` | N | Choice types must be initialized with a valid choice. `Default` is not implemented. |
| `CAL-016038` | N | Enums must be initialized with a valid variant. `Default` is not implemented. |
| `CAL-005275` | Y | Standard Rust scoping/lifetime. |
| `CAL-005290` | Y | Standard `Option` semantics |
| `CAL-005293` | Y | Standard `Option` semantics |
| `CAL-005294` | Y | Standard `Option` semantics |
| `CAL-005296` | Y | Standard `Option` semantics |
| `CAL-005364` | Y | Figure out what info topic needs and require as parameter for creating Writer. |
| `CAL-005368` | Y | See previous comment. |
| `CAL-005369` | Y | Return custom error type with write error (could just wrap io error). |
| `CAL-016043` | Y | See previous comment. |
| `CAL-005374` | Y | Figure out what info topic needs and require as parameter for creating Reader. |
| `CAL-005378` | Y | See previous comment. |
| `CAL-005379` | Y | Either have a mode selector or separate constructors for callback vs polling. |
| `CAL-005380` | Y | See previous comment. |
| `CAL-005391` | Y | Trivially achievable. |
| `CAL-005392` | Y | Utilize `Arc` to save space and permit flexibility. |
| `CAL-005394` | Y | Best effort will be made. |
| `CAL-016044` | Y | Utilize buffer either in reader or the listener/callback mechanism. Perhaps an overwriting ring buffer? |
| `CAL-016045` | Y | May just remove immediately depending where messages are stoed. |
| `CAL-016046` | Y | See `Arc` comment above. |
| `CAL-005396` | Y | Trivially achievable. |
| `CAL-016049` | Y | May be a tad annoying if forced to use tokio (timeout does exist though) |
| `CAL-016050` | Y | Return custom error or avoid entirely with separate readers for each type. |
| `CAL-016052` | Y | Trivially achievable. |
| `CAL-005431` | Y | Trivially achievable. |
| `CAL-005434` | Maybe | This seems like a broker/protocol specific capability, so may not support for every connection type. |
| `CAL-016076` | Maybe | See previous comment. |
| `CAL-005437` | Y | Trivially achievable. |
| `CAL-015746` | Y | Configuration option. |
| `CAL-005444` | Maybe | Buffer outgoing messages (if necessary). |
| `CAL-005445` | Maybe | If outgoing are buffered (see previous). |
| `CAL-016079` | Y | Ring buffer or something. |
| `CAL-016366` | Y | List of listeners called whenever status changes. |


[1]: https://gitlab.com/open-arsenal/oms/standard
[2]: https://gitlab.com/open-arsenal/uci/standard
