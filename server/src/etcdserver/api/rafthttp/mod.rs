use slog::o;
use slog::Drain;
mod transport;
mod snap;
mod util;
pub mod error;
pub mod types;
pub mod v2state;
mod peer_status;
mod remote;
mod url_pick;
mod pipeline;
pub mod http;
pub mod peer;
mod snapshot_sender;
pub mod test_util;
pub mod stream;

#[allow(dead_code)]
pub fn default_logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!())
}