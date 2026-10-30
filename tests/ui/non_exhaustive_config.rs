// Configurations are built with `new()` or `default()` and `with_*`, so
// that fields can be added later.
use groundlink::PusConfig;

fn main() {
    let _ = PusConfig {
        tm_time_len: 4,
        ..PusConfig::default()
    };
}
