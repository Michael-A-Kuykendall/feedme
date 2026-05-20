#![no_main]

use libfuzzer_sys::fuzz_target;
use feedme::Event;

// Fuzz `Event::from_raw_input` — the boundary where untrusted bytes enter the
// pipeline.  Any panic here is a bug; errors are expected and acceptable.
fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = Event::from_raw_input(s);
    }
});
