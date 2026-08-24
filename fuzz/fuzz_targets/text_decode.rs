#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    let _ = protocol::text::decode(input, protocol::text::DEFAULT_MAX_LINE_SIZE);
});
