#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = rewal::encoding::decode_all_batches(data, None);
    let _ = rewal::encoding::decode_batch_frame_borrowed(data, 0, None, &mut Vec::new());
});
