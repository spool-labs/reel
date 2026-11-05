use std::io::{Error, ErrorKind};

use reel::io::device_error::*;

// the errors a device reports count as damage, and the ones a process causes don't
#[test]
fn error_kinds() {
    assert!(is_device_error(&Error::from_raw_os_error(libc::EIO)));
    assert!(is_device_error(&Error::from(ErrorKind::UnexpectedEof)));

    assert!(!is_device_error(&Error::from_raw_os_error(libc::EBADF)));
    assert!(!is_device_error(&Error::from_raw_os_error(libc::ENOMEM)));
    assert!(!is_device_error(&Error::other("not from a device")));
}
