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

// a failed sync keeps its kind and says it came from a sync, so a retry can tell it from a failed write
#[test]
fn sync_failure_marks() {
    let marked = sync_failed(Error::from(ErrorKind::StorageFull));
    assert_eq!(marked.kind(), ErrorKind::StorageFull);
    assert!(is_sync_failure(&marked));
    assert!(!is_sync_failure(&Error::from(ErrorKind::StorageFull)));
}
