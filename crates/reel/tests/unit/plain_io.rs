use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};

use reel::io::Io;
use reel::io::plain::{DIRECTORY_MODE, FILE_MODE, PlainIo};
use reel_core::units::ByteCount;

// a store's files and roots are its user's alone, and a create never follows a link someone left in a root
#[test]
fn modes_and_links() {
    let base = std::env::temp_dir().join(format!("reel-plain-modes-{}", std::process::id()));
    let _removed = fs::remove_dir_all(&base);
    let io = PlainIo::new();
    let root = base.join("root");
    io.create_directory(&root).expect("root");
    assert_eq!(fs::metadata(&root).expect("root").permissions().mode() & 0o777, DIRECTORY_MODE);

    let path = root.join("file");
    let file = io.create_file(&path, ByteCount(0)).expect("create");
    io.close_file(file);
    assert_eq!(fs::metadata(&path).expect("file").permissions().mode() & 0o777, FILE_MODE);

    let target = base.join("elsewhere");
    let link = root.join("link");
    symlink(&target, &link).expect("link");
    assert!(io.create_file(&link, ByteCount(0)).is_err());
    assert!(!target.exists());
    let _removed = fs::remove_dir_all(&base);
}

// a reserve holds a file's room and never sets its length, so a writer that wrote past the reserved end keeps its bytes
#[test]
fn reserve_keeps_a_longer_file() {
    use std::io::IoSlice;

    use reel::io::IoFile;
    use reel_core::units::FileOffset;

    let base = std::env::temp_dir().join(format!("reel-plain-reserve-{}", std::process::id()));
    let _removed = fs::remove_dir_all(&base);
    let io = PlainIo::new();
    io.create_directory(&base).expect("base");
    let file = io.create_file(&base.join("segment"), ByteCount(4_096)).expect("create");
    let tail = vec![7u8; 8_192];
    file.write_at(FileOffset(1 << 20), &[IoSlice::new(&tail)]).expect("write past the room");
    let written = (1 << 20) + tail.len() as u64;
    file.reserve_to(ByteCount(64 * 1024)).expect("reserve short of the write");
    assert_eq!(file.length().expect("length").0, written);
    file.reserve_to(ByteCount(4 << 20)).expect("reserve past the write");
    assert!(file.held_bytes().expect("held").0 >= 4 << 20, "the reserve held {:?}", file.held_bytes());
    let mut read = vec![0u8; tail.len()];
    file.read_exact_at(FileOffset(1 << 20), &mut read).expect("read the tail");
    assert_eq!(read, tail);
    io.close_file(file);
    let _removed = fs::remove_dir_all(&base);
}
