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
