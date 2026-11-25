//! What the host's memory holds for a store's maintenance jobs, read at each start with nothing on the heap

use reel_core::units::ByteCount;

/// The host's memory as one read of it found
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HostMemory {
    /// The host's memory in all
    pub total: ByteCount,
    /// The bytes the kernel can hand out without swapping
    pub available: ByteCount,
    /// What the process's control group may still take under its tightest limit, its inactive file pages counted free as
    /// MemAvailable counts the host's, when a limit is set
    pub group_room: Option<ByteCount>,
    /// The tightest limit on the way from the process's control group to the root, when one is set
    pub group_limit: Option<ByteCount>,
}

impl HostMemory {
    /// Read the free bytes a job can count on: what the kernel has, no more than the control group's room
    pub fn free(&self) -> ByteCount {
        match self.group_room {
            Some(room) => ByteCount(self.available.0.min(room.0)),
            None => self.available,
        }
    }

    /// Read the memory the reserve takes its share of: the host's, no more than the control group's limit
    pub fn reserve_total(&self) -> ByteCount {
        self.group_limit.map_or(self.total, |limit| ByteCount(self.total.0.min(limit.0)))
    }
}

/// Bytes a small file read takes on the stack, more than meminfo or a control group file holds
#[cfg(target_os = "linux")]
const READ_BYTES: usize = 4_096;

/// Bytes a control group file's path takes on the stack
#[cfg(target_os = "linux")]
const PATH_BYTES: usize = 512;

/// Read the host's memory now
#[cfg(target_os = "linux")]
pub fn read_host_memory() -> HostMemory {
    let mut buffer = [0u8; READ_BYTES];
    let Some(length) = read_small(c"/proc/meminfo".to_bytes_with_nul(), &mut buffer) else {
        return HostMemory::default();
    };
    let meminfo = &buffer[..length];
    let total = field_kib(meminfo, b"MemTotal:").unwrap_or(0) * 1_024;
    let available = field_kib(meminfo, b"MemAvailable:").unwrap_or(0) * 1_024;
    let (group_room, group_limit) = group_room().unzip();
    HostMemory { total: ByteCount(total), available: ByteCount(available), group_room, group_limit }
}

/// Read the host's memory now: the total, and the share of it the kernel's pressure level calls free
#[cfg(target_os = "macos")]
pub fn read_host_memory() -> HostMemory {
    let total = sysctl_u64(c"hw.memsize").unwrap_or(0);
    let level = sysctl_u64(c"kern.memorystatus_level").unwrap_or(0).min(100);
    HostMemory { total: ByteCount(total), available: ByteCount(total / 100 * level), group_room: None, group_limit: None }
}

/// Read the host's memory now, where no reading is known
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn read_host_memory() -> HostMemory {
    HostMemory::default()
}

/// Read the room under the tightest memory.max on the way from the process's control group to the root, and that limit
#[cfg(target_os = "linux")]
fn group_room() -> Option<(ByteCount, ByteCount)> {
    let mut buffer = [0u8; READ_BYTES];
    let length = read_small(c"/proc/self/cgroup".to_bytes_with_nul(), &mut buffer)?;
    // the unified hierarchy's line is "0::/path"
    let line = buffer[..length].split(|byte| *byte == b'\n').find(|line| line.starts_with(b"0::"))?;
    let mut group = &line[3..];
    let mut room: Option<(u64, u64)> = None;
    loop {
        if let Some((level, limit)) = level_room(group) {
            room = Some(room.map_or((level, limit), |(room, tightest)| (room.min(level), tightest.min(limit))));
        }
        let Some(parent) = group.iter().rposition(|byte| *byte == b'/').filter(|at| *at > 0) else {
            break;
        };
        group = &group[..parent];
    }
    room.map(|(room, limit)| (ByteCount(room), ByteCount(limit)))
}

/// Read one control group's limit less its use past its inactive file pages, and the limit, or nothing when it has none
#[cfg(target_os = "linux")]
fn level_room(group: &[u8]) -> Option<(u64, u64)> {
    let mut path = [0u8; PATH_BYTES];
    let mut buffer = [0u8; READ_BYTES];
    let length = read_small(group_file(group, b"memory.max", &mut path)?, &mut buffer)?;
    let limit = parse_number(&buffer[..length])?;
    let length = read_small(group_file(group, b"memory.current", &mut path)?, &mut buffer)?;
    let current = parse_number(&buffer[..length]).unwrap_or(0);
    let length = read_small(group_file(group, b"memory.stat", &mut path)?, &mut buffer).unwrap_or(0);
    let inactive = field_number(&buffer[..length], b"inactive_file ").unwrap_or(0);
    Some((limit.saturating_sub(current.saturating_sub(inactive)), limit))
}

/// Lay a control group file's path out in a buffer, ending in a nul
#[cfg(target_os = "linux")]
fn group_file<'path>(group: &[u8], name: &[u8], path: &'path mut [u8; PATH_BYTES]) -> Option<&'path [u8]> {
    let root = b"/sys/fs/cgroup";
    let length = root.len() + group.len() + 1 + name.len();
    if length + 1 > PATH_BYTES {
        return None;
    }
    path[..root.len()].copy_from_slice(root);
    path[root.len()..root.len() + group.len()].copy_from_slice(group);
    path[root.len() + group.len()] = b'/';
    path[root.len() + group.len() + 1..length].copy_from_slice(name);
    path[length] = 0;
    Some(&path[..=length])
}

/// Read a small file whole into a buffer, by a path that ends in a nul
#[cfg(target_os = "linux")]
fn read_small(path: &[u8], buffer: &mut [u8]) -> Option<usize> {
    // SAFETY: the path ends in a nul and lives for the call
    let descriptor = unsafe { libc::open(path.as_ptr().cast(), libc::O_RDONLY | libc::O_CLOEXEC) };
    if descriptor < 0 {
        return None;
    }
    let mut length = 0;
    while length < buffer.len() {
        // SAFETY: the descriptor is open and the buffer has room past the length
        let read = unsafe { libc::read(descriptor, buffer[length..].as_mut_ptr().cast(), buffer.len() - length) };
        match usize::try_from(read) {
            Ok(0) | Err(_) => break,
            Ok(read) => length += read,
        }
    }
    // SAFETY: the descriptor is open and closes once
    unsafe { libc::close(descriptor) };
    Some(length)
}

/// Read a meminfo field's number of KiB
#[cfg(any(target_os = "linux", test))]
fn field_kib(meminfo: &[u8], name: &[u8]) -> Option<u64> {
    field_number(meminfo, name)
}

/// Read the number on the line a name starts
#[cfg(any(target_os = "linux", test))]
fn field_number(text: &[u8], name: &[u8]) -> Option<u64> {
    let line = text.split(|byte| *byte == b'\n').find(|line| line.starts_with(name))?;
    parse_number(line[name.len()..].trim_ascii_start())
}

/// Read the decimal number a text starts with, or nothing when it starts with something else
#[cfg(any(target_os = "linux", test))]
fn parse_number(text: &[u8]) -> Option<u64> {
    let digits = text.iter().take_while(|byte| byte.is_ascii_digit()).count();
    if digits == 0 {
        return None;
    }
    text[..digits].iter().try_fold(0u64, |number, digit| number.checked_mul(10)?.checked_add(u64::from(digit - b'0')))
}

/// Read a number from sysctl by its name
#[cfg(target_os = "macos")]
fn sysctl_u64(name: &std::ffi::CStr) -> Option<u64> {
    let mut value = [0u8; 8];
    let mut length = value.len();
    // SAFETY: the name ends in a nul, and the value and its length are valid for the call
    let status = unsafe { libc::sysctlbyname(name.as_ptr(), value.as_mut_ptr().cast(), &mut length, std::ptr::null_mut(), 0) };
    match (status, length) {
        (0, 8) => Some(u64::from_ne_bytes(value)),
        (0, 4) => Some(u64::from(u32::from_ne_bytes([value[0], value[1], value[2], value[3]]))),
        _ => None,
    }
}

/// The mark a trigger asks the kernel for: some task stalled on memory for 150 ms of any one-second window
#[cfg(target_os = "linux")]
pub const PRESSURE_TRIGGER: &[u8] = b"some 150000 1000000";
/// The share of the last ten seconds some task stalled on memory past which a read counts as pressure, in hundredths of a percent
pub const PRESSURE_READ_MARK: u32 = 1_000;
/// How long the watch waits on the kernel before it looks whether the store closed, in milliseconds
#[cfg(target_os = "linux")]
pub const PRESSURE_WAIT_MILLIS: libc::c_int = 250;

/// A thread that waits on the kernel's pressure events for the store's control group, which stops and joins when it drops
pub struct PressureWatch {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Drop for PressureWatch {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _joined = thread.join();
        }
    }
}

/// Copy the process's control group, as /proc/self/cgroup gives it, into a buffer, and return its length
#[cfg(target_os = "linux")]
fn own_group(group: &mut [u8; PATH_BYTES]) -> Option<usize> {
    let mut buffer = [0u8; READ_BYTES];
    let length = read_small(c"/proc/self/cgroup".to_bytes_with_nul(), &mut buffer)?;
    // the unified hierarchy's line is "0::/path"
    let line = buffer[..length].split(|byte| *byte == b'\n').find(|line| line.starts_with(b"0::"))?;
    let name = &line[3..];
    let target = group.get_mut(..name.len())?;
    target.copy_from_slice(name);
    Some(name.len())
}

/// Read the share of the last ten seconds some task of the store's control group stalled on memory, in hundredths of a percent,
/// or nothing where the kernel keeps no pressure for the group
#[cfg(target_os = "linux")]
pub fn group_pressure() -> Option<u32> {
    let mut group = [0u8; PATH_BYTES];
    let length = own_group(&mut group)?;
    let mut path = [0u8; PATH_BYTES];
    let mut buffer = [0u8; READ_BYTES];
    let read = read_small(group_file(&group[..length], b"memory.pressure", &mut path)?, &mut buffer)?;
    parse_avg10(&buffer[..read])
}

/// Read no pressure where the kernel keeps none
#[cfg(not(target_os = "linux"))]
pub fn group_pressure() -> Option<u32> {
    None
}

/// Count the times the store's control group hit its memory.high or memory.max, which the kernel keeps in memory.events, or nothing
/// where the group has no such file
#[cfg(target_os = "linux")]
pub fn group_limit_hits() -> Option<u64> {
    let mut group = [0u8; PATH_BYTES];
    let length = own_group(&mut group)?;
    let mut path = [0u8; PATH_BYTES];
    let mut buffer = [0u8; READ_BYTES];
    let read = read_small(group_file(&group[..length], b"memory.events", &mut path)?, &mut buffer)?;
    let events = &buffer[..read];
    Some(field_number(events, b"high ").unwrap_or(0) + field_number(events, b"max ").unwrap_or(0))
}

/// Count no hits where the kernel keeps none
#[cfg(not(target_os = "linux"))]
pub fn group_limit_hits() -> Option<u64> {
    None
}

/// Read the "some" line's avg10 from a pressure file, in hundredths of a percent
#[cfg(any(target_os = "linux", test))]
pub fn parse_avg10(text: &[u8]) -> Option<u32> {
    let line = text.split(|byte| *byte == b'\n').find(|line| line.starts_with(b"some "))?;
    let at = line.windows(6).position(|window| window == b"avg10=")? + 6;
    let field = line[at..].split(|byte| *byte == b' ').next()?;
    let mut parts = field.split(|byte| *byte == b'.');
    let whole = parse_number(parts.next()?)?;
    let fraction = parts.next().map_or(0, |digits| parse_number(&digits[..digits.len().min(2)]).unwrap_or(0) * if digits.len() == 1 { 10 } else { 1 });
    u32::try_from(whole * 100 + fraction).ok()
}

/// Write a trigger on the store's control group's memory.pressure and start a thread that calls `wake` on each event, or nothing
/// where the store may not write one
#[cfg(target_os = "linux")]
pub fn watch_pressure(wake: std::sync::Arc<dyn Fn() + Send + Sync>) -> Option<PressureWatch> {
    let mut group = [0u8; PATH_BYTES];
    let length = own_group(&mut group)?;
    let mut path = [0u8; PATH_BYTES];
    let file = group_file(&group[..length], b"memory.pressure", &mut path)?;
    // SAFETY: the path ends in a nul and lives for the call
    let descriptor = unsafe { libc::open(file.as_ptr().cast(), libc::O_RDWR | libc::O_NONBLOCK | libc::O_CLOEXEC) };
    if descriptor < 0 {
        return None;
    }
    // SAFETY: the descriptor is open, and the trigger's bytes live for the call
    let written = unsafe { libc::write(descriptor, PRESSURE_TRIGGER.as_ptr().cast(), PRESSURE_TRIGGER.len()) };
    if written < 0 {
        // SAFETY: the descriptor is open and closes once
        unsafe { libc::close(descriptor) };
        return None;
    }
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stopped = std::sync::Arc::clone(&stop);
    let thread = std::thread::Builder::new()
        .name("reel-pressure".to_string())
        .spawn(move || {
            while !stopped.load(std::sync::atomic::Ordering::Acquire) {
                let mut wait = libc::pollfd { fd: descriptor, events: libc::POLLPRI, revents: 0 };
                // SAFETY: the descriptor is open while the thread runs, and the poll entry lives for the call
                let ready = unsafe { libc::poll(&mut wait, 1, PRESSURE_WAIT_MILLIS) };
                if ready > 0 && wait.revents & libc::POLLERR != 0 {
                    break;
                }
                if ready > 0 && wait.revents & libc::POLLPRI != 0 {
                    wake();
                }
            }
            // SAFETY: the descriptor is open and closes once, here
            unsafe { libc::close(descriptor) };
        })
        .ok()?;
    Some(PressureWatch { stop, thread: Some(thread) })
}

/// Start no watch where the kernel offers no trigger
#[cfg(not(target_os = "linux"))]
pub fn watch_pressure(wake: std::sync::Arc<dyn Fn() + Send + Sync>) -> Option<PressureWatch> {
    let _unused = wake;
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_a_pressure_file() {
        let pressure = b"some avg10=12.34 avg60=1.00 avg300=0.50 total=123\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n";
        assert_eq!(parse_avg10(pressure), Some(1_234));
        assert_eq!(parse_avg10(b"some avg10=0.5 avg60=0.00\n"), Some(50));
        assert_eq!(parse_avg10(b"full avg10=3.00\n"), None);
    }

    #[test]
    fn reads_meminfo_fields() {
        let meminfo = b"MemTotal:       32763088 kB\nMemFree:          100 kB\nMemAvailable:   28218232 kB\n";
        assert_eq!(field_kib(meminfo, b"MemTotal:"), Some(32_763_088));
        assert_eq!(field_kib(meminfo, b"MemAvailable:"), Some(28_218_232));
        assert_eq!(field_kib(meminfo, b"Cached:"), None);
        assert_eq!(parse_number(b"max\n"), None);
        assert_eq!(parse_number(b"1073741824\n"), Some(1_073_741_824));
    }
}
