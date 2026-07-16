//! Linux `/proc`-based CPU and memory sampling for resource-efficiency
//! reporting.
//!
//! **CPU**: reads `utime + stime` (clock ticks) from `/proc/self/stat`,
//! converts to seconds via `sysconf(_SC_CLK_TCK)`. The value is
//! process-wide — covers all worker threads.
//!
//! **Memory**: reads `VmRSS` (KiB) from `/proc/self/status`.
//!
//! Both functions return 0 on parse failure — resource reporting is
//! best-effort and must never crash the benchmark.

use std::fs;
use std::io::Read;

/// Clock ticks per second — cached after first call.
static mut CLK_TCK: f64 = 0.0;

/// Returns `sysconf(_SC_CLK_TCK)` as `f64`, cached after first call.
/// Falls back to 100.0 (the Linux default on most distros) if `sysconf`
/// fails.
fn clk_tck() -> f64 {
    // SAFETY: `sysconf` is thread-safe and has no side effects.
    // The double-checked pattern avoids redundant syscalls after the
    // first successful call.
    unsafe {
        if CLK_TCK > 0.0 {
            return CLK_TCK;
        }
        let v = libc::sysconf(libc::_SC_CLK_TCK);
        let rate = if v > 0 { v as f64 } else { 100.0 };
        CLK_TCK = rate;
        rate
    }
}

/// Returns process CPU time (user + system) in seconds.
///
/// Reads `/proc/self/stat` fields 14 (utime) and 15 (stime) in clock
/// ticks. Returns 0.0 if the file cannot be read or parsed.
#[must_use]
pub fn cpu_time_secs() -> f64 {
    let mut buf = String::new();
    if fs::File::open("/proc/self/stat")
        .and_then(|mut f| f.read_to_string(&mut buf))
        .is_err()
    {
        return 0.0;
    }
    // /proc/self/stat format: pid (comm) state ppid pgid ... utime stime ...
    // Fields 1-2 (pid + comm) can contain spaces inside parentheses,
    // so we split from the LAST ')' to get the remaining whitespace-delimited
    // fields. After the ')', utime is field 11, stime is field 12 (0-indexed
    // from 0 after the ')').
    let rest = match buf.rfind(')') {
        Some(pos) => &buf[pos + 1..],
        None => return 0.0,
    };
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // After ')': state(0) ppid(1) pgrp(2) session(3) tty(4) tpgid(5)
    //            flags(6) minflt(7) cminflt(8) majflt(9) cmajflt(10)
    //            utime(11) stime(12) ...
    // So utime = fields[11], stime = fields[12].
    let utime: u64 = fields.get(11).and_then(|s| s.parse().ok()).unwrap_or(0);
    let stime: u64 = fields.get(12).and_then(|s| s.parse().ok()).unwrap_or(0);
    let ticks = utime.saturating_add(stime);
    ticks as f64 / clk_tck()
}

/// Returns current RSS (resident set size) in KiB from `/proc/self/status`.
///
/// Returns 0 if the file cannot be read or the `VmRSS:` line is missing.
#[must_use]
pub fn rss_kib() -> u64 {
    let mut buf = String::new();
    if fs::File::open("/proc/self/status")
        .and_then(|mut f| f.read_to_string(&mut buf))
        .is_err()
    {
        return 0;
    }
    for line in buf.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            // Format: "VmRSS:\t   12345 kB"
            let num: String = rest.chars().filter(|c| c.is_ascii_digit()).collect();
            return num.parse().unwrap_or(0);
        }
    }
    0
}

/// Returns any freed memory to the OS by asking glibc to `malloc_trim(0)`.
///
/// Between sweep steps, glibc malloc arenas retain peak heap segments even
/// after the allocations are freed. `malloc_trim(0)` releases the topmost
/// free chunk of every arena back to the OS via `munmap`/`MADV_DONTNEED`.
///
/// This is essential for sweep-mode RSS accuracy: without it, RSS measured
/// at the end of step N includes retained arena memory from all steps 1..N,
/// making `rss_per_conn` misleading across a sweep.
///
/// No-op on non-glibc allocators (returns `false`).
#[allow(unused_variables)]
pub fn malloc_trim() {
    #[cfg(target_env = "gnu")]
    unsafe {
        libc::malloc_trim(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_time_is_nonneg() {
        let t = cpu_time_secs();
        assert!(t >= 0.0, "cpu_time_secs should be non-negative");
    }

    #[test]
    fn rss_is_nonzero_for_running_process() {
        // A running test process should always have some resident memory.
        assert!(rss_kib() > 0, "rss_kib should be > 0 for a running process");
    }
}
