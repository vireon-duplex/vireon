//! eBPF `SO_REUSEPORT` steering program for QUIC DCID-based routing.
//!
//! When multiple worker sockets share a `SO_REUSEPORT` group, the kernel
//! distributes incoming packets using a default `(src_ip, src_port)` hash.
//! This breaks QUIC connection migration (RFC 9000 §9.5) and NAT rebinding:
//! if the client's 4-tuple changes, packets land on the wrong worker and
//! the connection dies.
//!
//! This module builds and loads a `BPF_PROG_TYPE_SK_REUSEPORT` eBPF program
//! that parses the QUIC Destination Connection ID (DCID) from each incoming
//! packet and hashes it `% num_sockets` to deterministically select the
//! target worker socket. Because the DCID is constant for the lifetime of
//! a connection (even across migration), every packet reaches the correct
//! worker.
//!
//! # Usage
//!
//! Call [`attach_quic_reuseport_steering`] on **one** socket fd after **all**
//! sockets in the REUSEPORT group are bound. The kernel propagates the
//! program to every socket in the group.
//!
//! ```no_run
//! use kernel_nic::attach_quic_reuseport_steering;
//!
//! // After all workers have bound their sockets:
//! # let socket_fd: std::os::fd::RawFd = 3; // any fd in the group
//! // offset = 0 when no ghost sockets are present
//! attach_quic_reuseport_steering(socket_fd, 4, 0)
//!     .expect("eBPF steering program attached");
//! ```

use std::os::fd::RawFd;

use crate::error::KernelNicError;

// ── Kernel constants ────────────────────────────────────────────────

/// `bpf(2)` command: load an eBPF program.
const BPF_PROG_LOAD: i32 = 5;

/// `BPF_PROG_TYPE_SK_REUSEPORT` — program type for REUSEPORT steering.
/// Available since kernel 4.19.
const BPF_PROG_TYPE_SK_REUSEPORT: u32 = 21;

// ── eBPF instruction struct ─────────────────────────────────────────

/// A single eBPF instruction (8 bytes).
///
/// Field layout matches the kernel's `struct bpf_insn`:
/// `{code, dst_reg:4, src_reg:4, off, imm}`.
/// The `regs` byte is `(src_reg << 4) | dst_reg`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct BpfInsn {
    /// Opcode — encodes class + mode + size/source.
    pub code: u8,
    /// `(src_reg << 4) | dst_reg`.
    pub regs: u8,
    /// Signed jump/offset in instructions.
    pub off: i16,
    /// Signed 32-bit immediate.
    pub imm: i32,
}

// ── bpf_attr for BPF_PROG_LOAD ──────────────────────────────────────

/// Subset of `union bpf_attr` used by `BPF_PROG_LOAD`.
///
/// Matches the kernel layout exactly (`include/uapi/linux/bpf.h`):
/// `prog_type, insn_cnt, insns, license, log_level, [pad], log_buf,
/// log_size, kern_version`. Fields beyond offset 48 (`prog_flags`,
/// `prog_name`, `prog_ifindex`, `expected_attach_type`) are zero-filled
/// by the kernel, defaulting `expected_attach_type` to
/// `BPF_SK_REUSEPORT_SELECT`.
#[repr(C)]
#[derive(Clone, Copy)]
struct BpfProgLoadAttr {
    prog_type: u32,    // offset  0
    insn_cnt: u32,     // offset  4
    insns: u64,        // offset  8
    license: u64,      // offset 16
    log_level: u32,    // offset 24
    _pad0: u32,        // offset 28 (align log_buf to 8 bytes)
    log_buf: u64,      // offset 32
    log_size: u32,     // offset 40
    kern_version: u32, // offset 44
}
// Total size = 48 bytes.

// ── Public API ──────────────────────────────────────────────────────

/// Builds, loads, and attaches a QUIC DCID-based eBPF steering program
/// to the `SO_REUSEPORT` group containing `fd`.
///
/// # When to call
///
/// After **all** sockets in the group are bound (i.e. after every worker
/// has called `bind(2)` with `SO_REUSEPORT`). Call on any **one** socket —
/// the kernel propagates the program group-wide.
///
/// # Arguments
///
/// * `fd` — Raw fd of any socket in the REUSEPORT group.
/// * `num_sockets` — Number of **live** sockets in the group. Baked into
///   the BPF bytecode as the modulus divisor. Must be in `[1, 65535]`.
/// * `offset` — Index of the first live socket in the kernel's socket
///   array. When ghost sockets from a previous run precede the live
///   sockets, this shifts the program's output range past them. Use
///   [`count_udp_sockets_on_port`] to detect ghosts and compute the
///   offset: `offset = total_on_port - num_live`.
///
/// # Errors
///
/// Returns [`KernelNicError::Ebpf`] if the kernel rejects the program
/// (with the verifier log in the error message), or
/// [`KernelNicError::Socket`] if `setsockopt` fails.
pub fn attach_quic_reuseport_steering(
    fd: RawFd,
    num_sockets: u32,
    offset: u32,
) -> Result<(), KernelNicError> {
    if num_sockets == 0 {
        return Err(KernelNicError::InvalidConfig(
            "num_sockets must be > 0".into(),
        ));
    }
    // MOD32 uses a 32-bit immediate, but practically num_sockets fits
    // in a u16. Cap at 65535 to catch absurd values.
    if num_sockets > 65535 {
        return Err(KernelNicError::InvalidConfig(format!(
            "num_sockets must be <= 65535 (is {num_sockets})"
        )));
    }
    // ADD32 imm is i32; offset is non-negative. 65535 + 65535 = 131070
    // which fits in i32, but cap for sanity.
    if offset > 65535 {
        return Err(KernelNicError::InvalidConfig(format!(
            "offset must be <= 65535 (is {offset})"
        )));
    }

    let insns = build_quic_dcid_program(num_sockets, offset);
    let prog_fd = load_sk_reuseport_prog(&insns)?;

    // Attach to the REUSEPORT group. The program fd can be closed
    // immediately after — the kernel holds its own reference once
    // attached.
    let prog_fd_val: libc::c_int = prog_fd;
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_ATTACH_REUSEPORT_EBPF,
            &prog_fd_val as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    // Always close the program fd (kernel keeps its ref).
    unsafe { libc::close(prog_fd) };

    if rc != 0 {
        return Err(KernelNicError::Socket(std::io::Error::last_os_error()));
    }

    if offset > 0 {
        tracing::info!(
            num_sockets,
            offset,
            insns = insns.len(),
            "eBPF QUIC DCID reuseport steering attached (ghost offset: skipping {offset} ghost socket(s))"
        );
    } else {
        tracing::info!(
            num_sockets,
            insns = insns.len(),
            "eBPF QUIC DCID reuseport steering program loaded and attached"
        );
    }
    Ok(())
}

/// Counts UDP sockets bound to `port` by parsing `/proc/net/udp`.
///
/// Returns the total number of sockets on the port, **including ghost
/// sockets** from dead processes. Used at startup to detect ghost sockets
/// and compute the eBPF steering offset.
///
/// # Errors
///
/// Returns `io::Error` if `/proc/net/udp` cannot be read.
pub fn count_udp_sockets_on_port(port: u16) -> std::io::Result<usize> {
    let content = std::fs::read_to_string("/proc/net/udp")?;
    let hex_port = format!("{:04X}", port);
    let mut count = 0;
    for line in content.lines().skip(1) {
        // Format: "  sl  local_address rem_address   st ..."
        // local_address example: "00000000:AD61" (0.0.0.0:44433)
        let mut iter = line.split_whitespace();
        let _sl = iter.next();
        if let Some(local_addr) = iter.next() {
            // local_addr format: "HEX_IP:HEX_PORT"
            if let Some(hex_val) = local_addr.rsplit(':').next() {
                if hex_val.eq_ignore_ascii_case(&hex_port) {
                    count += 1;
                }
            }
        }
    }
    Ok(count)
}

// ── BPF program construction ────────────────────────────────────────

/// Builds the `SK_REUSEPORT` eBPF program with optional ghost offset.
///
/// The program:
/// 1. Reads `ctx->data` and `ctx->data_end`.
/// 2. Bounds-checks for the worst-case read (10 bytes).
/// 3. Checks bit 7 of the first byte to distinguish short/long headers.
/// 4. **Short header** (1-RTT / 0-RTT): loads 4 bytes of DCID at offset 1.
/// 5. **Long header** (Initial / Handshake): loads 4 bytes of DCID at
///    offset 6 (after flags[1] + version[4] + dcid_len[1]).
/// 6. `r0 %= num_sockets` → socket index in `[0, num_sockets-1]`.
/// 7. If `offset > 0`: `r0 += offset` → shifts range to
///    `[offset, offset+num_sockets-1]`, bypassing ghost sockets.
/// 8. **Fallback** (packet too short): uses `ctx->hash` (kernel-provided
///    `skb->hash`) modulo `num_sockets`, same offset applied.
///
/// When `offset == 0` the program is identical to the original
/// 17-instruction build (no ADD inserted). When `offset > 0`, three
/// `ADD32` instructions are inserted (one per path), yielding 20
/// instructions with adjusted jump offsets.
#[allow(clippy::too_many_lines)]
fn build_quic_dcid_program(num_sockets: u32, offset: u32) -> Vec<BpfInsn> {
    let ns = num_sockets as i32;
    let off = offset as i32;
    let has_offset = off != 0;

    // Compute jump offsets. The JGT at insn 4 jumps to the fallback
    // block; the JEQ at insn 7 jumps to the long-header block. When
    // offset instructions are inserted, these targets shift.
    //
    // Layout (no offset):                    Layout (with offset):
    //  0-4: prologue + bounds check           0-4: same
    //  5-7: long-header check                 5-7: same
    //  8: short LDX   9: MOD   10: EXIT       8: short LDX  9: MOD  10: ADD  11: EXIT
    // 11: long  LDX  12: MOD  13: EXIT       12: long  LDX 13: MOD  14: ADD  15: EXIT
    // 14: fb    LDX  15: MOD  16: EXIT       16: fb    LDX 17: MOD  18: ADD  19: EXIT
    let fallback_insn = if has_offset { 16 } else { 14 };
    let long_hdr_insn = if has_offset { 12 } else { 11 };
    let jgt_off = (fallback_insn - 5) as i16; // relative from insn 5
    let jeq_off = (long_hdr_insn - 8) as i16; // relative from insn 8

    let mut prog = vec![
        // ── Load context pointers ──────────────────────────────────
        // r2 = *(u64*)(r1+0)  → ctx->data
        BpfInsn {
            code: 0x79,
            regs: 0x12,
            off: 0,
            imm: 0,
        },
        // r3 = *(u64*)(r1+8)  → ctx->data_end
        BpfInsn {
            code: 0x79,
            regs: 0x13,
            off: 8,
            imm: 0,
        },
        // ── Bounds check (worst case: data + 10) ───────────────────
        // r4 = r2 (copy data ptr)
        BpfInsn {
            code: 0xbf,
            regs: 0x24,
            off: 0,
            imm: 0,
        },
        // r4 += 10
        BpfInsn {
            code: 0x07,
            regs: 0x04,
            off: 0,
            imm: 10,
        },
        // if (u64)r4 > (u64)r3 goto fallback
        BpfInsn {
            code: 0x2d,
            regs: 0x34,
            off: jgt_off,
            imm: 0,
        },
        // ── Check long-header bit ──────────────────────────────────
        // r5 = *(u8*)(r2+0)  → first byte
        BpfInsn {
            code: 0x71,
            regs: 0x25,
            off: 0,
            imm: 0,
        },
        // r5 &= 0x80  → isolate bit 7
        BpfInsn {
            code: 0x57,
            regs: 0x05,
            off: 0,
            imm: 0x80,
        },
        // if r5 == 0x80 goto long_header
        BpfInsn {
            code: 0x15,
            regs: 0x05,
            off: jeq_off,
            imm: 0x80,
        },
        // ── Short header path ──────────────────────────────────────
        // DCID starts at byte 1 (quiche uses 8-byte DCIDs).
        // r0 = *(u32*)(r2+1)
        BpfInsn {
            code: 0x61,
            regs: 0x20,
            off: 1,
            imm: 0,
        },
        // r0 %= num_sockets
        BpfInsn {
            code: 0x94,
            regs: 0x00,
            off: 0,
            imm: ns,
        },
    ];

    if has_offset {
        // r0 += offset  (skip ghost sockets)
        prog.push(BpfInsn {
            code: 0x07,
            regs: 0x00,
            off: 0,
            imm: off,
        });
    }

    // exit
    prog.push(BpfInsn {
        code: 0x95,
        regs: 0x00,
        off: 0,
        imm: 0,
    });

    // ── Long header path ───────────────────────────────────────────
    // Layout: flags[1] + version[4] + dcid_len[1] + dcid[N]
    // DCID starts at offset 6. Load 4 bytes for hashing.
    prog.push(BpfInsn {
        code: 0x61,
        regs: 0x20,
        off: 6,
        imm: 0,
    });
    // r0 %= num_sockets
    prog.push(BpfInsn {
        code: 0x94,
        regs: 0x00,
        off: 0,
        imm: ns,
    });

    if has_offset {
        prog.push(BpfInsn {
            code: 0x07,
            regs: 0x00,
            off: 0,
            imm: off,
        });
    }

    // exit
    prog.push(BpfInsn {
        code: 0x95,
        regs: 0x00,
        off: 0,
        imm: 0,
    });

    // ── Fallback (packet too short to parse) ───────────────────────
    // Use the kernel-provided skb hash.
    prog.push(BpfInsn {
        code: 0x61,
        regs: 0x10,
        off: 16,
        imm: 0,
    });
    // r0 %= num_sockets
    prog.push(BpfInsn {
        code: 0x94,
        regs: 0x00,
        off: 0,
        imm: ns,
    });

    if has_offset {
        prog.push(BpfInsn {
            code: 0x07,
            regs: 0x00,
            off: 0,
            imm: off,
        });
    }

    // exit
    prog.push(BpfInsn {
        code: 0x95,
        regs: 0x00,
        off: 0,
        imm: 0,
    });

    prog
}

// ── BPF program loader ──────────────────────────────────────────────

/// Loads an eBPF program of type `BPF_PROG_TYPE_SK_REUSEPORT`.
///
/// Returns the program fd on success.
fn load_sk_reuseport_prog(insns: &[BpfInsn]) -> Result<RawFd, KernelNicError> {
    let license = b"GPL\0";
    let mut log_buf = vec![0u8; 4096];

    let attr = BpfProgLoadAttr {
        prog_type: BPF_PROG_TYPE_SK_REUSEPORT,
        insn_cnt: insns.len() as u32,
        insns: insns.as_ptr() as u64,
        license: license.as_ptr() as u64,
        log_level: 1,
        _pad0: 0,
        log_buf: log_buf.as_mut_ptr() as u64,
        log_size: log_buf.len() as u32,
        kern_version: 0,
    };

    // SAFETY: `bpf(2)` with BPF_PROG_LOAD. All pointers and sizes are
    // valid for the duration of the call. The attr struct matches the
    // kernel layout.
    let prog_fd = unsafe {
        libc::syscall(
            libc::SYS_bpf,
            BPF_PROG_LOAD,
            &attr as *const _ as *const libc::c_void,
            std::mem::size_of::<BpfProgLoadAttr>(),
        )
    };

    if prog_fd < 0 {
        let source = std::io::Error::last_os_error();
        let log_end = log_buf
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(log_buf.len());
        let verifier_log = String::from_utf8_lossy(&log_buf[..log_end]).into_owned();
        return Err(KernelNicError::Ebpf {
            stage: "BPF_PROG_LOAD (SK_REUSEPORT)",
            source,
            verifier_log,
        });
    }

    Ok(prog_fd as RawFd)
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;

    #[test]
    fn program_has_17_instructions_no_offset() {
        let prog = build_quic_dcid_program(4, 0);
        assert_eq!(prog.len(), 17);
    }

    #[test]
    fn program_has_20_instructions_with_offset() {
        let prog = build_quic_dcid_program(4, 2);
        assert_eq!(prog.len(), 20);
    }

    #[test]
    fn num_sockets_baked_into_mod_instructions() {
        let prog = build_quic_dcid_program(8, 0);
        // MOD32 at insns 9, 12, 15
        assert_eq!(prog[9].imm, 8);
        assert_eq!(prog[12].imm, 8);
        assert_eq!(prog[15].imm, 8);
        // Verify opcode is MOD32 (BPF_ALU | BPF_MOD | BPF_K = 0x94)
        assert_eq!(prog[9].code, 0x94);
        assert_eq!(prog[12].code, 0x94);
        assert_eq!(prog[15].code, 0x94);
    }

    #[test]
    fn offset_baked_into_add_instructions() {
        let prog = build_quic_dcid_program(8, 3);
        // With offset, ADD32 at insns 10, 14, 18
        assert_eq!(prog[10].imm, 3);
        assert_eq!(prog[14].imm, 3);
        assert_eq!(prog[18].imm, 3);
        // Verify opcode is ADD32 (BPF_ALU | BPF_ADD | BPF_K = 0x07)
        assert_eq!(prog[10].code, 0x07);
        assert_eq!(prog[14].code, 0x07);
        assert_eq!(prog[18].code, 0x07);
    }

    #[test]
    fn jump_offsets_correct_no_offset() {
        let prog = build_quic_dcid_program(2, 0);
        // insn 4: JGT → fallback at insn 14. off = 14 - 5 = 9.
        assert_eq!(prog[4].off, 9);
        // insn 7: JEQ → long header at insn 11. off = 11 - 8 = 3.
        assert_eq!(prog[7].off, 3);
    }

    #[test]
    fn jump_offsets_correct_with_offset() {
        let prog = build_quic_dcid_program(2, 1);
        // insn 4: JGT → fallback at insn 16. off = 16 - 5 = 11.
        assert_eq!(prog[4].off, 11);
        // insn 7: JEQ → long header at insn 12. off = 12 - 8 = 4.
        assert_eq!(prog[7].off, 4);
    }

    #[test]
    fn first_and_last_instructions() {
        let prog = build_quic_dcid_program(1, 0);
        // insn 0: LDX DW (0x79)
        assert_eq!(prog[0].code, 0x79);
        // insn 16: EXIT (0x95)
        assert_eq!(prog[16].code, 0x95);
    }

    #[test]
    fn rejects_zero_sockets() {
        let result = attach_quic_reuseport_steering(0, 0, 0);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            KernelNicError::InvalidConfig(_)
        ));
    }

    #[test]
    fn rejects_too_many_sockets() {
        let result = attach_quic_reuseport_steering(0, 70000, 0);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            KernelNicError::InvalidConfig(_)
        ));
    }

    #[test]
    fn rejects_too_large_offset() {
        let result = attach_quic_reuseport_steering(0, 4, 70000);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            KernelNicError::InvalidConfig(_)
        ));
    }

    #[test]
    fn bpf_insn_size_is_8_bytes() {
        assert_eq!(std::mem::size_of::<BpfInsn>(), 8);
    }

    #[test]
    fn bpf_attr_size_is_48_bytes() {
        assert_eq!(std::mem::size_of::<BpfProgLoadAttr>(), 48);
    }

    /// Verifies that the program can actually be loaded by the kernel.
    /// Requires kernel ≥ 4.19 and `CAP_SYS_ADMIN` (or CAP_BPF on ≥ 5.8).
    /// Skipped on permission denied (e.g. running as non-root in CI).
    #[test]
    fn kernel_loads_reuseport_program() {
        let insns = build_quic_dcid_program(2, 0);
        match load_sk_reuseport_prog(&insns) {
            Ok(fd) => {
                assert!(fd >= 0);
                unsafe { libc::close(fd) };
            }
            Err(KernelNicError::Ebpf {
                stage,
                source,
                verifier_log,
            }) => {
                // EACCES / EPERM means we're not running as root — skip.
                if source.raw_os_error() == Some(libc::EACCES)
                    || source.raw_os_error() == Some(libc::EPERM)
                {
                    eprintln!("skipped (no CAP_SYS_ADMIN): {stage}");
                    return;
                }
                panic!("BPF_PROG_LOAD failed: {stage}: {source}\n{verifier_log}");
            }
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }

    /// Verifies the offset variant also loads.
    #[test]
    fn kernel_loads_offset_reuseport_program() {
        let insns = build_quic_dcid_program(2, 1);
        match load_sk_reuseport_prog(&insns) {
            Ok(fd) => {
                assert!(fd >= 0);
                unsafe { libc::close(fd) };
            }
            Err(KernelNicError::Ebpf {
                stage,
                source,
                verifier_log,
            }) => {
                if source.raw_os_error() == Some(libc::EACCES)
                    || source.raw_os_error() == Some(libc::EPERM)
                {
                    eprintln!("skipped (no CAP_SYS_ADMIN): {stage}");
                    return;
                }
                panic!("BPF_PROG_LOAD failed: {stage}: {source}\n{verifier_log}");
            }
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }
}
