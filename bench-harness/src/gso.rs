//! GSO (`UDP_SEGMENT`) send helper.

use std::net::{SocketAddr, UdpSocket};

/// Sends `buf` to `dest`, using GSO when `segment_size` is `Some` on Linux.
pub fn send_with_gso(
    socket: &UdpSocket,
    buf: &[u8],
    dest: SocketAddr,
    segment_size: Option<usize>,
) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    if let Some(seg) = segment_size {
        if seg > 0 && seg < buf.len() {
            return send_gso_linux(socket, buf, dest, seg as u16);
        }
    }
    let _ = segment_size;
    socket.send_to(buf, dest)?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn send_gso_linux(
    socket: &UdpSocket,
    buf: &[u8],
    dest: SocketAddr,
    segment_size: u16,
) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    const SOL_UDP: libc::c_int = 17;
    const UDP_SEGMENT: libc::c_int = 103;

    let (name, name_len) = sockaddr_from_std(dest);
    let iov = libc::iovec {
        iov_base: buf.as_ptr() as *mut _,
        iov_len: buf.len(),
    };
    let mut cmsg_buf = [0u8; 16usize];
    let mut hdr = libc::msghdr {
        msg_name: name.as_ptr() as *mut _,
        msg_namelen: name_len,
        msg_iov: &iov as *const _ as *mut _,
        msg_iovlen: 1,
        msg_control: cmsg_buf.as_mut_ptr() as *mut _,
        msg_controllen: cmsg_buf.len(),
        msg_flags: 0,
    };
    unsafe {
        let cmsg = libc::CMSG_FIRSTHDR(&hdr as *const _);
        if !cmsg.is_null() {
            (*cmsg).cmsg_level = SOL_UDP;
            (*cmsg).cmsg_type = UDP_SEGMENT;
            (*cmsg).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<u16>() as u32) as _;
            (libc::CMSG_DATA(cmsg) as *mut u16).write_unaligned(segment_size);
        }
        hdr.msg_controllen = libc::CMSG_SPACE(std::mem::size_of::<u16>() as u32) as _;
        let ret = libc::sendmsg(socket.as_raw_fd(), &hdr, 0);
        if ret < 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn sockaddr_from_std(addr: SocketAddr) -> ([u8; 28], libc::socklen_t) {
    let mut storage = [0u8; 28];
    match addr {
        SocketAddr::V4(v4) => {
            let sin = unsafe { &mut *(storage.as_mut_ptr() as *mut libc::sockaddr_in) };
            sin.sin_family = libc::AF_INET as libc::sa_family_t;
            sin.sin_port = v4.port().to_be();
            sin.sin_addr = libc::in_addr {
                s_addr: u32::from(*v4.ip()).to_be(),
            };
            (storage, std::mem::size_of::<libc::sockaddr_in>() as _)
        }
        SocketAddr::V6(v6) => {
            let sin6 = unsafe { &mut *(storage.as_mut_ptr() as *mut libc::sockaddr_in6) };
            sin6.sin6_family = libc::AF_INET6 as libc::sa_family_t;
            sin6.sin6_port = v6.port().to_be();
            sin6.sin6_addr.s6_addr = v6.ip().octets();
            (storage, std::mem::size_of::<libc::sockaddr_in6>() as _)
        }
    }
}
