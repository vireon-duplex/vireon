use std::ptr;

pub struct ByteWriter<'a> {
    ptr: *mut u8,
    end: *mut u8,
    _marker: std::marker::PhantomData<&'a mut [u8]>,
}

impl<'a> ByteWriter<'a> {
    #[inline(always)]
    pub fn new(buf: &'a mut [u8]) -> Self {
        let ptr = buf.as_mut_ptr();

        Self {
            ptr,
            end: unsafe { ptr.add(buf.len()) },
            _marker: std::marker::PhantomData,
        }
    }

    #[inline(always)]
    fn check(&self, n: usize) {
        debug_assert!(unsafe { self.ptr.add(n) <= self.end });
    }

    #[inline(always)]
    pub fn write_u8(&mut self, v: u8) {
        self.check(1);

        unsafe {
            ptr::write(self.ptr, v);
            self.ptr = self.ptr.add(1);
        }
    }

    #[inline(always)]
    pub fn write_u32(&mut self, v: u32) {
        self.check(4);

        unsafe {
            ptr::write_unaligned(self.ptr as *mut u32, v.to_be());
            self.ptr = self.ptr.add(4);
        }
    }

    #[inline(always)]
    pub fn write_u64(&mut self, v: u64) {
        self.check(8);

        unsafe {
            ptr::write_unaligned(self.ptr as *mut u64, v.to_be());
            self.ptr = self.ptr.add(8);
        }
    }

    #[inline(always)]
    pub fn write_bytes(&mut self, src: &[u8]) {
        self.check(src.len());

        unsafe {
            ptr::copy_nonoverlapping(src.as_ptr(), self.ptr, src.len());

            self.ptr = self.ptr.add(src.len());
        }
    }

    /// Writes a byte slice into the buffer.
    #[inline(always)]
    pub fn write_slice(&mut self, src: &[u8]) {
        self.write_bytes(src);
    }
}
