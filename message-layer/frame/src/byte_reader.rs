use std::marker::PhantomData;
use std::ptr;

pub struct ByteReader<'a> {
    ptr: *const u8,
    end: *const u8,
    _marker: PhantomData<&'a [u8]>,
}

impl<'a> ByteReader<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self {
        let ptr = buf.as_ptr();

        Self {
            ptr,
            end: unsafe { ptr.add(buf.len()) },
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    fn check(&self, n: usize) {
        debug_assert!(unsafe { self.ptr.add(n) <= self.end });
    }

    #[inline(always)]
    pub fn read_u8(&mut self) -> u8 {
        self.check(1);

        unsafe {
            let v = ptr::read(self.ptr);

            self.ptr = self.ptr.add(1);

            v
        }
    }

    #[inline(always)]
    pub fn read_u16(&mut self) -> u16 {
        self.check(2);

        unsafe {
            let v = u16::from_be(ptr::read_unaligned(self.ptr as *const u16));

            self.ptr = self.ptr.add(2);

            v
        }
    }

    #[inline(always)]
    pub fn read_u32(&mut self) -> u32 {
        self.check(4);

        unsafe {
            let v = u32::from_be(ptr::read_unaligned(self.ptr as *const u32));

            self.ptr = self.ptr.add(4);

            v
        }
    }

    #[inline(always)]
    pub fn read_u64(&mut self) -> u64 {
        self.check(8);

        unsafe {
            let v = u64::from_be(ptr::read_unaligned(self.ptr as *const u64));

            self.ptr = self.ptr.add(8);

            v
        }
    }

    /// Returns the number of bytes remaining (not yet consumed).
    #[inline(always)]
    pub fn remaining(&self) -> usize {
        unsafe { self.end.offset_from(self.ptr) as usize }
    }

    /// Reads `n` bytes from the current position and advances the cursor.
    #[inline(always)]
    pub fn read_slice(&mut self, n: usize) -> &'a [u8] {
        self.check(n);

        unsafe {
            let slice = std::slice::from_raw_parts(self.ptr, n);

            self.ptr = self.ptr.add(n);

            slice
        }
    }
}
