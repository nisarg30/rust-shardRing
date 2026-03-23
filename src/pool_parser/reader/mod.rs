use hashbrown::HashMap;
use memmap2::Mmap;
use std::cell::RefCell;
use std::fs::File;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;

use crate::pool_parser::structs::{Ring, RingShmLayout, RING_SIZE};

pub struct RingReader<T>
where
    T: Send + Sync + 'static,
{
    mmaps: Vec<Mmap>,
    token_to_ring: HashMap<i64, NonNull<Ring<T, RING_SIZE>>>,
    last_read_seq: RefCell<HashMap<i64, u64>>,
}

impl<T> RingReader<T>
where
    T: Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self {
            mmaps: Vec::new(),
            token_to_ring: HashMap::new(),
            last_read_seq: RefCell::new(HashMap::new()),
        }
    }

    pub fn add_token(&mut self, token: i64) -> Result<(), std::io::Error> {
        let path = format!("shm/token_{}.shm", token);
        let file = File::options().read(true).open(&path)?;
        let mmap = unsafe {
            Mmap::map(&file).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?
        };
        let layout = mmap.as_ptr() as *const RingShmLayout<T, RING_SIZE>;
        let ring_ptr = unsafe {
            NonNull::new_unchecked(
                &(*(layout)).ring as *const Ring<T, RING_SIZE> as *mut Ring<T, RING_SIZE>,
            )
        };
        self.mmaps.push(mmap);
        self.token_to_ring.insert(token, ring_ptr);
        Ok(())
    }

    pub fn read_next(&self, token: i64) -> Option<(u64, &T)> {
        let ring_ptr = self.token_to_ring.get(&token)?;
        let mut last = self.last_read_seq.borrow_mut();
        let last = last.entry(token).or_insert(0);
        unsafe {
            let ring = ring_ptr.as_ptr();
            let write_seq = (*ring).header.write_seq.load(Ordering::Acquire);
            let ring_len = (*ring).header.ring_len as u64;
            if ring_len == 0 {
                return None;
            }
            if write_seq.saturating_sub(*last) > ring_len {
                *last = write_seq.saturating_sub(ring_len);
            }
            if *last >= write_seq {
                return None;
            }
            *last += 1;
            let seq = *last;
            let index = (seq % ring_len) as usize;
            let slot_ptr = (*ring).slots.as_ptr().add(index);
            let slot = &*slot_ptr;
            Some((slot.seq, &slot.data))
        }
    }

    pub fn read_latest(&self, token: i64) -> Option<(u64, &T)> {
        let ring_ptr = self.token_to_ring.get(&token)?;
        unsafe {
            let ring = ring_ptr.as_ptr();
            let write_seq = (*ring).header.write_seq.load(Ordering::Acquire);
            let ring_len = (*ring).header.ring_len as u64;
            let index = (write_seq % ring_len) as usize;
            let slot_ptr = (*ring).slots.as_ptr().add(index);
            let slot = &*slot_ptr;
            Some((slot.seq, &slot.data))
        }
    }
}

impl<T> Default for RingReader<T>
where
    T: Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}
