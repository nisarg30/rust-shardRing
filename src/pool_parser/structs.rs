use hashbrown::HashMap;
use std::ptr::NonNull;
use std::sync::atomic::AtomicU64;

pub const SHM_MAGIC: u64 = 0xfeed_cafe_dead_beef;
pub const SHM_VERSION: u32 = 1;

pub const RING_SIZE: usize = 1024;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct Tick {
    pub price: i64,
    pub qty: i32,
    pub ts: u64,
}

#[repr(C)]
pub struct ShmHeader {
    pub magic: u64,
    pub version: u32,
    pub reserved: u32,
}

#[repr(C)]
pub struct RingHeader {
    pub write_seq: AtomicU64,
    pub ring_len: u32,
}

#[repr(C)]
pub struct RingSlot<T>
where
    T: Send + Sync + 'static,
{
    pub seq: u64,
    pub data: T,
}

#[repr(C)]
pub struct Ring<T, const N: usize>
where
    T: Send + Sync + 'static,
{
    pub header: RingHeader,
    pub slots: [RingSlot<T>; N],
}

#[repr(C)]
pub struct RingShmLayout<T, const N: usize>
where
    T: Send + Sync + 'static,
{
    pub header: ShmHeader,
    pub ring: Ring<T, N>,
}

pub struct InstrumentEntry<T>
where
    T: Send + Sync + 'static,
{
    pub token: i64,
    pub ring: NonNull<Ring<T, RING_SIZE>>,
}

unsafe impl<T> Send for InstrumentEntry<T> where T: Send + Sync + 'static {}
unsafe impl<T> Sync for InstrumentEntry<T> where T: Send + Sync + 'static {}

pub type InstrumentMap<T> = HashMap<i64, InstrumentEntry<T>>;

pub struct RegisterTokenPayload {
    pub token: i64,
    pub shard: usize,
    pub ring_len: u32,
}
