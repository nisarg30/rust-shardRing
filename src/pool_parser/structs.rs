use hashbrown::HashMap;
use std::ptr::NonNull;
use std::sync::atomic::AtomicU64;

pub const SHM_MAGIC: u64 = 0xfeed_cafe_dead_beef;
pub const SHM_VERSION: u32 = 1;

pub const RING_SIZE: usize = 1024;
pub const SEQ_SLOTS: usize = 4;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum InstrumentMode {
    Ring,
    Latest,
}

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
pub struct SeqSlot<T>
where
    T: Send + Sync + 'static,
{
    pub seq: u64,
    pub data: T,
}

#[repr(C)]
pub struct MultiSeqLock<T>
where
    T: Send + Sync + 'static,
{
    pub write_seq: AtomicU64,
    pub slots: [SeqSlot<T>; SEQ_SLOTS],
}

#[repr(C)]
pub struct RingShmLayout<T, const N: usize>
where
    T: Send + Sync + 'static,
{
    pub header: ShmHeader,
    pub ring: Ring<T, N>,
}

#[repr(C)]
pub struct LatestShmLayout<T>
where
    T: Send + Sync + 'static,
{
    pub header: ShmHeader,
    pub seqlock: MultiSeqLock<T>,
}

pub enum InstrumentStorage<T>
where
    T: Send + Sync + 'static,
{
    Ring { ring: NonNull<Ring<T, RING_SIZE>> },
    Latest { seqlock: NonNull<MultiSeqLock<T>> },
}

unsafe impl<T> Send for InstrumentStorage<T> where T: Send + Sync + 'static {}
unsafe impl<T> Sync for InstrumentStorage<T> where T: Send + Sync + 'static {}

pub struct InstrumentEntry<T>
where
    T: Send + Sync + 'static,
{
    pub token: i64,
    pub mode: InstrumentMode,
    pub storage: InstrumentStorage<T>,
}

unsafe impl<T> Send for InstrumentEntry<T> where T: Send + Sync + 'static {}
unsafe impl<T> Sync for InstrumentEntry<T> where T: Send + Sync + 'static {}

pub type InstrumentMap<T> = HashMap<i64, InstrumentEntry<T>>;

pub struct RegisterTokenPayload {
    pub token: i64,
    pub shard: usize,
    pub mode: InstrumentMode,
}
