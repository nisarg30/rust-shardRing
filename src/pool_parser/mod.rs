use bytes::Bytes;
use hashbrown::HashMap;
use heapless::spsc::{Producer, Queue};
use memmap2::MmapMut;
use std::fs::{self, OpenOptions};
use std::mem::size_of;
use std::ptr::NonNull;
use threadpool::ThreadPool;

pub mod reader;
pub mod structs;
use crate::pool_parser::structs::*;

fn register_ring<T>(token: i64) -> (MmapMut, InstrumentEntry<T>)
where
    T: Send + Sync + 'static,
{
    let path = format!("shm/token_{}_ring.shm", token);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();

    let size = size_of::<RingShmLayout<T, RING_SIZE>>() as u64;
    file.set_len(size).unwrap();

    let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
    let layout = mmap.as_mut_ptr() as *mut RingShmLayout<T, RING_SIZE>;

    unsafe {
        (*layout).header.magic = SHM_MAGIC;
        (*layout).header.version = SHM_VERSION;
        (*layout).header.reserved = 0;
        (*layout)
            .ring
            .header
            .write_seq
            .store(0, std::sync::atomic::Ordering::Release);
    }

    let ring_ptr = unsafe { NonNull::new_unchecked(&mut (*layout).ring) };

    let entry = InstrumentEntry {
        token,
        mode: InstrumentMode::Ring,
        storage: InstrumentStorage::Ring { ring: ring_ptr },
    };

    (mmap, entry)
}

fn register_latest<T>(token: i64) -> (MmapMut, InstrumentEntry<T>)
where
    T: Send + Sync + 'static,
{
    let path = format!("shm/token_{}_latest.shm", token);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();

    let size = size_of::<LatestShmLayout<T>>() as u64;
    file.set_len(size).unwrap();

    let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
    let layout = mmap.as_mut_ptr() as *mut LatestShmLayout<T>;

    unsafe {
        (*layout).header.magic = SHM_MAGIC;
        (*layout).header.version = SHM_VERSION;
        (*layout).header.reserved = 0;
        (*layout)
            .seqlock
            .write_seq
            .store(0, std::sync::atomic::Ordering::Release);
    }

    let seqlock_ptr = unsafe { NonNull::new_unchecked(&mut (*layout).seqlock) };

    let entry = InstrumentEntry {
        token,
        mode: InstrumentMode::Latest,
        storage: InstrumentStorage::Latest {
            seqlock: seqlock_ptr,
        },
    };

    (mmap, entry)
}

fn write_ring<T>(parsed: T, ptr: NonNull<Ring<T, 1024>>)
where
    T: Send + Sync + 'static,
{
    unsafe {
        let ring = ptr.as_ptr();
        let mut seq = (*ring)
            .header
            .write_seq
            .load(std::sync::atomic::Ordering::Relaxed);

        seq = seq + 1;
        let index = (seq % RING_SIZE as u64) as usize;
        let slot_ptr = (*ring).slots.as_mut_ptr().add(index);
        let slot = RingSlot {
            seq: seq,
            data: parsed,
        };

        slot_ptr.write(slot);

        (*ring)
            .header
            .write_seq
            .store(seq, std::sync::atomic::Ordering::Release);
    }
}

fn write_latest<T>(parsed: T, ptr: NonNull<MultiSeqLock<T>>)
where
    T: Send + Sync + 'static,
{
    unsafe {
        let latest = ptr.as_ptr();
        let mut seq = (*latest)
            .write_seq
            .load(std::sync::atomic::Ordering::Relaxed);

        seq = seq + 1;
        let index = (seq % SEQ_SLOTS as u64) as usize;
        let slot_ptr = (*latest).slots.as_mut_ptr().add(index);
        let slot = SeqSlot {
            seq: seq,
            data: parsed,
        };

        slot_ptr.write(slot);

        (*latest)
            .write_seq
            .store(seq, std::sync::atomic::Ordering::Release);
    }
}

fn write_parsed<T>(token: i64, parsed: T, instruments: &HashMap<i64, InstrumentEntry<T>>)
where
    T: Send + Sync + 'static,
{
    let ins_entry = instruments.get(&token).unwrap();
    match &ins_entry.storage {
        InstrumentStorage::Ring { ring } => write_ring::<T>(parsed, *ring),
        InstrumentStorage::Latest { seqlock } => write_latest::<T>(parsed, *seqlock),
    }
}

pub struct PoolParser<T>
where
    T: Send + Sync + 'static,
{
    mmaps: Vec<MmapMut>,
    producers: Vec<Producer<'static, Bytes>>,
    parser_pool: ThreadPool,
    token_to_shard: HashMap<i64, usize>,
    parser: fn(Bytes) -> (i64, T),
}

impl<T> PoolParser<T>
where
    T: Send + Sync + 'static,
{
    pub fn new(
        n: usize,
        parser: fn(Bytes) -> (i64, T),
        token_to_map: Vec<RegisterTokenPayload>,
    ) -> Self {
        std::fs::create_dir_all("shm").unwrap();

        let mut mmaps = Vec::new();
        let mut shard_maps: Vec<HashMap<i64, InstrumentEntry<T>>> =
            (0..n).map(|_| HashMap::new()).collect();
        let mut token_to_shard: HashMap<i64, usize> = HashMap::new();

        for (i, payload) in token_to_map.into_iter().enumerate() {
            let shard_id = payload.shard;
            token_to_shard.insert(payload.token, shard_id);

            let (mmap, entry) = match payload.mode {
                InstrumentMode::Ring => register_ring::<T>(payload.token),
                InstrumentMode::Latest => register_latest::<T>(payload.token),
            };

            mmaps.push(mmap);

            shard_maps[shard_id].insert(payload.token, entry);
        }

        let parser_pool = ThreadPool::new(n);
        let mut producers = Vec::with_capacity(n);

        for (shard_id, entry_map) in shard_maps.into_iter().enumerate() {
            let parser_fn = parser;
            let queue = Box::leak(Box::new(Queue::<Bytes, 128>::new()));
            let (producer, mut consumer) = queue.split();
            producers[shard_id] = producer;

            parser_pool.execute(move || {
                while let Some(bytes) = consumer.dequeue() {
                    let (token, parsed) = (parser_fn)(bytes);
                    write_parsed::<T>(token, parsed, &entry_map);
                }
            });
        }

        Self {
            mmaps,
            producers: producers,
            parser_pool: parser_pool,
            token_to_shard,
            parser,
        }
    }
}
