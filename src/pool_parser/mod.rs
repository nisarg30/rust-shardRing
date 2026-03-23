use bytes::Bytes;
use hashbrown::HashMap;
use heapless::spsc::{Producer, Queue};
use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::mem::size_of;
use std::ptr::NonNull;
use std::sync::Mutex;
use std::time::Duration;
use threadpool::ThreadPool;

pub mod reader;
pub mod structs;
use crate::pool_parser::structs::*;

fn register_ring<T>(token: i64, ring_len: u32) -> (MmapMut, InstrumentEntry<T>)
where
    T: Send + Sync + 'static,
{
    let path = format!("shm/token_{}.shm", token);

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
        (*layout).ring.header.ring_len = ring_len.clamp(4, RING_SIZE as u32);
    }

    let ring_ptr = unsafe { NonNull::new_unchecked(&mut (*layout).ring) };
    (
        mmap,
        InstrumentEntry {
            token,
            ring: ring_ptr,
        },
    )
}

fn write_ring<T>(parsed: T, ptr: NonNull<Ring<T, RING_SIZE>>)
where
    T: Send + Sync + 'static,
{
    unsafe {
        let ring = ptr.as_ptr();
        let ring_len = (*ring).header.ring_len as u64;
        let mut seq = (*ring)
            .header
            .write_seq
            .load(std::sync::atomic::Ordering::Relaxed);

        seq += 1;
        let index = (seq % ring_len) as usize;
        let slot_ptr = (*ring).slots.as_mut_ptr().add(index);
        slot_ptr.write(RingSlot { seq, data: parsed });

        println!("ring written");

        (*ring)
            .header
            .write_seq
            .store(seq, std::sync::atomic::Ordering::Release);
    }
}

fn write_parsed<T>(token: i64, parsed: T, instruments: &HashMap<i64, InstrumentEntry<T>>)
where
    T: Send + Sync + 'static,
{
    let ins_entry = instruments.get(&token).unwrap();
    println!("writing parsed");
    write_ring(parsed, ins_entry.ring);
}

#[allow(dead_code)]
pub struct PoolParser<T>
where
    T: Send + Sync + 'static,
{
    mmaps: Vec<MmapMut>,
    producers: Vec<Mutex<Producer<'static, Bytes>>>,
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

        for (_i, payload) in token_to_map.into_iter().enumerate() {
            let shard_id = payload.shard;
            token_to_shard.insert(payload.token, shard_id);

            let (mmap, entry) = register_ring::<T>(payload.token, payload.ring_len);
            mmaps.push(mmap);
            shard_maps[shard_id].insert(payload.token, entry);
        }

        let parser_pool = ThreadPool::new(n);
        let mut producers = Vec::with_capacity(n);

        for (_shard_id, entry_map) in shard_maps.into_iter().enumerate() {
            let parser_fn = parser;
            let queue = Box::leak(Box::new(Queue::<Bytes, 128>::new()));
            let (producer, mut consumer) = queue.split();
            producers.push(Mutex::new(producer));

            parser_pool.execute(move || loop {
                if let Some(bytes) = consumer.dequeue() {
                    let (token, parsed) = (parser_fn)(bytes);
                    write_parsed::<T>(token, parsed, &entry_map);
                } else {
                    std::thread::sleep(Duration::from_micros(100));
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

    pub fn push(&self, token: i64, bytes: Bytes) -> Result<(), Bytes> {
        println!("{}", token);
        let shard = match self.token_to_shard.get(&token).copied() {
            Some(s) => s,
            None => return Err(bytes),
        };
        println!("push");
        self.producers[shard]
            .lock()
            .unwrap()
            .enqueue(bytes)
            .map_err(|b| b)
    }
}
