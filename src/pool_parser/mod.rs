use bytes::Bytes;
use hashbrown::HashMap;
use heapless::spsc::{Producer, Queue};
use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::mem::size_of;
use std::ptr::NonNull;
use std::sync::Arc;
use threadpool::ThreadPool;

pub mod structs;

use crate::pool_parser::structs::*;

pub struct PoolParser<T>
where
    T: Send + Sync + 'static,
{
    pub parser_pool: ThreadPool,
    pub producers: Vec<Producer<'static, Bytes>>,
    pub token_to_shard: HashMap<i64, usize>,
    pub parser: Arc<dyn Fn(Bytes) -> (i64, T) + Send + Sync + 'static>,
    pub instruments: InstrumentMap<T>,
    pub mmaps: Vec<MmapMut>,
}

impl<T> PoolParser<T>
where
    T: Send + Sync + 'static,
{
    pub fn new(
        shards: usize,
        parser: impl Fn(Bytes) -> (i64, T) + Send + Sync + 'static,
    ) -> Arc<Self> {
        let parser_pool = ThreadPool::new(shards);
        let mut producers = Vec::with_capacity(shards);
        let parser = Arc::new(parser);

        let this = Arc::new(Self {
            parser_pool,
            producers,
            token_to_shard: HashMap::new(),
            parser,
            instruments: HashMap::new(),
            mmaps: Vec::new(),
        });

        for shard_id in 0..shards {
            let queue = Box::leak(Box::new(Queue::<Bytes, 128>::new()));
            let (producer, mut consumer) = queue.split();
            Arc::get_mut(&mut Arc::clone(&this))
                .unwrap()
                .producers
                .push(producer);

            let this_clone = Arc::clone(&this);

            this.parser_pool.execute(move || {
                while let Some(bytes) = consumer.dequeue() {
                    // let (token, parsed) = (this_clone.parser)(bytes);
                    // this_clone.write_parsed(token, parsed);
                }
            });
        }

        this
    }

    fn register_ring(&mut self, token: i64) {
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
        }

        let ring_ptr = unsafe { NonNull::new_unchecked(&mut (*layout).ring) };

        self.mmaps.push(mmap);

        self.instruments.insert(
            token,
            InstrumentEntry {
                token,
                mode: InstrumentMode::Ring,
                storage: InstrumentStorage::Ring { ring: ring_ptr },
            },
        );
    }

    fn register_latest(&mut self, token: i64) {
        let path = format!("shm/token_{}_latest.shm", token);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();

        let size = std::mem::size_of::<LatestShmLayout<T>>() as u64;
        file.set_len(size).unwrap();

        let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        let layout = mmap.as_mut_ptr() as *mut LatestShmLayout<T>;

        unsafe {
            (*layout).header.magic = SHM_MAGIC;
            (*layout).header.version = SHM_VERSION;
            (*layout).header.reserved = 0;
        }

        let seqlock_ptr = unsafe { NonNull::new_unchecked(&mut (*layout).seqlock) };

        self.mmaps.push(mmap);

        self.instruments.insert(
            token,
            InstrumentEntry {
                token,
                mode: InstrumentMode::Latest,
                storage: InstrumentStorage::Latest {
                    seqlock: seqlock_ptr,
                },
            },
        );
    }

    fn write_ring(&self, token: i64, parsed: T, ptr: NonNull<Ring<T, 1024>>) {}

    fn write_latest(&self, token: i64, parsed: T, ptr: NonNull<MultiSeqLock<T>>) {}

    fn write_parsed(&self, token: i64, parsed: T) {
        let ins_entry = self.instruments.get(&token).unwrap();
        match &ins_entry.storage {
            InstrumentStorage::Ring { ring } => self.write_ring(token, parsed, *ring),
            InstrumentStorage::Latest { seqlock } => self.write_latest(token, parsed, *seqlock),
        }
    }

    pub fn register_token(&mut self, token: i64, shard_id: usize, mode: InstrumentMode) {
        self.token_to_shard.insert(token, shard_id);

        match mode {
            InstrumentMode::Ring => self.register_ring(token),
            InstrumentMode::Latest => self.register_latest(token),
        }
    }
}
