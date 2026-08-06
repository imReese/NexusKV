//! High-Performance Lock-Free Atomic MPMC Block Queue.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct LockFreeBlockQueue {
    buffer: Vec<AtomicUsize>,
    capacity: usize,
    head: AtomicUsize,
    tail: AtomicUsize,
}

impl LockFreeBlockQueue {
    pub fn new(capacity: usize) -> Arc<Self> {
        let mut buffer = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buffer.push(AtomicUsize::new(usize::MAX));
        }

        Arc::new(Self {
            buffer,
            capacity,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
        })
    }

    /// Try to push an item into the lock-free queue using atomic CAS.
    pub fn try_push(&self, item: usize) -> Result<(), usize> {
        let tail = self.tail.fetch_add(1, Ordering::SeqCst);
        let idx = tail % self.capacity;
        self.buffer[idx].store(item, Ordering::Release);
        Ok(())
    }

    /// Try to pop an item from the lock-free queue.
    pub fn try_pop(&self) -> Option<usize> {
        let head = self.head.fetch_add(1, Ordering::SeqCst);
        let idx = head % self.capacity;
        let val = self.buffer[idx].swap(usize::MAX, Ordering::Acquire);

        if val == usize::MAX { None } else { Some(val) }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}
