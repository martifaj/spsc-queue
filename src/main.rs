//! SPSC queue demo for "Low-Latency Rust, Ep. 2".
//!
//! One thread pushes 0..ITEMS into a queue, another pops them in order.
//! Four queues, each one small change from the last:
//!
//!   1. std::sync::mpsc::sync_channel, the queue you'd reach for first
//!   2. a lock-free ring buffer, head and tail next to each other
//!   3. the same ring, head and tail padded onto their own cache lines
//!   4. padded, and each side keeps a private copy of the other's index
//!
//! Run with:  cargo run --release

use std::cell::UnsafeCell;
use std::hint::spin_loop;
use std::ops::Deref;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const ITEMS: u64 = 50_000_000;
const CAPACITY: usize = 4096; // slots; must be a power of two
const RUNS: usize = 7;

/// An index with nothing around it: head and tail end up side by side.
struct Plain(AtomicUsize);

/// An index on its own cache line (crossbeam's `CachePadded` idea; see Ep. 1).
#[repr(align(128))]
struct Padded(AtomicUsize);

impl Deref for Plain {
    type Target = AtomicUsize;
    fn deref(&self) -> &AtomicUsize {
        &self.0
    }
}

impl Deref for Padded {
    type Target = AtomicUsize;
    fn deref(&self) -> &AtomicUsize {
        &self.0
    }
}

/// Bounded single-producer, single-consumer ring buffer.
/// `I` picks the index layout: `Plain` (shared line) or `Padded` (own line).
struct Ring<I> {
    head: I, // next slot to read; only the consumer writes it
    tail: I, // next slot to write; only the producer writes it
    slots: Box<[UnsafeCell<u64>]>,
}

// Safety: the slots are only reached through the single Producer and single
// Consumer that `split` hands out, and head/tail keep them on different slots.
unsafe impl<I: Sync> Sync for Ring<I> {}

impl<I: Deref<Target = AtomicUsize>> Ring<I> {
    fn new(head: I, tail: I) -> Self {
        assert!(CAPACITY.is_power_of_two());
        let slots = (0..CAPACITY).map(|_| UnsafeCell::new(0)).collect();
        Ring { head, tail, slots }
    }

    /// The one producer and one consumer. With `CACHED`, each side keeps its
    /// own index locally and a cached copy of the other side's index.
    fn split<const CACHED: bool>(&mut self) -> (Producer<'_, I, CACHED>, Consumer<'_, I, CACHED>) {
        let ring = &*self;
        (
            Producer { ring, tail: 0, head_cache: 0 },
            Consumer { ring, head: 0, tail_cache: 0 },
        )
    }

    fn slot(&self, i: usize) -> *mut u64 {
        self.slots[i & (CAPACITY - 1)].get()
    }

    /// Bytes between the two indices: 8 means they share a cache line.
    fn index_gap(&self) -> usize {
        (&*self.tail as *const AtomicUsize as usize).abs_diff(&*self.head as *const _ as usize)
    }
}

struct Producer<'a, I, const CACHED: bool> {
    ring: &'a Ring<I>,
    tail: usize,       // CACHED: our own index, so we never read it back
    head_cache: usize, // CACHED: the last head we saw
}

impl<I: Deref<Target = AtomicUsize>, const CACHED: bool> Producer<'_, I, CACHED> {
    fn push(&mut self, v: u64) -> bool {
        let r = self.ring;
        let tail = if CACHED { self.tail } else { r.tail.load(Relaxed) };
        if CACHED {
            // Only read the shared head when our copy says the ring is full.
            if tail.wrapping_sub(self.head_cache) == CAPACITY {
                self.head_cache = r.head.load(Acquire);
                if tail.wrapping_sub(self.head_cache) == CAPACITY {
                    return false;
                }
            }
        } else if tail.wrapping_sub(r.head.load(Acquire)) == CAPACITY {
            return false;
        }
        // Safety: the consumer has moved past this slot, and only we write slots.
        unsafe { r.slot(tail).write(v) };
        self.tail = tail.wrapping_add(1);
        r.tail.store(self.tail, Release); // publishes the slot to the consumer
        true
    }
}

struct Consumer<'a, I, const CACHED: bool> {
    ring: &'a Ring<I>,
    head: usize,       // CACHED: our own index, so we never read it back
    tail_cache: usize, // CACHED: the last tail we saw
}

impl<I: Deref<Target = AtomicUsize>, const CACHED: bool> Consumer<'_, I, CACHED> {
    fn pop(&mut self) -> Option<u64> {
        let r = self.ring;
        let head = if CACHED { self.head } else { r.head.load(Relaxed) };
        if CACHED {
            // Only read the shared tail when our copy says the ring is empty.
            if head == self.tail_cache {
                self.tail_cache = r.tail.load(Acquire);
                if head == self.tail_cache {
                    return None;
                }
            }
        } else if head == r.tail.load(Acquire) {
            return None;
        }
        // Safety: the producer published this slot (Acquire above), and only we read slots.
        let v = unsafe { r.slot(head).read() };
        self.head = head.wrapping_add(1);
        r.head.store(self.head, Release); // hands the slot back to the producer
        Some(v)
    }
}

/// One run: push 0..ITEMS on one thread, pop on another, check the order.
fn run_ring<I, const CACHED: bool>(mut ring: Ring<I>) -> Duration
where
    I: Deref<Target = AtomicUsize> + Sync,
{
    let (mut tx, mut rx) = ring.split::<CACHED>();
    let start = Instant::now();
    let in_order = thread::scope(|s| {
        s.spawn(move || {
            for i in 0..ITEMS {
                while !tx.push(i) {
                    spin_loop();
                }
            }
        });
        s.spawn(move || {
            let mut ok = true;
            for i in 0..ITEMS {
                let v = loop {
                    match rx.pop() {
                        Some(v) => break v,
                        None => spin_loop(),
                    }
                };
                ok &= v == i;
            }
            ok
        })
        .join()
        .unwrap()
    });
    let t = start.elapsed();
    assert!(in_order, "items arrived out of order");
    t
}

fn run_channel() -> Duration {
    let (tx, rx) = mpsc::sync_channel(CAPACITY);
    let start = Instant::now();
    let in_order = thread::scope(|s| {
        s.spawn(move || {
            for i in 0..ITEMS {
                tx.send(i).unwrap();
            }
        });
        s.spawn(move || {
            let mut ok = true;
            for i in 0..ITEMS {
                ok &= rx.recv().unwrap() == i;
            }
            ok
        })
        .join()
        .unwrap()
    });
    let t = start.elapsed();
    assert!(in_order, "items arrived out of order");
    t
}

fn median(f: impl Fn() -> Duration) -> Duration {
    let mut v: Vec<Duration> = (0..RUNS).map(|_| f()).collect();
    v.sort();
    v[v.len() / 2]
}

fn plain() -> Ring<Plain> {
    Ring::new(Plain(AtomicUsize::new(0)), Plain(AtomicUsize::new(0)))
}

fn padded() -> Ring<Padded> {
    Ring::new(Padded(AtomicUsize::new(0)), Padded(AtomicUsize::new(0)))
}

fn main() {
    println!("1 producer, 1 consumer, {ITEMS} items, capacity {CAPACITY}");
    println!(
        "head/tail gap: plain {} B, padded {} B; runs: {RUNS} (median)\n",
        plain().index_gap(),
        padded().index_gap()
    );

    let results = [
        ("std mpsc::sync_channel", median(run_channel)),
        ("ring (head, tail same line)", median(|| run_ring::<_, false>(plain()))),
        ("ring + padded indices", median(|| run_ring::<_, false>(padded()))),
        ("ring + padded + cached", median(|| run_ring::<_, true>(padded()))),
    ];

    let base = results[0].1.as_secs_f64();
    println!("{:<30}{:>10}{:>12}{:>9}", "queue", "ns/item", "M items/s", "vs std");
    for (name, t) in results {
        let s = t.as_secs_f64();
        println!(
            "{:<30}{:>10.2}{:>12.1}{:>8.1}x",
            name,
            s * 1e9 / ITEMS as f64,
            ITEMS as f64 / s / 1e6,
            base / s
        );
    }
}
