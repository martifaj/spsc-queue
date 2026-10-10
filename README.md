# spsc-queue

Benchmark from **Low-Latency Rust, Ep. 2: My Lock-Free Queue Lost to std (Then Beat It 2.5x)**.

📺 Video: [LINK]
📺 Ep. 1, False Sharing: https://youtu.be/ecu1ocgCcFQ

One thread pushes 50 million items into a bounded queue and another pops them. The program checks that every item arrives, in order. It compares four queues, each one small change from the last:

| queue | what changes |
|---|---|
| **std mpsc::sync_channel** | the queue you'd reach for first (a port of crossbeam-channel) |
| **ring (head, tail same line)** | a textbook lock-free SPSC ring buffer: `head` and `tail` sit side by side on one cache line, and every `push`/`pop` reads the other side's index |
| **ring + padded indices** | `head` and `tail` each on their own cache line (`#[repr(align(128))]`), which fixes the false sharing |
| **ring + padded + cached** | each side keeps its own index locally and a cached copy of the other side's, only reading the shared one when the ring looks full or empty |

## Run it

Requires Rust 1.85 or newer (edition 2024). No dependencies.

```sh
cargo run --release
```

Always use `--release`. It takes about 70 seconds on the machine below. Run it on an otherwise idle machine.

## Results from the video

Raspberry Pi 5, 4x Cortex-A76 @ 3.0 GHz (overclocked; stock is 2.4 GHz), `performance` governor. Capacity 4096 slots, median of 7 runs.

```text
1 producer, 1 consumer, 50000000 items, capacity 4096
head/tail gap: plain 8 B, padded 128 B; runs: 7 (median)

queue                            ns/item   M items/s   vs std
std mpsc::sync_channel             29.81        33.6     1.0x
ring (head, tail same line)        83.83        11.9     0.4x
ring + padded indices              64.39        15.5     0.5x
ring + padded + cached             11.96        83.6     2.5x
```

Across four runs, the first three rows varied by at most about 3%. The cached version was the noisy one: between 11.8 and 14.2 ns per item over four runs (2.2x to 2.5x faster than std).

**Your numbers will differ.** Core count, cache layout and how far apart the two cores are all change the cost of moving a cache line between them. The benchmark doesn't pin threads to cores (std has no API for it), so thread placement can also add noise. Leave a comment on the video with your CPU and results.

## Notes

- **Why the textbook ring is slow.** Every `push` reads `head`, which the consumer writes on every `pop`, and every `pop` reads `tail`, which the producer writes on every `push`. That's at least two cache-line transfers per item, whether or not the indices are padded.
- **Why caching is safe.** The consumer only ever moves `head` forward, so a stale copy can only make the producer think there's *less* room than there is. It's cautious, never wrong. The same goes for the consumer's cached `tail`.
- **Why std beat the first version.** In std's bounded channel (`library/std/src/sync/mpmc/array.rs`), each slot carries a stamp, so a sender checks the slot it's about to write and only reads the receiver's index when that slot looks full. It also supports many senders and receivers, blocking and disconnection, which this ring doesn't.
- **Memory ordering.** The producer writes the slot, then publishes it with `tail.store(.., Release)`; the consumer's `tail.load(Acquire)` guarantees it then sees the value. The mirror image protects slot reuse. Loading your *own* index can be `Relaxed`, because only that thread writes it.
- **Soundness.** `Ring::split(&mut self)` hands out exactly one `Producer` and one `Consumer`, so the single-producer, single-consumer rule is enforced by the types.
- **In real code, use a tested crate:** [`rtrb`](https://docs.rs/rtrb) or [`ringbuf`](https://docs.rs/ringbuf) for SPSC, or [`crossbeam`](https://docs.rs/crossbeam) when you need more than one producer.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
