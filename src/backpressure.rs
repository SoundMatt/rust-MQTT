// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! Bounded delivery channel that can genuinely implement all three RELAY
//! back-pressure policies (spec §10.5.3 / §14).
//!
//! `tokio::sync::mpsc`'s bounded channel only supports "drop the arriving
//! message when full" from the sender side — `try_send` fails without
//! touching the queue, so there is no way for a sender-only handle to evict
//! the head of the queue. That makes `BackPressurePolicy::DropOldest`
//! ("drain one message from the channel, then enqueue the new one",
//! spec §10.5.3) structurally impossible to implement correctly on top of a
//! plain `mpsc::Sender`. `RingSender`/`RingReceiver` wrap a
//! `Mutex<VecDeque<T>>` so the sending side can genuinely drain-then-enqueue.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::{Mutex, Notify};

struct Shared<T> {
    queue: Mutex<VecDeque<T>>,
    capacity: usize,
    // Separate `Notify`s for the two distinct wake conditions. Using a
    // single `Notify` for both would let `notify_one()` wake the wrong role
    // (e.g. a blocked sender waiting for room stealing the wake meant for
    // the receiver, or vice versa) — `Notify` only stores one permit and
    // hands it to an arbitrary waiter, it does not distinguish why callers
    // are waiting.
    item_available: Notify,
    space_available: Notify,
    closed: AtomicBool,
}

/// Sending half of a [`channel`].
pub struct RingSender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for RingSender<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

/// Receiving half of a [`channel`]. Single-consumer, like `mpsc::Receiver`.
pub struct RingReceiver<T> {
    shared: Arc<Shared<T>>,
}

/// Create a bounded ring channel with room for `capacity` items (minimum 1).
pub fn channel<T>(capacity: usize) -> (RingSender<T>, RingReceiver<T>) {
    let capacity = capacity.max(1);
    let shared = Arc::new(Shared {
        queue: Mutex::new(VecDeque::with_capacity(capacity)),
        capacity,
        item_available: Notify::new(),
        space_available: Notify::new(),
        closed: AtomicBool::new(false),
    });
    (
        RingSender {
            shared: shared.clone(),
        },
        RingReceiver { shared },
    )
}

impl<T: Send + 'static> RingSender<T> {
    /// `BackPressurePolicy::DropNewest`: drop the arriving item when full.
    /// Returns `true` if the item was enqueued, `false` if it was dropped.
    pub async fn push_drop_newest(&self, item: T) -> bool {
        let mut q = self.shared.queue.lock().await;
        if q.len() >= self.shared.capacity {
            return false;
        }
        q.push_back(item);
        drop(q);
        self.shared.item_available.notify_one();
        true
    }

    /// `BackPressurePolicy::DropOldest`: evict the oldest queued item to make
    /// room, then enqueue the arriving item (spec §10.5.3). The arriving item
    /// is always enqueued. Returns `true` if an older item was evicted to
    /// make room for it.
    pub async fn push_drop_oldest(&self, item: T) -> bool {
        let mut q = self.shared.queue.lock().await;
        let evicted = if q.len() >= self.shared.capacity {
            q.pop_front();
            true
        } else {
            false
        };
        q.push_back(item);
        drop(q);
        self.shared.item_available.notify_one();
        evicted
    }

    /// `BackPressurePolicy::Block`: wait asynchronously until there is room.
    pub async fn push_block(&self, item: T) {
        let mut item = Some(item);
        loop {
            {
                let mut q = self.shared.queue.lock().await;
                if q.len() < self.shared.capacity {
                    q.push_back(item.take().expect("item taken exactly once"));
                    drop(q);
                    self.shared.item_available.notify_one();
                    return;
                }
            }
            self.shared.space_available.notified().await;
        }
    }

    /// Wake any blocked receiver so it observes end-of-stream.
    pub fn close(&self) {
        self.shared.closed.store(true, Ordering::SeqCst);
        self.shared.item_available.notify_waiters();
        self.shared.space_available.notify_waiters();
    }
}

impl<T> RingReceiver<T> {
    /// Receive the next item, or `None` once the channel is closed and
    /// drained.
    pub async fn recv(&mut self) -> Option<T> {
        loop {
            {
                let mut q = self.shared.queue.lock().await;
                if let Some(item) = q.pop_front() {
                    drop(q);
                    // A slot just opened up — wake a sender blocked in
                    // push_block() waiting for room.
                    self.shared.space_available.notify_one();
                    return Some(item);
                }
                if self.shared.closed.load(Ordering::SeqCst) {
                    return None;
                }
            }
            self.shared.item_available.notified().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn drop_newest_discards_arriving_item_when_full() {
        let (tx, mut rx) = channel::<u8>(2);
        assert!(tx.push_drop_newest(1).await);
        assert!(tx.push_drop_newest(2).await);
        // Full: arriving item 3 is dropped, queue keeps [1, 2].
        assert!(!tx.push_drop_newest(3).await);
        assert_eq!(rx.recv().await, Some(1));
        assert_eq!(rx.recv().await, Some(2));
    }

    #[tokio::test]
    async fn drop_oldest_evicts_head_and_keeps_newest() {
        let (tx, mut rx) = channel::<u8>(3);
        for i in 0u8..10 {
            tx.push_drop_oldest(i).await;
        }
        // Depth 3, values 0..10: survivors must be the newest three: 7, 8, 9.
        assert_eq!(rx.recv().await, Some(7));
        assert_eq!(rx.recv().await, Some(8));
        assert_eq!(rx.recv().await, Some(9));
    }

    #[tokio::test]
    async fn drop_oldest_reports_eviction() {
        let (tx, _rx) = channel::<u8>(2);
        assert!(!tx.push_drop_oldest(1).await);
        assert!(!tx.push_drop_oldest(2).await);
        assert!(tx.push_drop_oldest(3).await, "third push must evict item 1");
    }

    #[tokio::test]
    async fn block_delivers_all_items_in_order() {
        let (tx, mut rx) = channel::<u8>(2);
        let tx2 = tx.clone();
        let sender = tokio::spawn(async move {
            for i in 0u8..5 {
                tx2.push_block(i).await;
            }
        });
        let mut got = vec![];
        for _ in 0u8..5 {
            got.push(rx.recv().await.unwrap());
        }
        sender.await.unwrap();
        assert_eq!(got, vec![0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn close_unblocks_receiver() {
        let (tx, mut rx) = channel::<u8>(2);
        tx.close();
        assert_eq!(rx.recv().await, None);
    }
}
