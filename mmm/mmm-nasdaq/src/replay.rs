use std::collections::{HashMap, VecDeque};

use crate::{book::NasdaqOrderBook, book::Message};

struct TimeBasedReplay {
    book: NasdaqOrderBook,
    messages: VecDeque<Vec<u64>>,
}

pub type VolumeResult = (u64, HashMap<String, HashMap<u64, u64>>, Vec<Vec<u64>>, bool);
pub type QueueResult = (
    u64,
    HashMap<String, HashMap<u64, Vec<(u64, u64, u64)>>>,
    Vec<Vec<u64>>,
    bool,
);

impl TimeBasedReplay {
    fn new(messages: VecDeque<Vec<u64>>) -> Self {
        let book = NasdaqOrderBook::new(false);
        Self { book, messages }
    }

    fn step(&mut self) -> Option<(u64, Vec<Vec<u64>>, bool)> {
        let mut messages = vec![self.messages.pop_front()?];
        let timestamp = messages[0][1];

        while let Some(front) = self.messages.front() {
            if front[1] == timestamp {
                messages.push(self.messages.pop_front().unwrap());
            } else {
                break;
            }
        }

        for msg in &messages {
            self.book.handle(&Message::from(&**msg)).unwrap();
        }

        // println!("AB {} {}", sum["Ask"].keys().len(), sum["Bid"].keys().len());
        Some((timestamp, messages, self.messages.is_empty()))
    }
}

pub enum OrderbookDepth {
    Level(usize),
    Spread(u64),
}

pub struct TimeBasedQueueReplay {
    inner: TimeBasedReplay,
    orderbook_depth: OrderbookDepth,
}

impl TimeBasedQueueReplay {
    pub fn new(messages: VecDeque<Vec<u64>>, orderbook_depth: OrderbookDepth) -> Self {
        let inner = TimeBasedReplay::new(messages);
        Self {
            inner,
            orderbook_depth,
        }
    }
    pub fn step(&mut self) -> Option<QueueResult> {
        let s = match self.orderbook_depth {
            OrderbookDepth::Level(l) => self.inner.book.level_snapshot(l),
            OrderbookDepth::Spread(s) => self.inner.book.spread_snapshot(s),
        };
        self.inner.step().map(|(t, ms, d)| (t, s, ms, d))
    }
}

pub struct TimeBasedVolumeReplay {
    inner: TimeBasedReplay,
    orderbook_depth: OrderbookDepth,
}

impl TimeBasedVolumeReplay {
    pub fn new(messages: VecDeque<Vec<u64>>, orderbook_depth: OrderbookDepth) -> Self {
        let inner = TimeBasedReplay::new(messages);
        Self {
            inner,
            orderbook_depth,
        }
    }
    pub fn step(&mut self) -> Option<VolumeResult> {
        let s = match self.orderbook_depth {
            OrderbookDepth::Level(l) => self.inner.book.level_summary(l),
            OrderbookDepth::Spread(s) => self.inner.book.spread_summary(s),
        };
        self.inner.step().map(|(t, ms, d)| (t, s, ms, d))
    }

    pub fn step_full(&mut self) -> Option<VolumeResult> {
        if let Some((t, ms, d)) = self.inner.step() {
            let s = match self.orderbook_depth {
                OrderbookDepth::Level(l) => self.inner.book.level_summary(l),
                OrderbookDepth::Spread(s) => self.inner.book.spread_summary(s),
            };
            return Some((t, s, ms, d));
        } else {
            return None;
        };
    }
}
