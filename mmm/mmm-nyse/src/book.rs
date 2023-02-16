use std::collections::HashMap;

use anyhow::bail;
// use mmm_core::collections::{
//     book::{OrderBook, PriceTimePriority},
//     Side,
// };
use taq::enums::CrossType;
use mmm_us::{OrderBook, PriceTimePriority, Side};
pub type IdTimeQuantity = (u64, u64, u64);

#[derive(Clone, Debug)]
pub struct Message {
    pub time: u64,
    pub body: Body,
}

#[derive(Debug, Clone)]
pub enum Body {
    AddOrder {
        reference: u64,
        shares: u64,
        price: u64,
        side: Side,
        mpid_val: u64,
    },
    DeleteOrder {
        reference: u64,
    },
    OrderCancelled {
        reference: u64,
        cancelled: u64,
    },
    ReplaceOrder {
        new_reference: u64,
        shares: u64,
        price: u64,
        old_reference: u64,
    },
    OrderExecuted {
        reference: u64,
        executed: u64,
    },
    OrderExecutedWithPrice {
        reference: u64,
        executed: u64,
    },
    CrossTrade {
        cross_type: CrossType,
    },
    NonCrossTrade {},
}

#[derive(Debug)]
pub struct NyseOrderBook {
    max_ref: u64,
    book: OrderBook<u64, u64, u64, u64>,
    with_validation: bool,
}

impl NyseOrderBook {
    pub fn new(with_validation: bool) -> Self {
        Self {
            max_ref: 0,
            book: OrderBook::new(),
            with_validation,
        }
    }
    pub fn handle(&mut self, msg: &Message) -> anyhow::Result<()> {
        // eprintln!("{:?}", msg);
        if let (true, Some((ar, at)), Some((br, bt))) = (
            self.with_validation,
            self.book.ask_top().map(|(k, o)| (*k, o.clone())),
            self.book.bid_top(),
        ) {
            assert!(
                at.price > bt.price,
                "{}, {}| {} <= {}",
                ar,
                br,
                at.price,
                bt.price
            );
        }
        match msg.body {
            Body::AddOrder {
                reference,
                side,
                shares,
                price,
                ..
            } => {
                if reference > self.max_ref {
                    self.max_ref = reference;
                    match side {
                        Side::Ask => {
                            self.book
                                .insert(reference, side, price, shares, msg.time)
                                .unwrap();
                        }
                        Side::Bid => {
                            self.book
                                .insert(reference, side, price, shares, msg.time)
                                .unwrap();
                        }
                    }
                } else {
                    match side {
                        Side::Ask => {
                            self.book
                                .sorted_insert_ask_by_key(reference, price, shares, msg.time)
                                .unwrap();
                        }
                        Side::Bid => {
                            self.book
                                .sorted_insert_bid_by_key(reference, price, shares, msg.time)
                                .unwrap();
                        }
                    }
                }
            }
            Body::DeleteOrder { reference } => {
                self.book.remove(&reference).unwrap();
            }
            Body::OrderCancelled {
                reference,
                cancelled,
            } => {
                self.book.reduce(&reference, cancelled).unwrap();
            }
            Body::ReplaceOrder {
                old_reference,
                new_reference,
                shares,
                price,
            } => {
                let s = self.book.remove(&old_reference).unwrap().side;
                match &s {
                    Side::Ask => self
                        .book
                        .insert(new_reference, s, price, shares, msg.time)
                        .unwrap(),
                    Side::Bid => self
                        .book
                        .insert(new_reference, s, price, shares, msg.time)
                        .unwrap(),
                };
            }
            Body::OrderExecuted {
                reference,
                executed,
            } => {
                if self.with_validation {
                    let ask_top = self.book.ask_top().map(|(k, o)| (*k, o.quantity));
                    let bid_top = self.book.bid_top().map(|(k, o)| (*k, o.quantity));
                    let top_qty = match (ask_top, bid_top) {
                        (Some((top_ref, quantity)), _) if top_ref == reference => quantity,
                        (_, Some((top_ref, quantity))) if top_ref == reference => quantity,
                        _ => {
                            println!("{:?}",self.book.get(&reference).unwrap());
                            println!("{:?}",self.book.get(&ask_top.unwrap().0).unwrap());
                            println!("{:?}",self.book.get(&bid_top.unwrap().0).unwrap());
                            unreachable!("[Error] {:?} {:?} {:?}\n", reference, ask_top, bid_top);
                        },
                    };
                    assert!(top_qty >= executed);
                }
                self.book.reduce(&reference, executed).unwrap();
            }
            Body::OrderExecutedWithPrice {
                reference,
                executed,
                ..
            } => {
                self.book.reduce(&reference, executed).unwrap();
            }
            // Body::CrossTrade {
            //     ..
            // } => {
            //     bail!("[ABNORMALLY] {:?}", msg.body);
            // }
            // | Body::BrokenTrade { .. }
            // | Body::TradingAction {
            //     trading_state:
            //         TradingState::Halted | TradingState::Paused | TradingState::QuotationOnly,
            //     ..
            // } => {
            //     return Err(format!("[ABNORMALLY] {:?}", msg.body));
            // }
            // Body::LULDAuctionCollar { .. }
            // | Body::RetailPriceImprovementIndicator(_)
            // | Body::Imbalance(_)
            // | Body::IpoQuotingPeriod(_)
            // | Body::MwcbDeclineLevel { .. }
            // | Body::Breach(_)
            // | Body::NonCrossTrade(_)
            // | Body::ParticipantPosition(_)
            // | Body::RegShoRestriction { .. }
            // | Body::StockDirectory(_)
            // | Body::SystemEvent { .. }
            | Body::CrossTrade{..}
            | Body::NonCrossTrade {}
            // | Body::TradingAction {
            //     trading_state: TradingState::Trading,
            //     ..
            // }
            => {}
        };
        Ok(())
    }
    pub fn level_summary(&mut self, level: usize) -> HashMap<String, HashMap<u64, u64>> {
        let mut summary = HashMap::new();

        summary.insert(
            "Ask".to_string(),
            self.book
                .sorted_ask_prices()
                .take(level)
                .cloned()
                .collect::<Vec<_>>()
                .into_iter()
                .map(|p| (p, self.book.ask_volume_at(p)))
                .collect::<HashMap<_, _>>(),
        );

        summary.insert(
            "Bid".to_string(),
            self.book
                .sorted_bid_prices()
                .take(level)
                .cloned()
                .collect::<Vec<_>>()
                .into_iter()
                .map(|p| (p, self.book.bid_volume_at(p)))
                .collect::<HashMap<_, _>>(),
        );

        summary
    }

    pub fn bbo(&self) -> (Option<u64>, Option<u64>) {
        let ask1 = self.book.sorted_ask_prices().next();
        let bid1 = self.book.sorted_bid_prices().next();

        match (ask1, bid1) {
            (None, None) => (None, None),
            (None, Some(b)) => (None, Some(*b)),
            (Some(a), None) => (Some(*a), None),
            (Some(a), Some(b)) => {
                let sum = *a + *b;
                (Some(*a), Some(*b))
            }
        }
    }

    fn get_spread_limit(&self, spread: u64) -> (Option<u64>, Option<u64>) {
        let ask1 = self.book.sorted_ask_prices().next();
        let bid1 = self.book.sorted_bid_prices().next();

        match (ask1, bid1) {
            (None, None) => (None, None),
            (None, Some(b)) => (None, Some(*b - spread)),
            (Some(a), None) => (Some(*a + spread), None),
            (Some(a), Some(b)) => {
                let sum = *a + *b;
                let mid = sum / 2;
                let ask_limit = mid + spread;
                let mut bid_limit = mid - spread;
                // adjust bid_limit consider remainder
                if sum % 2 != 0 {
                    bid_limit += 1;
                }
                (Some(ask_limit), Some(bid_limit))
            }
        }
    }

    pub fn spread_summary(&mut self, spread: u64) -> HashMap<String, HashMap<u64, u64>> {
        let mut summary = HashMap::new();
        let (ask_limit, bid_limit) = self.get_spread_limit(spread);

        summary.insert(
            "Ask".to_string(),
            self.book
                .sorted_ask_prices()
                .take_while(|p| {
                    if let Some(al) = ask_limit {
                        **p <= al
                    } else {
                        false
                    }
                })
                .cloned()
                .collect::<Vec<_>>()
                .into_iter()
                .map(|p| (p, self.book.ask_volume_at(p)))
                .collect::<HashMap<_, _>>(),
        );

        summary.insert(
            "Bid".to_string(),
            self.book
                .sorted_bid_prices()
                .take_while(|p| {
                    if let Some(bl) = bid_limit {
                        **p >= bl
                    } else {
                        false
                    }
                })
                .cloned()
                .collect::<Vec<_>>()
                .into_iter()
                .map(|p| (p, self.book.bid_volume_at(p)))
                .collect::<HashMap<_, _>>(),
        );

        summary
    }

    pub fn level_snapshot(
        &mut self,
        level: usize,
    ) -> HashMap<String, HashMap<u64, Vec<IdTimeQuantity>>> {
        let mut snapshot = HashMap::new();

        let mut ask_snapshot = HashMap::new();

        for price in self
            .book
            .sorted_ask_prices()
            .cloned()
            .take(level)
            .collect::<Vec<_>>()
        {
            let mut price_queue = Vec::new();
            for (id, order) in self
                .book
                .ask_orders_at(price, None, PriceTimePriority::BothDesc)
                .orders
            {
                price_queue.push((id, *order.info, *order.quantity() as u64))
            }
            ask_snapshot.insert(price, price_queue);
        }

        snapshot.insert("Ask".to_string(), ask_snapshot);

        let mut bid_snapshot = HashMap::new();

        for price in self
            .book
            .sorted_bid_prices()
            .cloned()
            .take(level)
            .collect::<Vec<_>>()
        {
            let mut price_queue = Vec::new();
            for (id, order) in self
                .book
                .bid_orders_at(price, None, PriceTimePriority::BothDesc)
                .orders
            {
                price_queue.push((id, *order.info, *order.quantity() as u64))
            }
            bid_snapshot.insert(price, price_queue);
        }

        snapshot.insert("Bid".to_string(), bid_snapshot);

        snapshot
    }

    pub fn spread_snapshot(
        &mut self,
        spread: u64,
    ) -> HashMap<String, HashMap<u64, Vec<IdTimeQuantity>>> {
        let mut snapshot = HashMap::new();

        let mut ask_snapshot = HashMap::new();

        let (ask_limit, bid_limit) = self.get_spread_limit(spread);

        for price in self
            .book
            .sorted_ask_prices()
            .cloned()
            .take_while(|p| {
                if let Some(al) = ask_limit {
                    *p <= al
                } else {
                    false
                }
            })
            .collect::<Vec<_>>()
        {
            let mut price_queue = Vec::new();
            for (id, order) in self
                .book
                .ask_orders_at(price, None, PriceTimePriority::BothDesc)
                .orders
            {
                price_queue.push((id, *order.info, *order.quantity() as u64))
            }
            ask_snapshot.insert(price, price_queue);
        }

        snapshot.insert("Ask".to_string(), ask_snapshot);

        let mut bid_snapshot = HashMap::new();

        for price in self
            .book
            .sorted_bid_prices()
            .cloned()
            .take_while(|p| {
                if let Some(bl) = bid_limit {
                    *p >= bl
                } else {
                    false
                }
            })
            .collect::<Vec<_>>()
        {
            let mut price_queue = Vec::new();
            for (id, order) in self
                .book
                .bid_orders_at(price, None, PriceTimePriority::BothDesc)
                .orders
            {
                price_queue.push((id, *order.info, *order.quantity() as u64))
            }
            bid_snapshot.insert(price, price_queue);
        }

        snapshot.insert("Bid".to_string(), bid_snapshot);

        snapshot
    }
}

// -------------------------------------------------------------------
