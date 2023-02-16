use super::book::{Error, Order, OrderBook, OrderPrice, OrderView, Result};
use super::Side;

// use crate::Result;
use serde::{Deserialize, Serialize};
use std::iter::Sum;
use std::ops::{AddAssign, SubAssign};
use std::{fmt::Debug, hash::Hash};

/// ExecInfo has three generic parameters: K, L, I
/// K: origin id
/// L: limit price
/// I: info
#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct ExecInfo<K, L, I> {
    pub origin_id: K,
    pub order_price: OrderPrice<L>,
    pub info: I,
}

impl<K, L, I> ExecInfo<K, L, I> {
    fn from_reduced(origin_id: K, order_price: OrderPrice<L>, info: I) -> Self {
        ExecInfo {
            origin_id,
            order_price,
            info,
        }
    }
}

/// ExecOrder has three generic parameters: K, L, Q, I
/// K: origin id
/// L: limit price
/// Q: volume
/// I: info
pub type ExecOrder<K, L, Q, I> = Order<L, Q, ExecInfo<K, L, I>>;

/// CancelInfo has three generic parameters: K, I
/// K: origin id
/// I: info
#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct CancelInfo<K, I> {
    pub origin_id: K,
    pub info: I,
}

impl<K, I> CancelInfo<K, I> {
    fn from_reduced(origin_id: K, info: I) -> Self {
        CancelInfo { origin_id, info }
    }
}

/// CancelOrder has three generic parameters: K, L, Q, I
/// K: origin id
/// L: limit price
/// Q: volume
/// I: info
pub type CancelOrder<K, L, Q, I> = Order<OrderPrice<L>, Q, CancelInfo<K, I>>;

/// Account has three generic parameters: K, L, Q, I
/// Pending and cancelled will have OrderPrice<L> as price since price L was not determined
/// K: origin id
/// L: limit price
/// Q: volume
/// I: info
#[derive(Clone, Debug)]
pub struct Account<K, L, Q, I = ()>
where
    K: Eq + Hash,
    L: Ord,
{
    pending: OrderBook<K, OrderPrice<L>, Q, I>,
    executed: OrderBook<K, L, Q, ExecInfo<K, L, I>>,
    cancelled: OrderBook<K, OrderPrice<L>, Q, CancelInfo<K, I>>,
}

impl<K, L, Q, I> Default for Account<K, L, Q, I>
where
    K: Eq + Hash,
    L: Ord,
{
    fn default() -> Self {
        Self {
            pending: OrderBook::new(),
            executed: OrderBook::new(),
            cancelled: OrderBook::new(),
        }
    }
}

impl<K, L, Q, I> Account<K, L, Q, I>
where
    K: Eq + Hash,
    L: Ord,
{
    pub fn new() -> Self {
        Self::default()
    }
    pub fn pending(&self) -> &OrderBook<K, OrderPrice<L>, Q, I> {
        &self.pending
    }
    pub fn executed(&self) -> &OrderBook<K, L, Q, ExecInfo<K, L, I>> {
        &self.executed
    }
    pub fn cancelled(&self) -> &OrderBook<K, OrderPrice<L>, Q, CancelInfo<K, I>> {
        &self.cancelled
    }
}

impl<K, L, Q, I> Account<K, L, Q, I>
where
    K: Eq + Hash + Debug + Clone,
    L: Ord + Debug + Clone,
    Q: AddAssign + SubAssign + Default + Clone + Sum + Debug + Ord,
    I: Clone + Debug,
{
    pub fn order(
        &mut self,
        id: K,
        side: Side,
        price: OrderPrice<L>,
        quantity: Q,
        info: I,
    ) -> std::result::Result<(), Error> {
        self.pending
            .insert(id, side, price, quantity, info)
            .and(Ok(()))
    }

    pub fn cancel(
        &mut self,
        id: K,
        origin_id: K,
        quantity: Option<Q>,
    ) -> Result<&CancelOrder<K, L, Q, I>> {
        let order = match quantity {
            Some(quantity) => self
                .pending
                .reduce(&origin_id, quantity)
                .map(OrderView::to_order),
            None => self.pending.remove(&origin_id),
        };
        match order {
            Some(Order {
                side,
                price,
                quantity,
                info,
            }) => {
                let cancel_info = CancelInfo::from_reduced(origin_id, info);
                self.cancelled
                    .insert(id, side, price, quantity, cancel_info)
            }
            None => Err(Error::OrderNotFound),
        }
    }

    pub fn execute(
        &mut self,
        id: K,
        origin_id: K,
        price: L,
        quantity: Q,
    ) -> Result<&ExecOrder<K, L, Q, I>> {
        match self.pending.reduce(&origin_id, quantity) {
            Some(order) => {
                let Order {
                    side,
                    price: order_price,
                    quantity,
                    info,
                } = order.to_order();
                let exec_info = ExecInfo::from_reduced(origin_id, order_price, info);
                self.executed.insert(id, side, price, quantity, exec_info)
            }
            None => Err(Error::OrderNotFound),
        }
    }
}
