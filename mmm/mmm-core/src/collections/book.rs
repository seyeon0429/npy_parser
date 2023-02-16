use super::Side;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::collections::{hash_map, BTreeMap, VecDeque};
use std::fmt::Debug;
use std::iter::Sum;
use std::ops::{AddAssign, SubAssign};
use std::{collections::HashMap, hash::Hash};
use thiserror::Error;
const GARBAGE_THRESHOLD: f32 = 0.5;

#[derive(Error, PartialEq, Eq, Debug, Clone)]
pub enum Error {
    #[error("A collision of order id has occurred.")]
    KeyAlreadyExists,
    #[error("order id not found.")]
    OrderNotFound,
}

pub type Result<T> = std::result::Result<T, Error>;

/// Order type with generic parameter: P, Q, I
/// P: Any type that can represent "Price"
/// Q: Any type that can represent "Quantity"
/// I: Any tyme that can represent "Info"
/// There are no other trait bounds for P, Q, I
/// # Examples
///
/// ```
///
/// let order = Order::new(Side::Ask, 0 as i64, 0 as i64, String::new());
///
#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub struct Order<P, Q, I> {
    pub side: Side,
    pub price: P,
    pub quantity: Q,
    pub info: I,
}

impl<P, Q, I> Order<P, Q, I> {
    fn new(side: Side, price: P, quantity: Q, info: I) -> Self {
        Order {
            side,
            price,
            quantity,
            info,
        }
    }
}

/// OrderPrice is price type with generic parameter:L
/// L: any time that can represent "Price"
/// Some types will have OrderPrice<L> if the price has not been determined yet(AddOrder)
/// Types with determined price will have just L(Exec)
/// # Examples
/// ```
/// struct LimitPrice {
///     inner: u64
/// }
/// let orderprice = OrderPrice::Limit(LimitPrice {inner: 1000});
/// let orderprice = OrderPrice::Limit(1000);
/// let orderprice = OrderPrice::Market;
///
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug, Serialize, Deserialize, Hash)]
pub enum OrderPrice<L> {
    Market,
    Limit(L),
}

impl<L> OrderPrice<L> {
    pub const fn market() -> Self {
        Self::Market
    }
    pub const fn limit(price: L) -> Self {
        Self::Limit(price)
    }
    pub const fn is_market(&self) -> bool {
        matches!(self, Self::Market)
    }
    pub fn as_limit(&self) -> Option<&L> {
        match &self {
            OrderPrice::Market => None,
            OrderPrice::Limit(p) => Some(p),
        }
    }
}

/// PartialOrder represents orders which is bound to a lifetime 'a.
/// This type can be turned into an owned type "Order" through to_order()
/// Partial order exitst to minimize memory overhead by borrowing all the data needed to "represent" order.
/// Thus, all the fields are borrowed with lifetime 'a
/// PartialOrder has same generics as Order
/// P: Any type that can represent "Price"
/// Q: Any type that can represent "Quantity"
/// I: Any tyme that can represent "Info"
/// There are no other trait bounds for P, Q, I
/// # Examples
/// ```
/// let partial_order = PartialOrder {side: &Side::Ask,price: &(0 as i64),quantity: 0 as i64,info: &String::new()};
///
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct PartialOrder<'a, P, Q, I> {
    pub side: &'a Side,
    pub price: &'a P,
    pub(crate) quantity: Q,
    pub info: &'a I,
}

impl<P: Clone, Q, I: Clone> PartialOrder<'_, P, Q, I> {
    pub fn quantity(&self) -> &Q {
        &self.quantity
    }
    pub fn to_order(self) -> Order<P, Q, I> {
        Order {
            side: *self.side,
            price: self.price.clone(),
            quantity: self.quantity,
            info: self.info.clone(),
        }
    }
}

/// Orderviews dictates how orders can be viewed, either partial or full.
/// Since orderview can be partial, it is also bounded by some lifetime 'a
/// Orderviews has same generics as Order
/// P: Any type that can represent "Price"
/// Q: Any type that can represent "Quantity"
/// I: Any tyme that can represent "Info"
/// There are no other trait bounds for P, Q, I
#[derive(PartialEq, Eq, Debug)]
pub enum OrderView<'a, P, Q, I> {
    Partial(PartialOrder<'a, P, Q, I>),
    Full(Order<P, Q, I>),
}

impl<P: Clone, Q: Clone, I: Clone> OrderView<'_, P, Q, I> {
    pub fn price(&self) -> &P {
        match self {
            OrderView::Partial(o) => o.price,
            OrderView::Full(o) => &o.price,
        }
    }
    pub fn quantity(&self) -> &Q {
        match self {
            OrderView::Partial(o) => &o.quantity,
            OrderView::Full(o) => &o.quantity,
        }
    }
    pub fn info(&self) -> &I {
        match self {
            OrderView::Partial(o) => o.info,
            OrderView::Full(o) => &o.info,
        }
    }
    pub fn to_order(self) -> Order<P, Q, I> {
        match self {
            OrderView::Partial(o) => o.to_order(),
            OrderView::Full(o) => o,
        }
    }
}

/// OrderMap is a generic map of orders.
/// It is used to keep track of existing orders.
/// Value is the Order<P, Q, I> struct and key is generic K
/// K has to implement eq + hash since it should be used a hash key
/// K, key, will usually be something like order_id in the form of uuid or strictly increasing numbers
/// # Examples
/// ```
/// let ordermap: OrderMap<i64, i64, i64, String> = OrderMap::new();
///
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderMap<K: Eq + Hash, P, Q, I>(HashMap<K, Order<P, Q, I>>);

impl<K: Eq + Hash, P, Q, I> OrderMap<K, P, Q, I> {
    fn new() -> Self {
        Self(HashMap::new())
    }
}

/// BookCleaner can only be called through ::clean method.
/// Since bookcleaner holds the orderqueue with a reference, it also has some lifetime 'a
/// Calling ::clean with some ordermap will remove all orders in the orderqueue that are also included in the orderqueue.
/// For example, it can be called to remove entire orders on ordermap from the orderqueue
#[must_use = "this `BookCleaner` should be handled with `BookCleaner::clean`"]
struct BookCleaner<'a, K, Q>(&'a mut OrderQueue<K, Q>);

impl<K: Eq + Hash + Clone, Q> BookCleaner<'_, K, Q> {
    fn clean<P, I>(self, order_map: &OrderMap<K, P, Q, I>) {
        self.0.order_ids = self
            .0
            .order_ids
            .iter()
            .filter(|id| order_map.0.contains_key(id))
            .cloned()
            .collect();
    }
}

/// OrderQueue keeps track of the order_ids(but not the information about each orders) and the total volume
/// Imagine a queue for each price level in the entire orderbook.
/// It has generic: K, Q
/// K: represent order_ids, very similar to key value in the order map
/// Q: represents the volume
#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct OrderQueue<K, Q> {
    count: usize,
    volume: Q,
    order_ids: VecDeque<K>,
}

impl<K, Q> OrderQueue<K, Q> {
    fn new() -> Self
    where
        Q: Default,
    {
        Self {
            count: 0,
            volume: Q::default(),
            order_ids: VecDeque::new(),
        }
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn volume(&self) -> &Q {
        &self.volume
    }

    pub fn order_ids(&self) -> &VecDeque<K> {
        &self.order_ids
    }
}

impl<K, Q> OrderQueue<K, Q>
where
    Q: AddAssign + SubAssign + Clone,
{
    fn push(&mut self, id: K, quantity: Q) {
        self.order_ids.push_back(id);
        self.count += 1;
        self.volume += quantity;
    }

    fn sorted_insert_by_key(&mut self, id: K, quantity: Q)
    where
        K: Ord,
    {
        let index = match self.order_ids.binary_search(&id) {
            Ok(n) => n,
            Err(n) => n,
        };
        self.order_ids.insert(index, id);
        self.count += 1;
        self.volume += quantity;
    }

    fn reduce(&mut self, quantity: Q)
    where
        Q: SubAssign,
    {
        self.volume -= quantity;
    }

    // this operation does not remove `K` from `orders`
    // removed order ids are lazily removed or garbage collected.
    fn remove(&mut self, quantity: Q)
    where
        Q: SubAssign,
    {
        self.count -= 1;
        self.reduce(quantity);
    }
}

/// Bookstatus keeps track of the current book "status" but not to the individual order level.
/// Order_ids are stored, but info about orders are not included in the bookstatus.
/// Use OrderBook to store all the info about each orders
/// Bookstatus will store data(btreemap) using price as the key, thus generic P should implement trait Ord
/// BOokstatus has generics: K, P, Q
/// K: order_id,
/// P: price
/// Q: quantity
#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct BookStatus<K, P: Ord, Q>(BTreeMap<P, OrderQueue<K, Q>>);

impl<K, P: Ord, Q> BookStatus<K, P, Q>
where
    Q: AddAssign + SubAssign + Default + Clone + Sum,
{
    fn push(&mut self, id: K, price: P, quantity: Q) {
        self.0
            .entry(price)
            .or_insert_with(OrderQueue::new)
            .push(id, quantity)
    }

    fn sorted_insert_by_key(&mut self, id: K, price: P, quantity: Q)
    where
        K: Ord,
    {
        self.0
            .entry(price)
            .or_insert_with(OrderQueue::new)
            .sorted_insert_by_key(id, quantity);
    }

    fn top(&mut self) -> Option<(&P, &mut OrderQueue<K, Q>)> {
        self.0.iter_mut().find(|(_, queue)| queue.count > 0)
    }

    fn bottom(&mut self) -> Option<(&P, &mut OrderQueue<K, Q>)> {
        self.0.iter_mut().rev().find(|(_, queue)| queue.count > 0)
    }

    fn reduce(&mut self, price: &P, quantity: Q) {
        self.0.get_mut(price).unwrap().reduce(quantity);
    }

    fn remove(&mut self, price: &P, quantity: Q) -> Option<BookCleaner<K, Q>> {
        let order_queue = self.0.get_mut(price).unwrap();
        order_queue.remove(quantity);

        if order_queue.count == 0 {
            self.0.remove(price);
            None
        } else if order_queue.order_ids.len() > 100
            && (order_queue.count as f32 / order_queue.order_ids.len() as f32) < GARBAGE_THRESHOLD
        {
            Some(BookCleaner(self.0.get_mut(price).unwrap()))
        } else {
            None
        }
    }

    fn get_mut(&mut self, price: &P) -> Option<&mut OrderQueue<K, Q>> {
        self.0.get_mut(price)
    }

    pub fn volume_at(&mut self, price: P) -> Q {
        self.get_mut(&price)
            .map(|order_group| order_group.volume.clone())
            .unwrap_or_else(Q::default)
    }

    pub fn total_volume(&mut self) -> Q {
        self.0.iter().map(|(_, og)| og.volume.clone()).sum()
    }

    fn sorted_prices(&self) -> impl DoubleEndedIterator<Item = &P> {
        self.0.keys()
    }
}

impl<K, S, Q> BookStatus<K, S, Q>
where
    Q: AddAssign + SubAssign + Default + Clone + Sum,
    S: Ord,
{
    fn market_volume<L>(&mut self) -> Q
    where
        S: SidePrice<Price = OrderPrice<L>>,
    {
        self.volume_at(S::new(OrderPrice::market()))
    }

    fn limit_volume<L>(&mut self) -> Q
    where
        S: SidePrice<Price = OrderPrice<L>>,
    {
        self.0
            .iter()
            .filter(|(p, _)| !p.price().is_market())
            .map(|(_, og)| og.volume.clone())
            .sum()
    }
}

/// Orders will give a full list of orders but with partialorder instead of order.
/// This is also to reduce overhead of owning type "Order"
/// Deficit represents the amount of volume that are missing when creating Orders from the orderbook since bookstatus and ordergroup may have discrepancy
/// Orders has all the generics required for orders
/// P: Any type that can represent "Price"
/// Q: Any type that can represent "Quantity"
/// I: Any tyme that can represent "Info"
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct Orders<'a, K, P, Q, I> {
    pub deficit: Q,
    pub orders: Vec<(K, PartialOrder<'a, P, Q, I>)>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub enum PriceTimePriority {
    BothDesc,
    BothAsc,
}

///AskPrice
#[derive(PartialEq, Eq, Clone, Copy, Debug, Serialize, Deserialize, Hash, PartialOrd, Ord)]
struct AskPrice<P>(P);

///AskPrice
#[derive(PartialEq, Eq, Clone, Copy, Debug, Serialize, Deserialize, Hash)]
struct BidPrice<P>(P);

impl<P: Ord> Ord for BidPrice<P> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0).reverse()
    }
}

impl<P: Ord> PartialOrd for BidPrice<P> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

trait SidePrice: Ord + Clone + Debug {
    type Price: Ord;
    fn new(price: Self::Price) -> Self;
    fn price(&self) -> &Self::Price;
}

impl<P: Ord + Clone + Debug> SidePrice for AskPrice<P> {
    type Price = P;
    fn new(price: P) -> Self {
        Self(price)
    }
    fn price(&self) -> &P {
        &self.0
    }
}

impl<P: Ord + Clone + Debug> SidePrice for BidPrice<P> {
    type Price = P;
    fn new(price: P) -> Self {
        Self(price)
    }
    fn price(&self) -> &P {
        &self.0
    }
}

/// OrderMap represents the current status of the orders
/// grouped by ask and bid status
/// OrderMap keeps track of the status for both ask and bid
/// ask_status and bid_status will be a snaphost of the orderbook represented by type "BookStatus"
/// order map will keep trach of each orders
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderBook<K: Eq + Hash, P: Ord, Q, I = ()> {
    ask_status: BookStatus<K, AskPrice<P>, Q>,
    bid_status: BookStatus<K, BidPrice<P>, Q>,
    order_map: OrderMap<K, P, Q, I>,
}

impl<K: Eq + Hash, P: Ord, Q, I> Default for OrderBook<K, P, Q, I> {
    fn default() -> Self {
        Self {
            ask_status: BookStatus(BTreeMap::new()),
            bid_status: BookStatus(BTreeMap::new()),
            order_map: OrderMap::new(),
        }
    }
}

impl<K: Eq + Hash, P: Ord, Q, I> OrderBook<K, P, Q, I> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<K, P, Q, I> OrderBook<K, P, Q, I>
where
    K: Eq + Hash + Clone + Debug,
    P: Ord + Debug + Clone,
    Q: AddAssign + SubAssign + Default + Clone + Sum + Debug + Ord,
    I: Debug,
{
    fn _insert<'a, S: SidePrice<Price = P>>(
        book_status: &mut BookStatus<K, S, Q>,
        order_map: &'a mut OrderMap<K, P, Q, I>,
        id: K,
        side: Side,
        price: P,
        quantity: Q,
        info: I,
    ) -> Result<&'a Order<P, Q, I>>
    where
        Q: Default,
    {
        match order_map.0.entry(id.clone()) {
            hash_map::Entry::Vacant(entry) => {
                book_status.push(id, S::new(price.clone()), quantity.clone());
                Ok(entry.insert(Order::new(side, price, quantity, info)))
            }
            hash_map::Entry::Occupied(_) => Err(Error::KeyAlreadyExists),
        }
    }

    pub fn insert(
        &mut self,
        id: K,
        side: Side,
        price: P,
        quantity: Q,
        info: I,
    ) -> Result<&Order<P, Q, I>>
    where
        Q: Default,
    {
        match side {
            Side::Ask => self.insert_ask(id, price, quantity, info),
            Side::Bid => self.insert_bid(id, price, quantity, info),
        }
    }

    pub fn insert_ask(&mut self, id: K, price: P, quantity: Q, info: I) -> Result<&Order<P, Q, I>>
    where
        Q: Default,
    {
        Self::_insert(
            &mut self.ask_status,
            &mut self.order_map,
            id,
            Side::Ask,
            price,
            quantity,
            info,
        )
    }
    pub fn insert_bid(&mut self, id: K, price: P, quantity: Q, info: I) -> Result<&Order<P, Q, I>>
    where
        Q: Default,
    {
        Self::_insert(
            &mut self.bid_status,
            &mut self.order_map,
            id,
            Side::Bid,
            price,
            quantity,
            info,
        )
    }

    fn sorted_insert_by_key<'a, S: SidePrice<Price = P>>(
        book_status: &mut BookStatus<K, S, Q>,
        order_map: &'a mut OrderMap<K, P, Q, I>,
        id: K,
        side: Side,
        price: P,
        quantity: Q,
        info: I,
    ) -> Result<&'a Order<P, Q, I>>
    where
        K: Ord,
        Q: Default,
    {
        match order_map.0.entry(id.clone()) {
            hash_map::Entry::Vacant(entry) => {
                book_status.sorted_insert_by_key(id, S::new(price.clone()), quantity.clone());
                Ok(entry.insert(Order::new(side, price, quantity, info)))
            }
            hash_map::Entry::Occupied(_) => Err(Error::KeyAlreadyExists),
        }
    }

    pub fn sorted_insert_ask_by_key(
        &mut self,
        id: K,
        price: P,
        quantity: Q,
        info: I,
    ) -> Result<&Order<P, Q, I>>
    where
        K: Ord,
        Q: Default,
    {
        Self::sorted_insert_by_key(
            &mut self.ask_status,
            &mut self.order_map,
            id,
            Side::Ask,
            price,
            quantity,
            info,
        )
    }

    pub fn sorted_insert_bid_by_key(
        &mut self,
        id: K,
        price: P,
        quantity: Q,
        info: I,
    ) -> Result<&Order<P, Q, I>>
    where
        K: Ord,
        Q: Default,
    {
        Self::sorted_insert_by_key(
            &mut self.bid_status,
            &mut self.order_map,
            id,
            Side::Bid,
            price,
            quantity,
            info,
        )
    }

    pub fn remove<R>(&mut self, id: &R) -> Option<Order<P, Q, I>>
    where
        K: Borrow<R>,
        R: Hash + Eq,
    {
        let removed = self.order_map.0.remove(id);
        if let Some(Order {
            side,
            price,
            quantity,
            ..
        }) = &removed
        {
            let bc = match side {
                Side::Ask => self
                    .ask_status
                    .remove(&AskPrice::new(price.clone()), quantity.clone()),
                Side::Bid => self
                    .bid_status
                    .remove(&BidPrice::new(price.clone()), quantity.clone()),
            };
            if let Some(bc) = bc {
                bc.clean(&self.order_map);
            }
        }
        removed
    }

    pub fn reduce<R: ?Sized>(&mut self, id: &R, quantity: Q) -> Option<OrderView<P, Q, I>>
    where
        K: Borrow<R>,
        R: Hash + Eq + Clone,
    {
        let order = self.order_map.0.get(id)?;

        Some(if order.quantity <= quantity {
            let removed = self.remove(id).unwrap();
            OrderView::Full(removed)
        } else {
            let order = self.order_map.0.get_mut(id).unwrap();
            order.quantity -= quantity.clone();
            let partial = PartialOrder {
                side: &order.side,
                price: &order.price,
                quantity: quantity.clone(),
                info: &order.info,
            };
            match order.side {
                Side::Ask => self
                    .ask_status
                    .reduce(&AskPrice::new(order.price.clone()), quantity),
                Side::Bid => self
                    .bid_status
                    .reduce(&BidPrice::new(order.price.clone()), quantity),
            }
            OrderView::Partial(partial)
        })
    }

    pub fn get_ask_order_queue(&mut self, price: P) -> Option<&OrderQueue<K, Q>> {
        self.ask_status.0.get(&AskPrice(price))
    }

    pub fn get_bid_order_queue(&mut self, price: P) -> Option<&OrderQueue<K, Q>> {
        self.bid_status.0.get(&BidPrice(price))
    }

    pub fn ask_volume_at(&mut self, price: P) -> Q {
        self.ask_status.volume_at(AskPrice(price))
    }

    pub fn bid_volume_at(&mut self, price: P) -> Q {
        self.bid_status.volume_at(BidPrice(price))
    }

    pub fn total_ask_volume(&mut self) -> Q {
        self.ask_status.total_volume()
    }

    pub fn total_bid_volume(&mut self) -> Q {
        self.bid_status.total_volume()
    }

    pub fn sorted_ask_prices(&self) -> impl Iterator<Item = &P> {
        self.ask_status.sorted_prices().map(SidePrice::price)
    }

    pub fn sorted_bid_prices(&self) -> impl Iterator<Item = &P> {
        self.bid_status.sorted_prices().map(SidePrice::price)
    }

    fn price_top<S: SidePrice<Price = P>>(
        book_status: &mut BookStatus<K, S, Q>,
    ) -> Option<(&P, &OrderQueue<K, Q>)> {
        book_status
            .top()
            .map(|(p, order_queue)| (p.price(), &*order_queue))
    }

    pub fn ask_price_top(&mut self) -> Option<(&P, &OrderQueue<K, Q>)> {
        Self::price_top(&mut self.ask_status)
    }

    pub fn bid_price_top(&mut self) -> Option<(&P, &OrderQueue<K, Q>)> {
        Self::price_top(&mut self.bid_status)
    }

    fn price_bottom<S: SidePrice<Price = P>>(
        book_status: &mut BookStatus<K, S, Q>,
    ) -> Option<(&P, &OrderQueue<K, Q>)> {
        book_status
            .bottom()
            .map(|(p, order_queue)| (p.price(), &*order_queue))
    }

    pub fn ask_price_bottom(&mut self) -> Option<(&P, &OrderQueue<K, Q>)> {
        Self::price_bottom(&mut self.ask_status)
    }

    pub fn bid_price_bottom(&mut self) -> Option<(&P, &OrderQueue<K, Q>)> {
        Self::price_bottom(&mut self.bid_status)
    }

    fn top<'a, 'b, S: SidePrice<Price = P>>(
        book_status: &'a mut BookStatus<K, S, Q>,
        order_map: &'b OrderMap<K, P, Q, I>,
    ) -> Option<(&'a K, &'b Order<P, Q, I>)> {
        let order_queue = book_status.top()?.1;
        loop {
            let front_key = order_queue
                .order_ids
                .front()
                .expect("inconsistency between self.book_status and self.order_map");
            let front_order = order_map.0.get(front_key);
            if front_order.is_none() {
                order_queue.order_ids.pop_front();
            } else {
                let k = order_queue.order_ids.front().unwrap();
                return front_order.map(|o| (k, o));
            }
        }
    }

    pub fn ask_top(&mut self) -> Option<(&K, &Order<P, Q, I>)> {
        Self::top(&mut self.ask_status, &self.order_map)
    }

    pub fn bid_top(&mut self) -> Option<(&K, &Order<P, Q, I>)> {
        Self::top(&mut self.bid_status, &self.order_map)
    }

    fn bottom<'a, 'b, S: SidePrice<Price = P>>(
        book_status: &'a mut BookStatus<K, S, Q>,
        order_map: &'b OrderMap<K, P, Q, I>,
    ) -> Option<(&'a K, &'b Order<P, Q, I>)> {
        let order_queue = book_status.bottom()?.1;
        loop {
            let back_key = order_queue
                .order_ids
                .back()
                .expect("inconsistency between self.book_status and self.order_map");
            let back_order = order_map.0.get(back_key);
            if back_order.is_none() {
                order_queue.order_ids.pop_back();
            } else {
                let k = order_queue.order_ids.back().unwrap();
                return back_order.map(|o| (k, o));
            }
        }
    }

    pub fn ask_bottom(&mut self) -> Option<(&K, &Order<P, Q, I>)> {
        Self::bottom(&mut self.ask_status, &self.order_map)
    }

    pub fn bid_bottom(&mut self) -> Option<(&K, &Order<P, Q, I>)> {
        Self::bottom(&mut self.bid_status, &self.order_map)
    }

    fn list_orders<'a, S: SidePrice<Price = P>>(
        book_status: &mut BookStatus<K, S, Q>,
        order_map: &'a OrderMap<K, P, Q, I>,
        prices: &[S],
        mut quantity: Option<Q>,
        sort_by: PriceTimePriority,
    ) -> Orders<'a, K, P, Q, I> {
        let mut orders_list = Vec::new();
        for price in prices {
            let Orders { deficit, orders } =
                Self::list_orders_at(order_map, book_status, price, quantity.clone(), sort_by);
            quantity = quantity.and(Some(deficit.clone()));
            orders_list.push(orders);
            if let Some(deficit) = &quantity {
                if deficit == &Q::default() {
                    break;
                }
            }
        }
        Orders {
            deficit: quantity.unwrap_or_default(),
            orders: orders_list.into_iter().flatten().collect(),
        }
    }

    pub fn total_ask_orders(
        &mut self,
        quantity: Option<Q>,
        sort_by: PriceTimePriority,
    ) -> Orders<K, P, Q, I> {
        let mut prices: Vec<_> = self.ask_status.sorted_prices().cloned().collect();
        if let PriceTimePriority::BothAsc = sort_by {
            prices.reverse()
        }
        Self::list_orders(
            &mut self.ask_status,
            &self.order_map,
            &*prices,
            quantity,
            sort_by,
        )
    }

    pub fn total_bid_orders(
        &mut self,
        quantity: Option<Q>,
        sort_by: PriceTimePriority,
    ) -> Orders<K, P, Q, I> {
        let mut prices: Vec<_> = self.bid_status.sorted_prices().cloned().collect();
        if let PriceTimePriority::BothAsc = sort_by {
            prices.reverse()
        }
        Self::list_orders(
            &mut self.bid_status,
            &self.order_map,
            &*prices,
            quantity,
            sort_by,
        )
    }

    pub fn ask_orders_at(
        &mut self,
        price: P,
        quantity: Option<Q>,
        sort_by: PriceTimePriority,
    ) -> Orders<K, P, Q, I> {
        Self::list_orders_at(
            &self.order_map,
            &mut self.ask_status,
            &AskPrice(price),
            quantity,
            sort_by,
        )
    }

    pub fn bid_orders_at(
        &mut self,
        price: P,
        quantity: Option<Q>,
        sort_by: PriceTimePriority,
    ) -> Orders<K, P, Q, I> {
        Self::list_orders_at(
            &self.order_map,
            &mut self.bid_status,
            &BidPrice(price),
            quantity,
            sort_by,
        )
    }

    pub fn get<R>(&mut self, id: &R) -> Option<&Order<P, Q, I>>
    where
        K: Borrow<R>,
        R: Hash + Eq,
    {
        self.order_map.0.get(id)
    }

    pub fn get_info_mut<R>(&mut self, id: &R) -> Option<&mut I>
    where
        K: Borrow<R>,
        R: Hash + Eq,
    {
        self.order_map.0.get_mut(id).map(|o| &mut o.info)
    }

    fn list_orders_at<'a, S: SidePrice<Price = P>>(
        order_map: &'a OrderMap<K, P, Q, I>,
        book_status: &mut BookStatus<K, S, Q>,
        price: &S,
        quantity: Option<Q>,
        sort_by: PriceTimePriority,
    ) -> Orders<'a, K, P, Q, I> {
        type FnPop<K> = fn(&mut VecDeque<K>) -> Option<K>;
        type FnPush<K> = fn(&mut VecDeque<K>, K);

        if let Some(order_group) = book_status.get_mut(price) {
            let mut quantity = quantity.unwrap_or_else(|| order_group.volume.clone());
            let mut orders = Vec::new();
            let order_ids = &mut order_group.order_ids;
            let mut valids = Vec::new();

            let (fn_pop, fn_push): (FnPop<K>, FnPush<K>) =
                if let PriceTimePriority::BothDesc = sort_by {
                    (VecDeque::pop_front, VecDeque::push_front)
                } else {
                    (VecDeque::pop_back, VecDeque::push_back)
                };
            let orders = loop {
                if quantity > Q::default() {
                    if let Some(id) = fn_pop(order_ids) {
                        if let Some(order) = order_map.0.get(&id) {
                            valids.push(id.clone());
                            let partial_quantitiy = order.quantity.clone().min(quantity.clone());
                            quantity -= partial_quantitiy.clone();
                            orders.push((
                                id,
                                PartialOrder {
                                    side: &order.side,
                                    price: &order.price,
                                    quantity: partial_quantitiy,
                                    info: &order.info,
                                },
                            ))
                        }
                    } else {
                        break Orders {
                            deficit: quantity,
                            orders,
                        };
                    };
                } else {
                    break Orders {
                        deficit: Q::default(),
                        orders,
                    };
                }
            };

            for id in valids.into_iter().rev() {
                fn_push(order_ids, id);
            }
            orders
        } else {
            Orders {
                deficit: quantity.unwrap_or_default(),
                orders: Vec::with_capacity(0),
            }
        }
    }

    fn book_status_check<S: SidePrice<Price = P>>(
        book_status: &BookStatus<K, S, Q>,
        order_map: &OrderMap<K, P, Q, I>,
    ) {
        for order_queue in book_status.0.values() {
            let count = order_queue.count();
            let volume = order_queue.volume();
            let orders = order_queue
                .order_ids()
                .iter()
                .filter_map(|k| order_map.0.get(k).map(|o| o.quantity.clone()))
                .collect::<Vec<_>>();
            assert_eq!(count, orders.len());
            assert_eq!(volume, &orders.into_iter().sum::<Q>());
        }
    }
    pub fn integrity_check(&self) {
        Self::book_status_check(&self.ask_status, &self.order_map);
        Self::book_status_check(&self.bid_status, &self.order_map);
    }
}

impl<K, L, Q, I> OrderBook<K, OrderPrice<L>, Q, I>
where
    K: Eq + Hash + Clone + Debug,
    L: Ord + Debug + Clone,
    Q: AddAssign + SubAssign + Default + Clone + Sum + Debug + Ord,
    I: Debug,
{
    pub fn ask_market_volume(&mut self) -> Q {
        self.ask_status.market_volume()
    }

    pub fn bid_market_volume(&mut self) -> Q {
        self.bid_status.market_volume()
    }

    pub fn ask_limit_volume(&mut self) -> Q {
        self.ask_status.limit_volume()
    }

    pub fn bid_limit_volume(&mut self) -> Q {
        self.bid_status.limit_volume()
    }

    fn limit_orders<'a, S: SidePrice<Price = OrderPrice<L>>>(
        book_status: &mut BookStatus<K, S, Q>,
        order_map: &'a OrderMap<K, OrderPrice<L>, Q, I>,
        quantity: Option<Q>,
        sort_by: PriceTimePriority,
    ) -> Orders<'a, K, OrderPrice<L>, Q, I> {
        let mut prices: Vec<_> = book_status
            .sorted_prices()
            .cloned()
            .filter(|p| !p.price().is_market())
            .collect();
        if let PriceTimePriority::BothAsc = sort_by {
            prices.reverse()
        }
        Self::list_orders(book_status, order_map, &prices, quantity, sort_by)
    }

    pub fn ask_limit_orders(
        &mut self,
        quantity: Option<Q>,
        sort_by: PriceTimePriority,
    ) -> Orders<K, OrderPrice<L>, Q, I> {
        Self::limit_orders(&mut self.ask_status, &self.order_map, quantity, sort_by)
    }

    pub fn bid_limit_orders(
        &mut self,
        quantity: Option<Q>,
        sort_by: PriceTimePriority,
    ) -> Orders<K, OrderPrice<L>, Q, I> {
        Self::limit_orders(&mut self.bid_status, &self.order_map, quantity, sort_by)
    }

    fn market_orders<'a, S: SidePrice<Price = OrderPrice<L>>>(
        book_status: &mut BookStatus<K, S, Q>,
        order_map: &'a OrderMap<K, OrderPrice<L>, Q, I>,
        quantity: Option<Q>,
        sort_by: PriceTimePriority,
    ) -> Orders<'a, K, OrderPrice<L>, Q, I> {
        Self::list_orders_at(
            order_map,
            book_status,
            &S::new(OrderPrice::market()),
            quantity,
            sort_by,
        )
    }

    pub fn ask_market_orders(
        &mut self,
        quantity: Option<Q>,
        sort_by: PriceTimePriority,
    ) -> Orders<K, OrderPrice<L>, Q, I> {
        Self::market_orders(&mut self.ask_status, &self.order_map, quantity, sort_by)
    }

    pub fn bid_market_orders(
        &mut self,
        quantity: Option<Q>,
        sort_by: PriceTimePriority,
    ) -> Orders<K, OrderPrice<L>, Q, I> {
        Self::market_orders(&mut self.bid_status, &self.order_map, quantity, sort_by)
    }
}
