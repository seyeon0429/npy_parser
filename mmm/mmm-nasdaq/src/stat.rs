use crate::book::NasdaqOrderBook;
use crate::constants::{P_N, REG_START_TIME_NS, R_N, T_N};
use crate::data::{OrderStatus, LEVEL};
use crate::data::StockContainer;
use crate::interval_loc;
use itchy::{CrossTrade, NonCrossTrade, StockDirectory};
use itertools::Itertools;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug)]
pub(crate) struct StatBuilder {
    partial_stat: MarketStat,
    last_execution_ns: u64,
    last_lob_interval_ind: Option<usize>,
    last_high_interval_ind: Option<usize>,
    last_low_interval_ind: Option<usize>,
    last_price_interval_ind: Option<usize>,
}

impl StatBuilder {
    pub(crate) fn new() -> Self {
        Self {
            partial_stat: MarketStat::new(),
            last_execution_ns: 0,
            last_lob_interval_ind: None,
            last_high_interval_ind: None,
            last_low_interval_ind: None,
            last_price_interval_ind: None,
        }
    }

    fn update_execute_msg(
        &mut self,
        status_map: &HashMap<u64, OrderStatus>,
        executed: u64,
        price: u64,
        reference: &u64,
        timestamp: u64,
        printable: bool,
    ) {
        let partial_stat = &mut self.partial_stat;

        partial_stat.total_volume += executed;
        partial_stat.post_market_volume += executed;

        let bin_ind = interval_loc(timestamp);

        // Sometimes we get multiple execute messages at the same
        // timestamp. If that's the case, we treat it as a single
        // execute message
        if timestamp != self.last_execution_ns {
            if printable {
                partial_stat.interval_execute_msg_count[bin_ind] += 1;    
            }
        }
        // There are two edge cases
        // - messages between 9:30~Cross trade message Q (with field O)
        // - messages between 16:00-Cross trade message Q (with field C)
        // we assume that cross trade message Q (with field O) always occurs after 9:30 and
        // and cross trade message Q (with field C) always occurs after 16:00
        let status = status_map.get(reference).unwrap();
        if (partial_stat.regular_market_start.is_none()) & (bin_ind >= R_N) {
            // these are messages between 9:30~Cross trade message Q (with field O)
            // For these messages put it in the last premarket bin
            if printable{            
                partial_stat.interval_volume[R_N - 1] += executed;
                partial_stat.interval_price_volume[R_N - 1] += executed * price;
                self.update_ohlc(R_N-1, price);            
            }

            //if status.mpid_val > 0 {
            //    if printable {
            //        partial_stat.interval_lp_volume[R_N - 1] += executed;
            //        partial_stat.interval_lp_price_volume[R_N - 1] += executed * price;
            //    }
            //}
        } else if (partial_stat.post_market_start.is_none()) & (bin_ind >= P_N) {
            // these are messages between 16:00~Cross trade message Q (with field C)
            // For these messages put it in the last regular market bin
            if printable{
                partial_stat.interval_volume[P_N - 1] += executed;
                partial_stat.interval_price_volume[P_N - 1] += executed * price;
                self.update_ohlc(P_N-1, price);            
            }   
            //if status.mpid_val > 0 {
            //    if printable {
            //        partial_stat.interval_lp_volume[P_N - 1] += executed;
            //        partial_stat.interval_lp_price_volume[P_N - 1] += executed * price;

            //    }
            //}
        } else {
            // for all other messages, add it to the corresponding bin
            if printable {
                partial_stat.interval_volume[bin_ind] += executed;
                partial_stat.interval_price_volume[bin_ind] += executed * price;
                self.update_ohlc(bin_ind, price);            
           }
            
            // add order without mpid has mpid_val of 0
            //if status.mpid_val > 0 {
            //    if printable{
            //        partial_stat.interval_lp_volume[bin_ind] += executed;
            //        partial_stat.interval_lp_price_volume[bin_ind] += executed * price;
            //    }
            //}
        }
        self.last_execution_ns = timestamp;
    }


    fn update_ohlc(&mut self, bin_ind: usize, price: u64){
        let partial_stat = &mut self.partial_stat;
        
        partial_stat.interval_price[bin_ind] = price;
        if self.last_price_interval_ind.is_some() {
            if bin_ind > self.last_price_interval_ind.unwrap() + 1 {
                for i in (self.last_price_interval_ind.unwrap()+1)..bin_ind{
                    partial_stat.interval_price[i] = partial_stat.interval_price[self.last_price_interval_ind.unwrap()].clone();
                }
            }
        } 
        

        if price > partial_stat.interval_high[bin_ind] {
            partial_stat.interval_high[bin_ind] = price;
        }
        if price < partial_stat.interval_low[bin_ind] {
            partial_stat.interval_low[bin_ind] = price;
        }
        
        if self.last_high_interval_ind.is_some(){
            if bin_ind > self.last_high_interval_ind.unwrap() + 1 { 
                for i in (self.last_high_interval_ind.unwrap()+1)..bin_ind{
                    partial_stat.interval_high[i] = partial_stat.interval_high[self.last_high_interval_ind.unwrap()].clone();
                }
            }
        }
        self.last_high_interval_ind = Some(bin_ind);
        
        if self.last_low_interval_ind.is_some() {
            if bin_ind > self.last_low_interval_ind.unwrap() + 1 {
                for i in (self.last_low_interval_ind.unwrap()+1)..bin_ind {
                    partial_stat.interval_low[i] = partial_stat.interval_low[self.last_low_interval_ind.unwrap()].clone();
                }
            }
        }
        self.last_low_interval_ind = Some(bin_ind);
 
    }


    pub(crate) fn update_lob(
        &mut self,
        symbol: String,
        book: &mut NasdaqOrderBook,
        timestamp: u64,
        level: usize,
    ) {
        let bin_ind = interval_loc(timestamp);
        
        let partial_stat = &mut self.partial_stat;
        
        let summary = book.level_summary(level);
        
        let mut lob = vec![timestamp as i64];
        let mut bo = None;
        let mut bb = None;

        let mut bid_vec = summary["Bid"]
            .clone()
            .into_iter()
            .sorted()
            .rev()
            .map(|x| {
                if x.1 > partial_stat.lob_max_shares {
                    partial_stat.lob_max_shares = x.1;
                }
                vec![-1 * x.1 as i64, x.0 as i64]    // shares, price
            })
            .flatten()
            .collect::<Vec<_>>();
        
        if bid_vec.len() > 0{
            bb = Some(bid_vec[bid_vec.len()-1] as u64);
        }
        
        bid_vec.append(&mut vec![0; 2*level - bid_vec.len()]);
        bid_vec.reverse();
        
        
        lob.append(&mut bid_vec);
        
        let mut ask_vec = summary["Ask"]
            .clone()
            .into_iter()
            .sorted()
            .map(|x| {
                if x.1 > partial_stat.lob_max_shares {
                    partial_stat.lob_max_shares = x.1;
                }    
                vec![x.0 as i64, x.1 as i64]  // price, shares
            })
            .flatten()
            .collect::<Vec<_>>();
        
            if ask_vec.len() > 0{
            bo = Some(ask_vec[0] as u64);
        }
        
        ask_vec.append(&mut vec![0; 2*level-ask_vec.len()]);
        
        lob.append(&mut ask_vec);
        if self.last_lob_interval_ind.is_some(){
            if bin_ind > self.last_lob_interval_ind.unwrap() + 1 { 
                        for i in (self.last_lob_interval_ind.unwrap()+1)..bin_ind{
                    partial_stat.lob_level[i] = partial_stat.lob_level[self.last_lob_interval_ind.unwrap()].clone();
                }
            }
        }

        match (bo, bb) {
            (Some(a), Some(b)) => {
                // we need to check if a > b because during halt trade, it is possible that b > a
                if a > b {
                    if partial_stat.lob_max_spread.is_none(){
                        partial_stat.lob_max_spread = Some(a-b);
                    }
                    else{
                        // we need to check that a > b because during trade halts, b can be greater than a
                        if (a-b) > partial_stat.lob_max_spread.unwrap() {
                            partial_stat.lob_max_spread = Some(a-b);
                        }
                    }

                    if (bin_ind >= R_N) && (bin_ind < P_N){
                       if partial_stat.lob_regmkt_max_spread.is_none(){
                           partial_stat.lob_regmkt_max_spread = Some(a-b);
                       }
                       else if (a-b) > partial_stat.lob_regmkt_max_spread.unwrap() {
                           partial_stat.lob_regmkt_max_spread = Some(a-b);
                       }
                    }
                }
            }, 
            _ => {}
        };
        
        partial_stat.lob_level[bin_ind] = lob
            .clone()
            .try_into()
            .unwrap_or_else(
                |v: Vec<i64>| panic!("Expected a vec of length 21 but it was {}", v.len()));
 
        self.last_lob_interval_ind = Some(bin_ind);

    }

    pub(crate) fn update(
        &mut self,
        message: &itchy::Message,
        container: &StockContainer,
        status_map: &HashMap<u64, OrderStatus>,
    ) {
        let partial_stat = &mut self.partial_stat;
        match &message.body {
            itchy::Body::OrderExecuted {
                executed,
                reference,
                ..
            } => {
                let order_status = status_map.get(reference).unwrap();
                self.update_execute_msg(
                    status_map,
                    *executed as u64,
                    order_status.price,
                    reference, 
                    message.timestamp, 
                    true);
            }
            itchy::Body::OrderExecutedWithPrice {
                executed,
                printable,
                reference,
                price,
                ..
            } => {
                let executed = *executed as u64;
                // we only care about printable order executed with price messages
                self.update_execute_msg(
                    status_map, 
                    executed,
                    price.inner() as u64, 
                    reference, 
                    message.timestamp, 
                    *printable);
            }
            itchy::Body::CrossTrade(CrossTrade {
                shares,
                cross_type,
                cross_price,
                ..
            }) => {
                partial_stat.total_volume += shares;
                match cross_type {
                    itchy::CrossType::Opening => {
                        assert!(message.timestamp >= REG_START_TIME_NS);
                        partial_stat.opening_cross_price = cross_price.inner();
                        partial_stat.opening_cross_volume = *shares;

                        partial_stat.pre_market_volume =
                            std::mem::take(&mut partial_stat.post_market_volume);
                        partial_stat
                            .regular_market_start
                            .replace(container.messages.len());
                    }
                    itchy::CrossType::Closing => {
                        partial_stat.closing_cross_price = cross_price.inner();
                        partial_stat.closing_cross_volume = *shares;

                        partial_stat.regular_market_volume =
                            std::mem::take(&mut partial_stat.post_market_volume);
                        partial_stat
                            .post_market_start
                            .replace(container.messages.len());
                    }
                    _ => {}
                }
            }
            itchy::Body::NonCrossTrade(NonCrossTrade { shares, price, .. }) => {
                let shares = *shares as u64;

                // There are two edge cases
                // - messages between 9:30~Cross trade message Q (with field O)
                // - messages between 16:00-Cross trade message Q (with field C)
                // we assume that cross trade message Q (with field O) always occurs after 9:30 and
                // and cross trade message Q (with field C) always occurs after 16:00
                let bin_ind = interval_loc(message.timestamp);
                //if (partial_stat.regular_market_start.is_none()) & (bin_ind >= R_N) {
                //    // these are messages between 9:30~Cross trade message Q (with field O)
                //    // For these messages put it in the last premarket bin
                //    partial_stat.interval_nondisp_volume[R_N - 1] += shares;
                //    partial_stat.interval_nondisp_price_volume[R_N - 1] += shares * price.inner();
                //} else if (partial_stat.post_market_start.is_none()) & (bin_ind >= P_N) {
                //    // these are messages between 16:00~Cross trade message Q (with field C)
                //    // For these messages put it in the last regular market bin
                //    partial_stat.interval_nondisp_volume[P_N - 1] += shares;
                //    partial_stat.interval_nondisp_price_volume[P_N - 1] += shares * price.inner();
                //} else {
                //    // for all other messages, add it to the corresponding bin
                //    partial_stat.interval_nondisp_volume[bin_ind] += shares;
                //    partial_stat.interval_nondisp_price_volume[bin_ind] += shares * price.inner();
                //}
            }
            // itchy::Body::AddOrder(_) => todo!(),
            //body @ itchy::Body::Breach(_)
            //| body @ itchy::Body::BrokenTrade { .. }
            //| body @ itchy::Body::IpoQuotingPeriod(_)
            //| body @ itchy::Body::LULDAuctionCollar { .. }
            //| body @ itchy::Body::MwcbDeclineLevel { .. }
            //| body @ itchy::Body::RegShoRestriction { .. }
            //| body @ itchy::Body::TradingAction { .. } => partial_stat
            //    .events
            //    .push((container.messages.len(), body.clone())),
            //itchy::Body::SystemEvent { event } => match event {
            //    itchy::EventCode::StartOfMarketHours => {
            //        partial_stat.pre_market_volume =
            //            std::mem::take(&mut partial_stat.post_market_volume);
            //        partial_stat.regular_market_start = container.messages.len();
            //        partial_stat.interval_regular_market_start =
            //            interval_loc(message.timestamp) as usize;
            //    }
            //    itchy::EventCode::EndOfMarketHours => {
            //        partial_stat.regular_market_volume =
            //            std::mem::take(&mut partial_stat.post_market_volume);
            //        partial_stat.post_market_start = container.messages.len();
            //        partial_stat.interval_post_market_start =
            //            interval_loc(message.timestamp) as usize;
            //    }
            //    _ => {}
            //},
            itchy::Body::StockDirectory(sd) => {
                partial_stat.stock_directory.replace(sd.clone());
            }
            // itchy::Body::AddOrder(_)
            // | itchy::Body::DeleteOrder { .. }
            // | itchy::Body::Imbalance(_)
            // | itchy::Body::OrderCancelled { .. }
            // | itchy::Body::ParticipantPosition(_)
            // | itchy::Body::ReplaceOrder(_)
            // | itchy::Body::RetailPriceImprovementIndicator(_) => {}
            _ => {}
        }
    }

 
    pub(crate) fn build(self) -> MarketStat {
        self.partial_stat
    }
}

#[derive(Serialize, Debug)]
pub(crate) struct MarketStat {
    total_volume: u64,
    pre_market_volume: u64,
    regular_market_volume: u64,
    post_market_volume: u64,
    regular_market_start: Option<usize>,
    post_market_start: Option<usize>,
    pub stock_directory: Option<StockDirectory>,
    opening_cross_price: u64,
    opening_cross_volume: u64,
    closing_cross_price: u64,
    closing_cross_volume: u64,
    //events: Vec<(usize, itchy::Body)>,
    
    interval_volume: Vec<u64>,
    interval_price_volume: Vec<u64>,
    interval_high: Vec<u64>,
    interval_low: Vec<u64>,
    interval_price: Vec<u64>,
    //pub interval_lp_volume: Vec<u64>,
    //pub interval_lp_price_volume: Vec<u64>,
    //interval_nondisp_volume: Vec<u64>,
    //interval_nondisp_price_volume: Vec<u64>,
    interval_execute_msg_count: Vec<u64>,

    lob_level: Vec<[i64; LEVEL * 2 * 2 + 1]>,
    lob_max_shares: u64,
    lob_max_spread: Option<u64>,
    lob_regmkt_max_spread: Option<u64>,
}

impl MarketStat {
    pub(crate) fn new() -> Self {
        Self {
            total_volume: Default::default(),
            pre_market_volume: Default::default(),
            regular_market_volume: Default::default(),
            post_market_volume: Default::default(),
            regular_market_start: Default::default(),
            post_market_start: Default::default(),
            stock_directory: Default::default(),
            opening_cross_price: Default::default(),
            opening_cross_volume: Default::default(),
            closing_cross_price: Default::default(),
            closing_cross_volume: Default::default(),
            //events: Default::default(),
             
            interval_volume: vec![0; T_N],
            interval_price_volume: vec![0; T_N],
            interval_high: vec![0; T_N],
            interval_low: vec![u64::MAX; T_N],
            interval_price: vec![0; T_N],
            //interval_lp_volume: vec![0; T_N],
            //interval_lp_price_volume: vec![0; T_N],
            //interval_nondisp_volume: vec![0; T_N],
            //interval_nondisp_price_volume: vec![0; T_N],
            interval_execute_msg_count: vec![0; T_N],
            
            lob_level: vec![[0; LEVEL * 2 * 2 + 1]; T_N],
            lob_max_shares: Default::default(),
            lob_max_spread: None,
            lob_regmkt_max_spread: None,
        }
    }
}
