use crate::book::NyseOrderBook;
use crate::constants::{P_N, REG_END_TIME_NS, REG_START_TIME_NS, R_N, T_N};
use crate::data::OrderStatus;
use crate::data::StockContainer;
use crate::interval_loc;
use serde::{Serialize};
use std::collections::HashMap;
use taq::enums::CrossTrade;
use taq::enums::NonDisplayedTrade;
use taq::enums::OrderExecution;
use taq::parser::Message;

#[derive(Debug)]
pub(crate) struct StatBuilder {
    pub partial_stat: MarketStat,
    last_execution_ns: u64,
    last_lob_interval_ind: usize,
}

impl StatBuilder {
    pub(crate) fn new() -> Self {
        Self {
            partial_stat: MarketStat::new(),
            last_execution_ns: 0,
            last_lob_interval_ind: 0,
        }
    }
    
    fn update_execute_msg(
        &mut self,
        status_map: &HashMap<u64, OrderStatus>,
        executed: u64,
        price: u64,
        reference: &u64,
        timestamp: u64,
    ) {
        let partial_stat = &mut self.partial_stat;

        partial_stat.total_volume += executed;
        partial_stat.post_market_volume += executed;

        let bin_ind = interval_loc(timestamp);

        // Sometimes we get multiple execute messages at the same
        // timestamp. If that's the case, we treat it as a single
        // execute message
        if timestamp != self.last_execution_ns {
            partial_stat.interval_execute_msg_count[bin_ind] += 1;
        }
        
        // unlike itch data, taq data sometimes do not contain opening cross messages...
        let status = status_map.get(reference).unwrap();
        // for all other messages, add it to the corresponding bin
        partial_stat.interval_volume[bin_ind] += executed;
        partial_stat.interval_price_volume[bin_ind] += executed * price;

        // add order without mpid has mpid_val of 0
        if status.mpid_val > 0 {
            partial_stat.interval_lp_volume[bin_ind] += executed;
            partial_stat.interval_lp_price_volume[bin_ind] += executed * price;
        }
        self.last_execution_ns = timestamp;
    }

    pub(crate) fn update_lob(
        &mut self,
        book: &mut NyseOrderBook,
        timestamp: u64,
        level: usize,
    ) {
        let bin_ind = interval_loc(timestamp);
        let partial_stat = &mut self.partial_stat;
        let summary = book.level_summary(level);
        assert!(summary["Bid"].len() <= level);
        assert!(summary["Ask"].len() <= level);
        let mut lob = vec![timestamp];
        let mut bid_vec = summary["Bid"]
            .clone()
            .into_iter()
            .map(|x| vec![x.0, x.1])
            // .collect::<Vec<_>>()
            // .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert!(bid_vec.len() <= level*2);
        bid_vec.append(&mut vec![0; 2*level - bid_vec.len()]);
        assert!(bid_vec.len() == level*2);
        bid_vec.reverse();
        assert!(bid_vec.len() == level*2);
        lob.append(&mut bid_vec);
        
        let mut ask_vec = summary["Ask"]
            .clone()
            .into_iter()
            .map(|x| vec![x.0, x.1])
            // .collect::<Vec<_>>()
            // .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert!(ask_vec.len() <= level*2);
        ask_vec.append(&mut vec![0; 2*level-ask_vec.len()]);
        assert!(ask_vec.len() == level*2);
        lob.append(&mut ask_vec);

        if (self.last_lob_interval_ind > 0) & (bin_ind-1 > self.last_lob_interval_ind) {
            for i in (self.last_lob_interval_ind+1)..bin_ind-1{
                partial_stat.lob_level_5[i] = partial_stat.lob_level_5[self.last_lob_interval_ind].clone();
            }
        }
        assert!(lob.len() == (level*2)*2 + 1);

        partial_stat.lob_level_5[bin_ind] = lob
            .clone()
            .try_into()
            .unwrap_or_else(
                |v: Vec<u64>| panic!("Expected a vec of length 21 but it was {}", v.len()));

        self.last_lob_interval_ind = bin_ind;
    }



    pub(crate) fn update(
        &mut self,
        message: &taq::parser::Message,
        container: &StockContainer,
        status_map: &HashMap<u64, OrderStatus>, //this keeps track of the current add_orders
    ) {
        // print!("Hello");
        let partial_stat = &mut self.partial_stat;
        match &message.body {
            taq::parser::Body::OrderExecution(OrderExecution {
                volume, 
                order_id, 
                printable_flag,
                price, 
                ..
            }) => {
                if *printable_flag > 0
                {
                    self.update_execute_msg(
                            status_map,
                            *volume as u64,
                            price.inner(), 
                            order_id,
                            message.source_time.unwrap());
                }
            }
            taq::parser::Body::CrossTrade(CrossTrade {
                // symbol_seq_number,
                price, //need to check if this should be price8 or not
                volume,
                cross_type,
                ..
            }) => {
                partial_stat.total_volume += *volume as u64;
                match cross_type {
                    taq::enums::CrossType::O => {}
                    taq::enums::CrossType::C => {}
                    taq::enums::CrossType::E => {}
                    taq::enums::CrossType::R => {}
                    _ => panic!("This should never happen!")
                }
            }
            taq::parser::Body::NonDisplayedTrade(NonDisplayedTrade {
                // symbol_seq_number,
                price,
                volume,
                ..
            }) => {
                let shares = *volume as u64;
                let bin_ind = interval_loc(message.source_time.unwrap());
                partial_stat.interval_nondisp_volume[bin_ind] += shares;
                partial_stat.interval_nondisp_price_volume[bin_ind] += shares * price.inner();
            }
            // taq::parser::Body::AddOrder(_) => todo!(),
            body @ taq::parser::Body::CrossCorrection(_)
            //| body @ taq::parser::Body::RetailPriceImprovement(RetailPriceImprovement { .. })
            //| body @ taq::parser::Body::TradeCancel(TradeCancel { .. })
            //| body @ taq::parser::Body::Imbalance { .. } 
            => partial_stat
                .events
                .push((container.messages.len(), body.clone())),
            // taq::parser::Body::DeleteOrder { reference } => todo!(),
            // taq::parser::Body::Imbalance(_) => todo!(),
            // taq::parser::Body::OrderCancelled { reference, cancelled } => todo!(),
            // taq::parser::Body::ParticipantPosition(_) => todo!(),
            // taq::parser::Body::ReplaceOrder(_) => todo!(),
            //todo for now, nyse does not provide systemevent messsages
            // },
            taq::parser::Body::SymbolIndexMapping(_) => {
                partial_stat.stock_directory.replace(message.clone());
            }
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
    pub stock_directory: Option<Message>,
    opening_cross_price: u64,
    opening_cross_volume: u64,
    closing_cross_price: u64,
    closing_cross_volume: u64,
    events: Vec<(usize, taq::parser::Body)>,

    interval_volume: Vec<u64>,
    interval_price_volume: Vec<u64>,
    pub interval_lp_volume: Vec<u64>,
    pub interval_lp_price_volume: Vec<u64>,
    interval_nondisp_volume: Vec<u64>,
    interval_nondisp_price_volume: Vec<u64>,
    interval_execute_msg_count: Vec<u64>,

    lob_level_5: Vec<[u64; 21]>,
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
            events: Default::default(),
            
            interval_volume: vec![0; T_N],
            interval_price_volume: vec![0; T_N],
            interval_lp_volume: vec![0; T_N],
            interval_lp_price_volume: vec![0; T_N],
            interval_nondisp_volume: vec![0; T_N],
            interval_nondisp_price_volume: vec![0; T_N],
            interval_execute_msg_count: vec![0; T_N],
            
            lob_level_5: vec![[0; 21]; T_N],
        }
    }
}


