use itchy::{
    AddOrder, CrossTrade, CrossType, MarketParticipantPosition, NonCrossTrade, ReplaceOrder,
    StockDirectory, TradingState, ImbalanceIndicator, ImbalanceDirection
};
use itertools::Itertools;
// use mmm_core::collections::Side;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::book::{Body,Message,NasdaqOrderBook};
use crate::create_folder;
use crate::stat::StatBuilder;

pub const LEVEL: usize = 5;
use mmm_us::{decode_side, encode_side, price::PriceBasis, Side};
pub const NUM_FIELDS: usize = 9;

fn encode_printable(printable: bool) -> u64 {
    match printable {
        true => 1,
        false => 2,
    }
}

// fn decode_printable(printable: u64) -> bool {
//     match printable {
//         1 => true,
//         2 => false,
//         _ => panic!("unknown value found for 'printable'"),
//     }
// }

fn encode_cross_type(cross_type: CrossType) -> u64 {
    match cross_type {
        CrossType::Opening => 1,
        CrossType::Closing => 2,
        CrossType::IpoOrHalted => 3,
        CrossType::Intraday => 4,
        CrossType::ExtendedTradingClose => 5,
    }
}

fn decode_cross_type(cross_type: u64) -> CrossType {
    match cross_type {
        1 => CrossType::Opening,
        2 => CrossType::Closing,
        3 => CrossType::IpoOrHalted,
        4 => CrossType::Intraday,
        _ => panic!("unknown value found for 'CrossType'"),
    }
}

fn encode_imbalance_direction(direction: ImbalanceDirection) -> u64{
    match direction {
        ImbalanceDirection::Buy => 1,
        ImbalanceDirection::Sell => 2,
        ImbalanceDirection::NoImbalance => 3,
        ImbalanceDirection::InsufficientOrders => 4,
    }
}

fn encode_price_variation_indicator(ind: char) -> u64 {
    match ind {
        'L' => 0,
        '1' => 1,
        '2' => 2,
        '3' => 3,
        '4' => 4,
        '5' => 5,
        '6' => 6,
        '7' => 7,
        '8' => 8,
        '9' => 9,
        'A' => 10,
        'B' => 11,
        'C' => 12,
        ' ' => 13,
        _ => panic!("unknown ind")
    }
}


impl From<&[u64]> for Message {
    fn from(values: &[u64]) -> Self {
        let body = match values[0] {
            0 => Body::AddOrder {
                reference: values[2],
                shares: values[3],
                price: values[4],
                side: decode_side(values[5]),
                mpid_val: values[7],
            },
            1 => Body::DeleteOrder {
                reference: values[2],
            },
            2 => Body::OrderCancelled {
                reference: values[2],
                cancelled: values[3],
            },
            3 => Body::ReplaceOrder {
                new_reference: values[2],
                shares: values[3],
                price: values[4],
                old_reference: values[7],
            },
            4 => Body::OrderExecuted {
                reference: values[2],
                executed: values[3],
            },
            5 => Body::OrderExecutedWithPrice {
                reference: values[2],
                executed: values[3],
            },
            6 => Body::CrossTrade {
                cross_type: decode_cross_type(values[7]),
            },
            7 => Body::NonCrossTrade {},
            _ => panic!("unknown value found for 'MessageType'"),
        };
        Self {
            time: values[1],
            body,
        }
    }
}

#[derive(Debug)]
pub(crate) struct StockContainer {
    pub(crate) name: Option<String>,
    pub(crate) messages: Vec<[u64; NUM_FIELDS]>,
    pub(crate) noii_messages: Vec<[u64; 8]>,
    //pub(crate) bbos: Vec::<[i64; 2]>, 
    pub(crate) book: NasdaqOrderBook,
}

impl StockContainer {
    fn new() -> Self {
        Self {
            name: None,
            messages: Vec::new(),
            noii_messages: Vec::new(),
            //bbos: Vec::new(),
            book: NasdaqOrderBook::new(false),
        }
    }
}

#[derive(Clone)]
pub struct OrderStatus {
    pub price: u64,
    pub side: u64,
    pub shares: u64,
    pub index: usize,
    pub mpid_val: u64,
}

impl OrderStatus {
    pub fn new(
        price: PriceBasis,
        side: itchy::Side,
        quantity: u32,
        last_index: usize,
        mpid_val: u64,
    ) -> Self {
        Self {
            price: price.inner(),
            side: encode_side(side),
            shares: quantity as u64,
            index: last_index,
            mpid_val,
        }
    }
}

pub fn process_file(path: PathBuf, out_dir: PathBuf, meta_only: bool) {
    let out_dir = create_folder(&path, &out_dir);
    let done_file = out_dir.join(".done");
    if !meta_only && done_file.exists() {
        println!(
            "skip `process_file` for {:?}. done file already exists.",
            path
        );
        return;
    }

    let mut containers: HashMap<usize, (StatBuilder, StockContainer)> = HashMap::new();
    let mut status_map = HashMap::new();
    let mut global_mpid_map: HashMap<String, u64> = HashMap::new();
    let mut count = 0;
    for message in itchy::MessageStream::from_gzip(path).unwrap() {
        let message = message.unwrap();
        
        count += 1;
        if count % 25_000_000 == 0{
            println!("Parsing message number {:?}", count);
        }
        let stock_locate = message.stock_locate as usize;

        let (stat_builder, stock_container) = if stock_locate == 0 {
            for (_, (stat_builder, stock_container)) in containers.iter_mut() {
                stat_builder.update(&message, stock_container, &status_map);
            }
            (None, &mut containers
                .entry(stock_locate)
                .or_insert_with(|| (StatBuilder::new(), StockContainer::new()))
                .1)
        } else {
            let (stat_builder, stock_container) = containers
                .entry(stock_locate)
                .or_insert_with(|| (StatBuilder::new(), StockContainer::new()));
            stat_builder.update(&message, stock_container, &status_map);
            (Some(stat_builder), stock_container)
        };

        let StockContainer {
            name,
            messages: stock_messages,
            noii_messages, 
            //bbos, 
            book 
        } = stock_container;

        let encoded = match message.body {
            itchy::Body::AddOrder(AddOrder {
                reference,
                price,
                side,
                shares,
                mpid,
                ..
            }) => {
                let mpid_val = mpid
                    .and_then(|mpid| global_mpid_map.get(mpid.trim_end()).cloned())
                    .unwrap_or_default();
                let status = OrderStatus::new(price, side, shares, stock_messages.len(), mpid_val);
                status_map.insert(reference, status.clone());
                Some([
                    0,
                    message.timestamp,
                    reference,
                    status.shares,
                    status.price,
                    status.side,
                    0,
                    mpid_val,
                    0,
                ])
            }
            itchy::Body::DeleteOrder { reference } => {
                let status = status_map.remove(&reference).unwrap();
                let current_index = stock_messages.len();
                stock_messages[status.index][NUM_FIELDS - 1] = current_index as u64;

                Some([
                    1,
                    message.timestamp,
                    reference,
                    status.shares,
                    status.price,
                    status.side,
                    status.shares,
                    0,
                    0,
                ])
            }
            itchy::Body::OrderCancelled {
                reference,
                cancelled,
                ..
            } => {
                let status = status_map.get_mut(&reference).unwrap();
                let current_index = stock_messages.len();
                stock_messages[status.index][NUM_FIELDS - 1] = current_index as u64;
                status.index = current_index;

                let orig_shares = status.shares;
                let cancelled = cancelled as u64;
                status.shares -= cancelled;
                Some([
                    2,
                    message.timestamp,
                    reference,
                    cancelled,
                    status.price,
                    status.side,
                    orig_shares,
                    0,
                    0,
                ])
            }
            itchy::Body::ReplaceOrder(ReplaceOrder {
                old_reference,
                new_reference,
                price,
                shares,
            }) => {
                let status = status_map.remove(&old_reference).unwrap();
                let current_index = stock_messages.len();
                stock_messages[status.index][NUM_FIELDS - 1] = current_index as u64;

                let orig_shares = status.shares;
                let status = OrderStatus {
                    price: price.inner(),
                    side: status.side,
                    shares: shares as u64,
                    index: stock_messages.len(),
                    mpid_val: status.mpid_val,
                };
                status_map.insert(new_reference, status.clone());
                Some([
                    3,
                    message.timestamp,
                    new_reference,
                    status.shares,
                    status.price,
                    status.side,
                    orig_shares,
                    old_reference,
                    0,
                ])
            }
            itchy::Body::OrderExecuted {
                reference,
                executed,
                ..
            } => {
                let status = status_map.get_mut(&reference).unwrap();
                let current_index = stock_messages.len();
                stock_messages[status.index][NUM_FIELDS - 1] = current_index as u64;
                status.index = current_index;

                let orig_shares = status.shares;
                let executed = executed as u64;
                status.shares -= executed;
                Some([
                    4,
                    message.timestamp,
                    reference,
                    executed,
                    status.price,
                    status.side,
                    orig_shares,
                    0,
                    0,
                ])
            }
            itchy::Body::OrderExecutedWithPrice {
                reference,
                executed,
                price,
                printable,
                ..
            } => {
                let status = status_map.get_mut(&reference).unwrap();
                let current_index = stock_messages.len();
                stock_messages[status.index][NUM_FIELDS - 1] = current_index as u64;
                status.index = current_index;

                let orig_shares = status.shares;
                let executed = executed as u64;
                status.shares -= executed;
                Some([
                    5,
                    message.timestamp,
                    reference,
                    executed,
                    price.inner(),
                    status.side,
                    orig_shares,
                    encode_printable(printable),
                    0,
                ])
            }
            itchy::Body::CrossTrade(CrossTrade {
                cross_type: CrossType::IpoOrHalted | CrossType::Intraday,
                ..
            })
            | itchy::Body::BrokenTrade { .. }
            | itchy::Body::TradingAction {
                trading_state:
                    TradingState::Halted | TradingState::Paused | TradingState::QuotationOnly,
                ..
            } => {
                eprintln!("[ABNORMALLY] {:?}", message.body);
                None
            }
            itchy::Body::CrossTrade(CrossTrade {
                shares,
                cross_price,
                cross_type,
                ..
            }) => Some([
                6,
                message.timestamp,
                0,
                shares as u64,
                cross_price.inner(),
                0,
                0,
                encode_cross_type(cross_type),
                0,
            ]),
            itchy::Body::NonCrossTrade(NonCrossTrade { shares, price, .. }) => Some([
                7,
                message.timestamp,
                0,
                shares as u64,
                price.inner(),
                0,
                0,
                0,
                0,
            ]),
            itchy::Body::StockDirectory(StockDirectory { stock, .. }) => {
                *name = Some(stock.to_string());
                None
            }
            itchy::Body::ParticipantPosition(MarketParticipantPosition { mpid, .. }) => {
                let mpid = mpid.trim_end();
                if !global_mpid_map.contains_key(mpid) {
                    global_mpid_map.insert(mpid.to_string(), global_mpid_map.len() as u64 + 1);
                }
                None
            }
            itchy::Body::Imbalance(ImbalanceIndicator{
                paired_shares,
                imbalance_shares,
                imbalance_direction,
                stock,
                far_price,
                near_price,
                current_ref_price,
                cross_type,
                price_variation_indicator,
            }) => {

                noii_messages.push([
                    paired_shares,
                    imbalance_shares,
                    encode_imbalance_direction(imbalance_direction),
                    far_price.inner(),
                    near_price.inner(),
                    current_ref_price.inner(),
                    encode_cross_type(cross_type),
                    encode_price_variation_indicator(price_variation_indicator),
                ]);
                None
            }
            itchy::Body::LULDAuctionCollar { .. }
            | itchy::Body::RetailPriceImprovementIndicator(_)
            | itchy::Body::IpoQuotingPeriod(_)
            | itchy::Body::MwcbDeclineLevel { .. }
            | itchy::Body::Breach(_)
            | itchy::Body::RegShoRestriction { .. }
            | itchy::Body::SystemEvent { .. }
            | itchy::Body::TradingAction {
                trading_state: TradingState::Trading,
                ..
            } => None,
        };

        if let Some(encoded) = encoded {
            book.handle(&Message::from(encoded.as_slice())).unwrap();
            //let (bo, bb) = book.bbo();
            //match (bo, bb) {
            //    (None, None) => bbos.push([-1, 0]),
            //    (None, Some(b)) => bbos.push([-1, b as i64]),
            //    (Some(a), None) => bbos.push([a as i64, 0]),
            //    (Some(a), Some(b)) => bbos.push([a as i64, b as i64])
            //}
            stock_messages.push(encoded);
            stat_builder.unwrap().update_lob((stock_container.name.as_ref().unwrap().to_string()), book, message.timestamp, LEVEL);
        }

    }

    containers.remove(&0).unwrap(); // stock_locate 0 is used for special purpose.
    let (stat_builders, stock_containers) = containers
        .into_values()
        .unzip::<StatBuilder, StockContainer, Vec<_>, Vec<_>>();

    let market_stats = stat_builders
        .into_iter()
        .filter_map(|builder| {
            let market_stat = builder.build();
            let name = market_stat
                .stock_directory
                .as_ref()
                .map(|name| name.stock.clone().trim_end().to_string());
            let out_path = out_dir.join(format!("{}.json.zst", name.as_ref().unwrap().trim_end()));
            dump(out_path, &serde_json::to_vec(&market_stat).unwrap());
            name.map(|name| (name, market_stat))
        })
        .collect::<HashMap<_, _>>();

    ////we don't use the market_stats file which contains market stats for all securities
    //dump(
    //    out_dir.join("market_stats.json.zst"),
    //    &serde_json::to_vec(&market_stats).unwrap(),
    //);

    dump(
        out_dir.join("mpid_map.json.zst"),
        &serde_json::to_vec(
            &global_mpid_map
                .into_iter()
                .map(|(k, v)| (v, k))
                .collect::<HashMap<_, _>>(),
        )
        .unwrap(),
    );

    if meta_only {
        return;
    }

    stock_containers
        .par_iter()
        .filter(|container| !container.messages.is_empty())
        .map(|container| {
            // save actions
            let out_path = out_dir.join(format!(
                "{}.bin.zst",
                container.name.as_ref().unwrap().trim_end()
            ));
            //let bbo_out_path = out_dir.join(format!(
            //    "{}_bbo.bin.zst", 
            //    container.name.as_ref().unwrap().trim_end()
            //));
            let noii_out_path = out_dir.join(format!(
                "{}_noii.bin.zst",
                container.name.as_ref().unwrap().trim_end()
            ));
            //dump(
            //    bbo_out_path,
            //    &container
            //        .bbos
            //        .iter()
            //        .flatten()
            //        .flat_map(|x| x.to_ne_bytes())
            //        .collect::<Vec<u8>>(),
            //);
            dump(
                noii_out_path,
                &container
                .noii_messages
                .iter()
                .flatten()
                .flat_map(|x| x.to_ne_bytes())
                .collect::<Vec<u8>>(),
            );
            dump(
                out_path,
                &container
                    .messages
                    .iter()
                    .flatten()
                    .flat_map(|x| x.to_ne_bytes())
                    .collect::<Vec<u8>>(),
            )
        })
        .collect::<Vec<_>>();
    File::create(done_file).unwrap();
}

pub(crate) fn dump(out_path: PathBuf, serialized: &[u8]) {
    let mut out_file = std::fs::File::create(out_path).unwrap();
    let compressed = zstd::block::compress(&*serialized, 0).unwrap();
    out_file.write_all(&compressed).unwrap();
}

pub fn load<P: AsRef<Path>>(path: P, num_fields: usize) -> Vec<Vec<u64>> {
    let mut buf = vec![];
    std::fs::File::open(&path)
        .unwrap()
        .read_to_end(&mut buf)
        .unwrap();
    let decompressed = zstd::decode_all(&*buf).unwrap();
    decompressed
        .into_iter()
        .chunks(8)
        .into_iter()
        .map(|chunk| u64::from_ne_bytes(chunk.collect::<Vec<_>>().try_into().unwrap()))
        .chunks(num_fields)
        .into_iter()
        .map(|chunk| chunk.collect::<Vec<_>>())
        .collect::<Vec<_>>()
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_preprocess() {
        std::env::set_var("RUST_BACKTRACE", "full");
        println!("Running mmm-nasdaq preprocess...");
        use std::time::Instant;
        let now = Instant::now();
        let path = PathBuf::from("./test/S100421-v50.txt.gz");
        // process_file(nypath, PathBuf::from("../sample/done"), false);
        process_file(path, PathBuf::from("./test/"), false);
        let elapsed = now.elapsed();
        println!("Elapsed: {:.2?}", elapsed);

        let data = load("./test/S100421-v50/AAPL.bin.zst", NUM_FIELDS);
        //println!("data: {:?}", data);
        let bbo_data = load("./test/S100421-v50/AAPL_bbo.bin.zst", 2);
        println!("bbo_data: {:?}", bbo_data);
        
        //let stats = load("./test/S010322-v50/market_stats.json.zst");
        //println!("stats: {:?}", stats);
    }

    //#[test]
    //fn preprop_test() {
    //    use std::fs::File;
    //    use std::io::{BufRead, BufReader};
    //    use std::iter::zip;
    //    let nypath = PathBuf::from("../test/Test_A_B_C_10");
    //    process_file(nypath, PathBuf::from("../test/done"), false);
    //    let dat = load("../test/done/Test_A_B_C_10/KAIIW.bin.zst");
    //    let f = File::open("../test/TestAnswer").unwrap();
    //    let f = BufReader::new(f);
    //    for (lin, d) in zip(f.lines(), dat) {
    //        let lin = lin.unwrap();
    //        let collected: Vec<&str> = lin.split(",").collect();
    //        // println!("{:?}",collected);
    //        // println!("{:?}",d);
    //        assert_eq!(d[0], collected[0].parse::<u64>().unwrap());
    //        assert_eq!(d[3], collected[3].parse::<u64>().unwrap());
    //        assert_eq!(d[4], collected[4].parse::<u64>().unwrap());

    //        if d[0] != 6 && d[0] != 7 {
    //            assert_eq!(d[5], collected[5].parse::<u64>().unwrap());
    //            assert_eq!(d[6], collected[6].parse::<u64>().unwrap());
    //            assert_eq!(d[8], collected[8].parse::<u64>().unwrap());
    //            assert_eq!(d[2], collected[2].parse::<u64>().unwrap());
    //        }
    //    }
    //}
}
