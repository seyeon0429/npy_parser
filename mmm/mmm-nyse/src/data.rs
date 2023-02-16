use crate::book::{Body, Message, NyseOrderBook};
use crate::stat::{StatBuilder};
use crate::{create_folder, delete_channel_id};
pub use decimal::d128;
use flate2::read::GzDecoder;
use itertools::Itertools;
// use mmm_core::collections::Side;
// use mmm_us::Side;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use taq;
use taq::enums::{
    AddOrder, CrossTrade, CrossType, DeleteOrder, ModifyOrder, NonDisplayedTrade, OrderExecution,
    ReplaceOrder,
};

pub const LEVEL: usize = 5;
use mmm_us::{decode_side, encode_side, price::PriceBasis, Side};
pub const NUM_FIELDS: usize = 9;

// fn encode_side(side: Side) -> u64 {
//     match side {
//         Side::Bid => 2,
//         Side::Ask => 1,
//     }
// }

// fn decode_side(side: u64) -> Side {
//     match side {
//         1 => Side::Ask,
//         2 => Side::Bid,
//         _ => panic!("unknown value {} found for 'Side'", side),
//     }
// }

// E, //Market Center Early Opening Auction
// O, //Market Center Opening Auction
// R, //Market Center Reopening Auction
// C, //Market Center Closing Auction

//todo, nyse crosstype is only same for open and close
//encoding does not matter for now
//but should have some kind of documetationd

fn encode_cross_type(cross_type: CrossType) -> u64 {
    match cross_type {
        CrossType::O => 1,
        CrossType::C => 2,
        //3, 4 is left out for nasdaq
        CrossType::E => 5,
        CrossType::R => 6,
    }
}

fn decode_cross_type(cross_type: u64) -> CrossType {
    match cross_type {
        1 => CrossType::O,
        2 => CrossType::C,
        //3, 4 is left out for nasdaq
        5 => CrossType::E,
        6 => CrossType::R,
        _ => panic!("unknown value found for 'CrossType'"),
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
    pub(crate) bbos: Vec::<[i64; 2]>,
    pub(crate) book: NyseOrderBook,
}

impl StockContainer {
    fn new() -> Self {
        Self {
            name: None,
            messages: Vec::new(),
            bbos: Vec::new(),
            book: NyseOrderBook::new(false),
        }
    }
    fn new_with_name(name: String) -> Self {
        Self {
            name: Some(name),
            messages: Vec::new(),
            bbos: Vec::new(),
            book: NyseOrderBook::new(false),
        }
    }
}

#[derive(Clone, Debug)]
pub struct OrderStatus {
    price: u64, //store price as bp
    side: u64,
    shares: u64,
    index: usize,
    pub mpid_val: u64,
}

impl OrderStatus {
    fn new(price: PriceBasis, side: Side, quantity: u32, last_index: usize, mpid_val: u64) -> Self {
        Self {
            //bug
            price: price.inner(),
            side: encode_side(side),
            shares: quantity as u64,
            index: last_index,
            mpid_val,
        }
    }
}

pub fn process_file(path_list: Vec<PathBuf>, out_dir: PathBuf, meta_only: bool) {
    println!("Processing files...");
    let path = &path_list[0];
    let path = delete_channel_id(path);
    let out_dir = create_folder(&path, &out_dir);
    let done_file = out_dir.join(".done");
    if !meta_only && done_file.exists() {
        println!(
            "skip `process_file` for {:?}. done file already exists.",
            path
        );
        return;
    }

    let mut containers: HashMap<String, (StatBuilder, StockContainer)> = HashMap::new();
    let mut status_map = HashMap::new();
    // there's no participant position msg in nyse taq so add firm id whenever there is some firm id
    let mut global_mpid_map: HashMap<String, u64> = HashMap::new();
    global_mpid_map.insert("".to_string(), 0);

    for path in path_list {
        println!("path: {:?}", path);
        let mut count = 0;
        let mut reader = taq::parser::MessageStream::<GzDecoder<File>>::from_gzip(path).unwrap();
        loop {
            let msg = reader.read_record();
            if msg.is_err() {
                break;
            }
            count += 1;
            if count % 25_000_000 == 0 {
                println!("parsing message number {:?}", count);
            }
            let message = msg.unwrap();
            let stock_locate = message.symbol.as_str();
            let timestamp = message.source_time;
            let (stat_builder, stock_container) = {
                let (stat_builder, stock_container) = containers
                    .entry(stock_locate.to_string())
                    .or_insert_with(|| {
                        (
                            StatBuilder::new(),
                            StockContainer::new_with_name(stock_locate.to_string()),
                        )
                    });
                stat_builder.update(&message, stock_container, &status_map);
                (stat_builder, stock_container)
            };

            let StockContainer {
                name,
                messages: stock_messages,
                bbos,
                book,
            } = stock_container;

            let encoded: Option<Vec<[u64; 9]>> = match message.body {
                taq::parser::Body::AddOrder(AddOrder {
                    price,
                    side,
                    volume,
                    firm_id,
                    order_id,
                    ..
                }) => {
                    let mpid = firm_id.trim_end().to_string();
                    if !global_mpid_map.contains_key(&mpid) {
                        global_mpid_map.insert(mpid.clone(), global_mpid_map.len() as u64 + 1);
                    }
                    let mpid_val = global_mpid_map.get(&mpid).unwrap();

                    let status =
                        OrderStatus::new(price, side, volume, stock_messages.len(), *mpid_val);
                    status_map.insert(order_id, status.clone());
                    Some(vec![[
                        0,
                        timestamp.unwrap(),
                        order_id,
                        status.shares,
                        status.price,
                        status.side,
                        0,
                        *mpid_val,
                        0,
                    ]])
                }
                //modify is only for cases other than cancel or replace
                //but we just treat everything as modify
                taq::parser::Body::ModifyOrder(ModifyOrder {
                    // symbol_seq_number,
                    order_id,
                    price,
                    volume,
                    ..
                }) => {
                    //status map at this point should have some kind of add order
                    //we want to change this order
                    let status = status_map.get_mut(&order_id).unwrap();
                    let current_index = stock_messages.len();
                    stock_messages[status.index][NUM_FIELDS - 1] = current_index as u64;
                    //give that add order index: current index and update the order
                    status.index = current_index;

                    if (price.inner()) != status.price {
                        //first add a delete order for the existing order
                        let mut encoded = vec![[
                            1,
                            timestamp.unwrap(),
                            order_id,
                            status.shares,
                            status.price,
                            status.side,
                            status.shares,
                            0,
                            0,
                        ]];
                        let original_status = status_map.remove(&order_id).unwrap();
                        let status = OrderStatus::new(
                            price,
                            decode_side(original_status.side),
                            volume,
                            stock_messages.len() + 1,
                            original_status.mpid_val,
                        );
                        status_map.insert(order_id, status.clone());
                        encoded.push([
                            0,
                            timestamp.unwrap(),
                            order_id,
                            status.shares,
                            status.price,
                            status.side,
                            0,
                            status.mpid_val,
                            0,
                        ]);
                        Some(encoded)
                    } else if volume as u64 > status.shares {
                        let mut encoded = vec![[
                            1,
                            timestamp.unwrap(),
                            order_id,
                            status.shares,
                            status.price,
                            status.side,
                            status.shares,
                            0,
                            0,
                        ]];
                        //second add a new order while removing the previous add order
                        let original_status = status_map.remove(&order_id).unwrap();
                        let mpid_val = original_status.mpid_val;
                        let side = decode_side(original_status.side);
                        let status = OrderStatus::new(
                            price,
                            side,
                            volume,
                            stock_messages.len() + 1,
                            mpid_val,
                        );
                        status_map.insert(order_id, status.clone());
                        encoded.push([
                            0,
                            timestamp.unwrap(),
                            order_id,
                            status.shares,
                            status.price,
                            status.side,
                            0,
                            mpid_val,
                            0,
                        ]);
                        Some(encoded)
                    } else {
                        let cancelled = status.shares - volume as u64;
                        status.shares = volume as u64;
                        Some(vec![[
                            2,
                            timestamp.unwrap(),
                            order_id,
                            cancelled,
                            status.price,
                            status.side,
                            status.shares,
                            0,
                            0,
                        ]])
                    }
                }
                taq::parser::Body::DeleteOrder(DeleteOrder { order_id, .. }) => {
                    let status = status_map.remove(&order_id).unwrap();
                    let current_index = stock_messages.len();
                    stock_messages[status.index][NUM_FIELDS - 1] = current_index as u64;
                    Some(vec![[
                        1,
                        timestamp.unwrap(),
                        order_id,
                        status.shares,
                        status.price,
                        status.side,
                        status.shares,
                        0,
                        0,
                    ]])
                }
                taq::parser::Body::ReplaceOrder(ReplaceOrder {
                    order_id,
                    new_order_id,
                    price,
                    volume,
                    ..
                }) => {
                    let status = status_map.remove(&order_id).unwrap();
                    let current_index = stock_messages.len();
                    stock_messages[status.index][NUM_FIELDS - 1] = current_index as u64;

                    let orig_shares = status.shares;
                    let status = OrderStatus {
                        price: price.inner(),
                        side: status.side,
                        shares: volume as u64,
                        index: stock_messages.len(),
                        mpid_val: status.mpid_val,
                    };
                    status_map.insert(new_order_id, status.clone());
                    Some(vec![[
                        3,
                        timestamp.unwrap(),
                        new_order_id,
                        status.shares,
                        status.price,
                        status.side,
                        orig_shares,
                        order_id,
                        0,
                    ]])
                }
                //since nyse does not differentiate order execution with price, we need to figure that out here
                taq::parser::Body::OrderExecution(OrderExecution {
                    order_id,
                    price,
                    volume,
                    printable_flag,
                    ..
                }) => {
                    let status = status_map.get_mut(&order_id).unwrap();
                    let original_price = status.price;
                    let current_price = price.inner();
                    let current_index = stock_messages.len();
                    stock_messages[status.index][NUM_FIELDS - 1] = current_index as u64;
                    status.index = current_index;

                    let orig_shares = status.shares;
                    let executed = volume as u64;
                    status.shares -= executed;

                    if current_price != original_price {
                        //order execution with price
                        Some(vec![[
                            5,
                            timestamp.unwrap(),
                            order_id,
                            executed,
                            current_price,
                            status.side,
                            orig_shares,
                            printable_flag as u64,
                            0,
                        ]])
                    } else {
                        //order execution
                        Some(vec![[
                            4,
                            timestamp.unwrap(),
                            order_id,
                            executed,
                            status.price,
                            status.side,
                            orig_shares,
                            printable_flag as u64,
                            0,
                        ]])
                    }
                }
                taq::parser::Body::CrossTrade(CrossTrade {
                    // symbol_seq_number,
                    price,
                    volume,
                    cross_type,
                    ..
                }) => Some(vec![[
                    6,
                    timestamp.unwrap(),
                    0,
                    volume as u64,
                    price.inner(),
                    0,
                    0,
                    encode_cross_type(cross_type),
                    0,
                ]]),
                taq::parser::Body::NonDisplayedTrade(NonDisplayedTrade {
                    // symbol_seq_number,
                    price,
                    volume,
                    ..
                }) => Some(vec![[
                    7,
                    timestamp.unwrap(),
                    0,
                    volume as u64,
                    price.inner(),
                    0,
                    0,
                    0,
                    0,
                ]]),
                //taq::parser::Body::StockSummary(StockSummary {
                //    high_price,
                //    low_price,
                //    opening_price,
                //    closing_price,
                //    total_volume,
                //}) => {
                //    // TODO why don't we have stocksummary messages?
                //    //todo could store this data in the future
                //    None
                //}
                _ => None,
            };

            if let Some(encoded) = encoded {
                for encoded_array in encoded{
                    book.handle(&Message::from(encoded_array.as_slice())).unwrap();
                    let (bo, bb) = book.bbo();
                    match (bo, bb) {
                        (None, None) => bbos.push([-1, 0]),
                        (None, Some(b)) => bbos.push([-1, b as i64]),
                        (Some(a), None) => bbos.push([a as i64, 0]),
                        (Some(a), Some(b)) => bbos.push([a as i64, b as i64])
                    } 
                    stock_messages.push(encoded_array);
                }
                stat_builder.update_lob(book, timestamp.unwrap(), LEVEL);
            }
        }
    }
    // containers.remove(&0).unwrap(); // stock_locate 0 is used for special purpose.
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
                .map(|name| name.symbol.clone().trim_end().to_string());
            let out_path = out_dir.join(format!("{}.json.zst", name.as_ref().unwrap().trim_end()));
            dump(out_path, &serde_json::to_vec(&market_stat).unwrap());
            name.map(|name| (name, market_stat))
        })
        .collect::<HashMap<_, _>>();

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
            let out_path = out_dir.join(format!(
                "{}.bin.zst",
                container.name.as_ref().unwrap().trim_end()
            ));
            let bbo_out_path = out_dir.join(format!(
                "{}_bbo.bin.zst",
                container.name.as_ref().unwrap().trim_end()
            ));
            dump(
                out_path,
                &container
                    .messages
                    .iter()
                    .flatten()
                    .flat_map(|x| x.to_ne_bytes())
                    .collect::<Vec<u8>>(),
            );
            dump(
                bbo_out_path,
                &container
                    .bbos
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
        env::set_var("RUST_BACKTRACE", "1");
        println!("Running mmm-nyse preprocess...");
        use std::time::Instant;
        let now = Instant::now();
        let nypath = vec![
            PathBuf::from("./taq_data/EQY_US_ARCA_IBF_1_20211004.gz"),
            PathBuf::from("./taq_data/EQY_US_ARCA_IBF_2_20211004.gz"),
            PathBuf::from("./taq_data/EQY_US_ARCA_IBF_3_20211004.gz"),
            PathBuf::from("./taq_data/EQY_US_ARCA_IBF_4_20211004.gz"),
            PathBuf::from("./taq_data/EQY_US_ARCA_IBF_5_20211004.gz"),
            PathBuf::from("./taq_data/EQY_US_ARCA_IBF_6_20211004.gz"),
            PathBuf::from("./taq_data/EQY_US_ARCA_IBF_7_20211004.gz"),
            PathBuf::from("./taq_data/EQY_US_ARCA_IBF_8_20211004.gz"),
            PathBuf::from("./taq_data/EQY_US_ARCA_IBF_9_20211004.gz"),
            PathBuf::from("./taq_data/EQY_US_ARCA_IBF_10_20211004.gz"),
            PathBuf::from("./taq_data/EQY_US_ARCA_IBF_11_20211004.gz"),
        ];
        // process_file(nypath, PathBuf::from("../sample/done"), false);
        process_file(nypath, PathBuf::from("./test_data/"), false);
        let elapsed = now.elapsed();
        println!("NYSE preprocess elapsed: {:.2?}", elapsed);
       
        let data = load("./test_data/EQY_US_ARCA_IBF_20211004/AAPL.bin.zst", NUM_FIELDS);
        //println!("data: {:?}", data);
        let bbo_data = load("./test_data/EQY_US_ARCA_IBF_20211004/AAPL_bbo.bin.zst", 2);
        println!("bbo data : {:?}", bbo_data);

        //let stats = load("./test_data/EQY_US_ARCA_IBF_20211004/market_stats.json.zst");
        //println!("stats: {:?}", stats);
    }


    //#[test]
    //fn run_load() {
    //    let loaddat = load("./taq_data/EQY_US_ARCA_IBF_20211004/AAPL.bin.zst");
    //}

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
