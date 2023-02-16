use arraystring::{typenum, ArrayString};
use csv::{Reader, StringRecord};
use std::io;
use std::{error::Error, fs::File, io::ErrorKind, io::Read, path::Path};
//only use itchy for basic type naming conventions
use serde::{Deserialize, Serialize};

/// Stack-allocated string of size 5 bytes (re-exported from `arraystring`)
pub type ArrayString5 = ArrayString<typenum::U5>;

// Stack-allocated string of size 11 bytes (re-exported from `arraystring`)
pub type ArrayString11 = ArrayString<typenum::U11>;

use crate::enums::*;
use flate2::read::GzDecoder;
use mmm_us::price::PriceBasis;
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SequenceNumber {
    channel_id: u8,
    sequence_number: u64,
}

impl SequenceNumber {
    fn new(channel_id: u8, sequence_number: u64) -> SequenceNumber {
        SequenceNumber {
            channel_id,
            sequence_number,
        }
    }

    pub fn calculate_unique_reference_number(&self) -> u64 {
        // Basic Cantor Pairing function
        let k1 = self.channel_id as u64;
        let k2 = self.sequence_number;
        ((k1 + k2) * (k1 + k2 + 1)) / 2 + k2
    }
}

/// An TAQ protocol message. Refer to the protocol spec for interpretation.
/// using somewa
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    /// Message Type
    pub msg_type: u8,
    /// String identifying the underlying instrument updated daily(ascii code)
    pub symbol: ArrayString11,
    /// nyse internal tracking number for each channel(starts at 1 every day), this should be in question
    pub sequence_number: SequenceNumber,
    /// Nanoseconds since midnight, upto nanosecond but is only available for certain messagesdf
    pub source_time: Option<u64>,
    /// Body of one of the supported message types
    pub body: Body,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Body {
    //data types come here
    SymbolIndexMapping(SymbolIndexMapping),
    SecurityStatus(SecurityStatus),
    AddOrder(AddOrder),
    ModifyOrder(ModifyOrder),
    ReplaceOrder(ReplaceOrder),
    DeleteOrder(DeleteOrder),
    OrderExecution(OrderExecution),
    NonDisplayedTrade(NonDisplayedTrade),
    TradeCancel(TradeCancel),
    CrossTrade(CrossTrade),
    CrossCorrection(CrossCorrection),
    RetailPriceImprovement(RetailPriceImprovement),
    Imbalance(Imbalance),
    AddOrderRefresh(AddOrderRefresh),
    StockSummary(StockSummary),
    //... add more using itchy-rust
}
//message format for taq is different from totlaview
//A sequence number is an increasing number that uniquely identifies each message per channel. It
//startsthe day at 1 and increments by 1 for each new message per channel
//theres eight channel for tag arca integrated feedd
pub struct MessageStream<F: Read> {
    //this is the csv reader
    reader: Reader<F>,
    channel_id: u8,
}

impl<F: Read> MessageStream<F> {
    pub fn from_file<P>(path: P) -> Result<MessageStream<File>, Box<dyn Error>>
    where
        P: AsRef<Path>,
    {
        let channel_id = parse_channel_id(&path);
        let file = File::open(path)?;
        let reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(file);
        Ok(MessageStream { reader, channel_id })
    }

    pub fn from_gzip<P>(path: P) -> Result<MessageStream<GzDecoder<File>>, Box<dyn Error>>
    where
        P: AsRef<Path>,
    {
        let channel_id = parse_channel_id(&path);
        let file = File::open(path)?;
        let reader = GzDecoder::new(file);
        // let mut s = String::new();
        // reader.read_to_string(&mut s);
        // println!("{:?}",s);
        let reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(reader);

        // let mut record=StringRecord::new();
        // reader.read_record(&mut record);
        // println!("{:?}",record);
        // todo!();
        Ok(MessageStream { reader, channel_id })
    }

    pub fn read_record_raw(&mut self) -> Result<StringRecord, csv::Error> {
        let mut record = StringRecord::new();
        match self.reader.read_record(&mut record) {
            Ok(_) => Ok(record),
            Err(e) => Err(e),
        }
    }

    pub fn read_record(&mut self) -> csv::Result<Message> {
        let mut record = StringRecord::new();
        match self.reader.read_record(&mut record) {
            Ok(not_done) => {
                if !not_done {
                    assert!(record.is_empty());
                    Err(csv::Error::from(io::Error::from(ErrorKind::UnexpectedEof)))
                } else {
                    parse_message(&mut record, self.channel_id)
                }
            }
            Err(e) => {
                println!("{:?}", e);
                Err(e)
            }
        }
    }

    //todo, return something that's not csv result
    pub fn get_all_records(&mut self) -> Vec<csv::Result<Message>> {
        let id = self.channel_id;
        let dd = self.reader.records();
        dd.map(|record| parse_message(&mut record.unwrap(), id))
            .collect()
    }
}

//todo @hyunwoo
fn parse_source_time(str: &str) -> u64 {
    // convert string to nanoseconds after 00:00
    // EX : 02:20:38.352691712 -> 735835269172
    let d: Vec<&str> = str.split(':').collect();
    let h = d[0].parse::<u64>().unwrap() * 3600 * 1000000000;
    let m = d[1].parse::<u64>().unwrap() * 60 * 1000000000;
    let s = (d[2].parse::<f64>().unwrap() * 1000000000.0) as u64;
    h + m + s
}

fn parse_channel_id<P>(s: P) -> u8
where
    P: AsRef<Path>,
{
    let s = s.as_ref().file_name().unwrap().to_str().unwrap();
    //let s = s.as_ref().to_str().unwrap();
    let collected: Vec<&str> = s.split('_').collect();
    // println!("{:?}", collected);
    let id = collected.get(4).expect("Wrong file format?");
    id.parse::<u8>().unwrap()
}

//change csv record into Message
pub fn parse_message(record: &mut StringRecord, channel_id: u8) -> csv::Result<Message> {
    let msg_type: u32 = record.get(0).unwrap().parse::<u32>().unwrap(); //handle all unwrap after finishing basic logic

    // Aside from symbol index mapping, all the other messages have the same
    // field index for sequence number, symbol and symbol sequence number
    let sequence_number = record.get(1).unwrap().parse::<u64>().unwrap();
    let symbol = record.get(3).unwrap();

    let msg: Message = match msg_type {
        //SymbolIndexMapping
        3 => {
            let round_lots_accepted = match record.get(11).unwrap() {
                "Y" => true,
                "N" => false,
                _ => panic!(),
            };
            Message {
                msg_type: msg_type as u8,
                sequence_number: SequenceNumber::new(channel_id, sequence_number),
                symbol: ArrayString11::from(record.get(2).unwrap()),
                source_time: None, // no timestamp for symbol index mapping
                body: Body::SymbolIndexMapping(SymbolIndexMapping {
                    market_category: parse_market_category(record.get(3).unwrap().parse().unwrap()),
                    system_id: record.get(4).unwrap().parse::<u64>().unwrap(),
                    exchange_code: parse_exchange_code(record.get(5).unwrap()),
                    issue_classification: parse_issue_classifcation(record.get(6).unwrap()),
                    round_lot_size: record.get(7).unwrap().parse::<u32>().unwrap(),
                    prev_close_price: PriceBasis::from(
                        record.get(8).unwrap().parse::<f64>().unwrap(),
                    ),
                    prev_close_volume: record.get(9).unwrap().parse::<u64>().unwrap(),
                    price_resolution: record.get(10).unwrap().parse::<u64>().unwrap(),
                    round_lots_accepted,
                    mpv: (record.get(12).unwrap().parse::<f64>().unwrap() * 10000.0) as u32,
                    unit_of_trade: record.get(13).unwrap().parse::<u64>().unwrap(),
                }),
            }
        }
        //Security Status
        34 => Message {
            msg_type: msg_type as u8,
            symbol: ArrayString11::from(symbol),
            sequence_number: SequenceNumber::new(channel_id, sequence_number),
            source_time: Some(parse_source_time(record.get(2).unwrap())),
            body: Body::SecurityStatus(SecurityStatus {
                security_status: encode_security_status_type(record.get(5).unwrap()),
                halt_condition: encode_halt_condition(record.get(6).unwrap()),
                price_1: PriceBasis::from(
                    record.get(7).unwrap().parse::<f64>().unwrap_or_default(),
                ),
                price_2: PriceBasis::from(
                    record.get(8).unwrap().parse::<f64>().unwrap_or_default(),
                ),
                ssr_triggering_exchange_id: parse_ssr_triggering_exchange_id(
                    record.get(9).unwrap(),
                ),
                ssr_triggering_volume: record.get(10).unwrap().parse::<u64>().unwrap_or_default(),
                time: record.get(11).unwrap().parse::<u64>().unwrap_or_default(),
                ssr_state: parse_ssr_state(record.get(12).unwrap()),
                market_state: parse_market_state(record.get(13).unwrap()),
            }),
        },
        //ADD ORDER
        100 => Message {
            msg_type: msg_type as u8,
            symbol: ArrayString11::from(symbol),
            sequence_number: SequenceNumber::new(channel_id, sequence_number),
            source_time: Some(parse_source_time(record.get(2).unwrap())),
            body: Body::AddOrder(AddOrder {
                order_id: record.get(5).unwrap().parse::<u64>().unwrap(),
                price: PriceBasis::from(record.get(6).unwrap().parse::<f64>().unwrap()),
                volume: record.get(7).unwrap().parse::<u32>().unwrap(),
                side: parse_side(record.get(8).unwrap()).unwrap(),
                firm_id: ArrayString5::from(record.get(9).unwrap().trim_end()),
            }),
        },
        // ModifyOrder
        101 => Message {
            msg_type: msg_type as u8,
            symbol: ArrayString11::from(symbol),
            sequence_number: SequenceNumber::new(channel_id, sequence_number),
            source_time: Some(parse_source_time(record.get(2).unwrap())),
            body: Body::ModifyOrder(ModifyOrder {
                order_id: record.get(5).unwrap().parse::<u64>().unwrap(),
                price: PriceBasis::from(record.get(6).unwrap().parse::<f64>().unwrap()),
                volume: record.get(7).unwrap().parse::<u32>().unwrap(),
                position_change: parse_position_change(record.get(8).unwrap()),
            }),
        },
        // Replace Order
        104 => Message {
            msg_type: msg_type as u8,
            symbol: ArrayString11::from(symbol),
            sequence_number: SequenceNumber::new(channel_id, sequence_number),
            source_time: Some(parse_source_time(record.get(2).unwrap())),
            body: Body::ReplaceOrder(ReplaceOrder {
                order_id: record.get(5).unwrap().parse::<u64>().unwrap(),
                new_order_id: record.get(6).unwrap().parse::<u64>().unwrap(),
                price: PriceBasis::from(record.get(7).unwrap().parse::<f64>().unwrap()),
                volume: record.get(8).unwrap().parse::<u32>().unwrap(),
            }),
        },
        // DeleteOrder
        102 => Message {
            msg_type: msg_type as u8,
            symbol: ArrayString11::from(symbol),
            sequence_number: SequenceNumber::new(channel_id, sequence_number),
            source_time: Some(parse_source_time(record.get(2).unwrap())),
            body: Body::DeleteOrder(DeleteOrder {
                order_id: record.get(5).unwrap().parse::<u64>().unwrap(),
            }),
        },
        // Order Execution Message
        103 => {
            Message {
                msg_type: msg_type as u8,
                symbol: ArrayString11::from(symbol),
                sequence_number: SequenceNumber::new(channel_id, sequence_number),
                source_time: Some(parse_source_time(record.get(2).unwrap())),
                body: Body::OrderExecution(OrderExecution {
                    order_id: record.get(5).unwrap().parse::<u64>().unwrap(),
                    trade_id: record.get(6).unwrap().parse::<u32>().unwrap(),
                    price: PriceBasis::from(record.get(7).unwrap().parse::<f64>().unwrap()),
                    volume: record.get(8).unwrap().parse::<u32>().unwrap(),
                    printable_flag: record.get(9).unwrap().parse::<u8>().unwrap(),
                    //trade_condition_1: encode_trade_condition_1(record.get(11).unwrap()),
                    //trade_condition_2: encode_trade_condition_2(record.get(12).unwrap()),
                    //trade_condition_3: encode_trade_condition_3(record.get(13).unwrap()),
                    //trade_condition_4: encode_trade_condition_4(record.get(14).unwrap()),
                }),
            }
        }
        // NonDisplayedTrade Message
        110 => Message {
            msg_type: msg_type as u8,
            symbol: ArrayString11::from(symbol),
            sequence_number: SequenceNumber::new(channel_id, sequence_number),
            source_time: Some(parse_source_time(record.get(2).unwrap())),
            body: Body::NonDisplayedTrade(NonDisplayedTrade {
                trade_id: record.get(5).unwrap().parse::<u32>().unwrap(),
                price: PriceBasis::from(record.get(6).unwrap().parse::<f64>().unwrap()),
                volume: record.get(7).unwrap().parse::<u32>().unwrap(),
            }),
        },
        // TradeCancelMessage
        112 | 221 => Message {
            msg_type: msg_type as u8,
            symbol: ArrayString11::from(symbol),
            sequence_number: SequenceNumber::new(channel_id, sequence_number),
            source_time: Some(parse_source_time(record.get(2).unwrap())),
            body: Body::TradeCancel(TradeCancel {
                trade_id: record.get(5).unwrap().parse::<u32>().unwrap(),
            }),
        },
        // RetailPriceImprovement
        114 => Message {
            msg_type: msg_type as u8,
            symbol: ArrayString11::from(symbol),
            sequence_number: SequenceNumber::new(channel_id, sequence_number),
            source_time: Some(parse_source_time(record.get(2).unwrap())),
            body: Body::RetailPriceImprovement(RetailPriceImprovement {
                rpi_indicator: parse_rti_indicator(record.get(5).unwrap()),
            }),
        },
        // Cross Trade Message
        111 => Message {
            msg_type: msg_type as u8,
            symbol: ArrayString11::from(symbol),
            sequence_number: SequenceNumber::new(channel_id, sequence_number),
            source_time: Some(parse_source_time(record.get(2).unwrap())),
            body: Body::CrossTrade(CrossTrade {
                cross_id: record.get(5).unwrap().parse::<u32>().unwrap(),
                price: PriceBasis::from(record.get(6).unwrap().parse::<f64>().unwrap()),
                volume: record.get(7).unwrap().parse::<u32>().unwrap(),
                cross_type: parse_cross_type(record.get(8).unwrap()),
            }),
        },
        // Cross Correction Message
        113 => Message {
            msg_type: msg_type as u8,
            symbol: ArrayString11::from(symbol),
            sequence_number: SequenceNumber::new(channel_id, sequence_number),
            source_time: Some(parse_source_time(record.get(2).unwrap())),
            body: Body::CrossCorrection(CrossCorrection {
                cross_id: record.get(5).unwrap().parse::<u32>().unwrap(),
                volume: record.get(6).unwrap().parse::<u32>().unwrap(),
            }),
        },
        // Imbalance Message
        105 => {
            let mut auction_time = record.get(9).unwrap().parse::<u64>().unwrap();
            auction_time = (auction_time / 100 * 3600 + auction_time % 100 * 60) * 1000000000;
            // ------------------------------------------------------
            Message {
                msg_type: msg_type as u8,
                symbol: ArrayString11::from(symbol),
                sequence_number: SequenceNumber::new(channel_id, sequence_number),
                source_time: Some(parse_source_time(record.get(2).unwrap())),
                body: Body::Imbalance(Imbalance {
                    reference_price: record.get(5).unwrap().parse::<f64>().unwrap(),
                    paired_qty: record.get(6).unwrap().parse::<u32>().unwrap(),
                    total_imbalance_qty: record.get(7).unwrap().parse::<u32>().unwrap(),
                    market_imbalance_qty: record.get(8).unwrap().parse::<u32>().unwrap(),
                    auction_time,
                    auction_type: parse_auction_type(record.get(10).unwrap()),
                    imbalance_side: parse_side(record.get(11).unwrap()),
                    continous_book_clearing_price: record.get(12).unwrap().parse::<f64>().unwrap(),
                    auction_interest_clearing_price: record
                        .get(13)
                        .unwrap()
                        .parse::<f64>()
                        .unwrap(),
                    ssr_filling_price: record.get(14).unwrap().parse::<f64>().unwrap(),
                    indicative_match_price: record.get(15).unwrap().parse::<f64>().unwrap(),
                    upper_collar: record.get(16).unwrap().parse::<f64>().unwrap(),
                    lower_collar: record.get(17).unwrap().parse::<f64>().unwrap(),
                    auction_status: num::FromPrimitive::from_u8(
                        record.get(18).unwrap().parse::<u8>().unwrap(),
                    )
                    .unwrap(),
                    freeze_status: num::FromPrimitive::from_u8(
                        record.get(19).unwrap().parse::<u8>().unwrap(),
                    )
                    .unwrap(),
                    unpaired_qty: record.get(21).unwrap().parse::<u32>().unwrap(),
                    unpaired_side: parse_side(record.get(22).unwrap()),
                }),
            }
        }
        // AddOrderRefresh
        106 => Message {
            msg_type: msg_type as u8,
            symbol: ArrayString11::from(symbol),
            sequence_number: SequenceNumber::new(channel_id, sequence_number),
            source_time: Some(parse_source_time(record.get(2).unwrap())),
            body: Body::AddOrderRefresh(AddOrderRefresh {
                order_id: record.get(5).unwrap().parse::<u16>().unwrap(),
                price: PriceBasis::from(record.get(6).unwrap().parse::<f64>().unwrap()),
                volume: record.get(7).unwrap().parse::<u32>().unwrap(),
                side: parse_side(record.get(8).unwrap()).unwrap(),
                firm_id: ArrayString5::from_str_truncate(record.get(9).unwrap()),
            }),
        },
        // StockSummaryMessage
        223 => Message {
            msg_type: msg_type as u8,
            symbol: ArrayString11::from(symbol),
            sequence_number: SequenceNumber::new(channel_id, sequence_number),
            source_time: Some(parse_source_time(record.get(2).unwrap())),
            body: Body::StockSummary(StockSummary {
                high_price: record.get(4).unwrap().parse::<f64>().unwrap(),
                low_price: record.get(5).unwrap().parse::<f64>().unwrap(),
                opening_price: record.get(6).unwrap().parse::<f64>().unwrap(),
                closing_price: record.get(7).unwrap().parse::<f64>().unwrap(),
                total_volume: record.get(8).unwrap().parse::<u64>().unwrap(),
            }),
        },
        _ => panic!(
            "Not a valid message Type of integrated message {:?}",
            msg_type
        ),
    };

    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_parse() {
        let path = "./taq_data/EQY_US_ARCA_IBF_10_20211028.gz";
        // let id = parse_channel_id(path);
        let mut d = MessageStream::<GzDecoder<File>>::from_gzip(path).unwrap();
        loop {
            let a = d.read_record();
            if a.is_err() {
                break;
            }
            //println!("{:?}",a.unwrap());
        }
    }

    #[test]
    fn start_parse_gzip() {
        let path = "../sample/EQY_US_ARCA_IBF_11_20211004.gz";
        // let id = parse_channel_id(path);
        let mut d = MessageStream::<GzDecoder<File>>::from_gzip(path).unwrap();
        loop {
            let a = d.read_record();
            if a.is_err() {
                // panic!("Hello");
                break;
            }

            // println!("{:?}",a.unwrap());
        }
    }

    #[test]
    fn start_parse_all() {
        use std::time::Instant;
        let now = Instant::now();
        let path = "../sample/EQY_US_ARCA_IBF_9_20210310";
        // let id = parse_channel_id(path);
        let mut d = MessageStream::<File>::from_file(path).unwrap();
        let _allrecord = d.get_all_records();
        // for v in allrecord {
        //     println! {"{:?}",v.unwrap()};
        // }
        let elpased = now.elapsed();
        println!("{:.2?}", elpased);
    }
    #[test]
    #[ignore]
    fn enocde_sequence_number() {
        let channel_id: u8 = 11;
        let sequence_num: u64 = 1162362;
        let s = SequenceNumber::new(channel_id, sequence_num);
        let r = s.calculate_unique_reference_number().to_string();
        //let s2 = parse_sequence_number(r.as_str());
        //assert_eq!(s.channel_id, s2.channel_id);
        //assert_eq!(s.sequence_number, s2.sequence_number);
    }
}
