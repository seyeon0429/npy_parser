use core::panic;

// use mmm_core::collections::Side;
// use itchy::{ArrayString8, Price4, Side};
use arraystring::{typenum, ArrayString};
pub use decimal::d128;
use serde::{Deserialize, Serialize};

use mmm_us::{price::PriceBasis, Side};

/// Stack-allocated string of size 5 bytes (re-exported from `arraystring`)
pub type ArrayString5 = ArrayString<typenum::U5>;

/// Stack-allocated string of size 11 bytes (re-exported from `arraystring`)
pub type ArrayString11 = ArrayString<typenum::U11>;

/// Opaque type representing a price to four decimal places
// #[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
// pub struct Price4(pub u32);

// impl Price4 {
//     pub fn to_f32(self) -> f32 {
//         self.0 as f32 / 1e4
//     }
// }

// impl Into<d128> for Price4 {
//     fn into(self) -> d128 {
//         d128::from(self.0) / d128::from(10_000)
//     }
// }

// impl From<u32> for Price4 {
//     fn from(v: u32) -> Price4 {
//         Price4(v)
//     }
// }

// impl From<f64> for Price4 {
//     fn from(v: f64) -> Price4 {
//         //some value v can be of of format 10.4, 10.123, ..etc
//         //so we multiply by 1000 then drop the rest
//         //todo this can be exposed to overflow, so double check is necessary
//         let value = v * 10_000.0;
//         Price4(value.floor() as u32)
//     }
// }

pub fn parse_side(s: &str) -> Option<Side> {
    match s {
        "B" => Some(Side::Bid),
        "S" => Some(Side::Ask),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum PositionChange {
    Lost,
    Kept,
}

pub fn parse_position_change(s: &str) -> PositionChange {
    match s {
        "0" => PositionChange::Kept,
        "1" => PositionChange::Lost,
        _ => panic!("Wrong position change! {:?}", s),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RetailPriceImprovementExist {
    A, //bid side
    B, //offer side
    C, //both
}

pub fn parse_rti_indicator(s: &str) -> Option<RetailPriceImprovementExist> {
    match s {
        "A" => Some(RetailPriceImprovementExist::A),
        "B" => Some(RetailPriceImprovementExist::B),
        "C" => Some(RetailPriceImprovementExist::C),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CrossType {
    E, //Market Center Early Opening Auction
    O, //Market Center Opening Auction
    R, //Market Center Reopening Auction
    C, //Market Center Closing Auction
}

pub fn parse_cross_type(s: &str) -> CrossType {
    match s {
        "E" => CrossType::E,
        "O" => CrossType::O,
        "5" => CrossType::R,
        "6" => CrossType::C,
        _ => panic!("Unresolvable CrossType"),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TradeCondition1 {
    RegularSale,
    Cash,
    NextDayTrade,
}

pub fn encode_trade_condition_1(v: &str) -> TradeCondition1 {
    match v {
        "@" => TradeCondition1::RegularSale,
        "C" => TradeCondition1::Cash,
        "N" => TradeCondition1::NextDayTrade,
        _ => panic!("Wrong trade condition 1!"),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TradeCondition2 {
    NotAvailable,
    IntermarketSweepOrder,
    MarketCenterOpeningTrade,
    MarketCenterReopeningTrade,
    MarketCenterClosingTrade,
    QualifiedContingentTrade,
}

pub fn encode_trade_condition_2(v: &str) -> TradeCondition2 {
    match v {
        " " => TradeCondition2::NotAvailable,
        "F" => TradeCondition2::IntermarketSweepOrder,
        "O" => TradeCondition2::MarketCenterOpeningTrade,
        "5" => TradeCondition2::MarketCenterReopeningTrade,
        "6" => TradeCondition2::MarketCenterClosingTrade,
        "7" => TradeCondition2::QualifiedContingentTrade,
        _ => panic!("Wrong trade condition 2!"),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TradeCondition3 {
    NotAvailable,
    ExtendedHoursTrade,
    ExtendedHoursSoldOutOfSequence,
    Sold,
}

pub fn encode_trade_condition_3(v: &str) -> TradeCondition3 {
    match v {
        " " => TradeCondition3::NotAvailable,
        "T" => TradeCondition3::ExtendedHoursTrade,
        "U" => TradeCondition3::ExtendedHoursSoldOutOfSequence,
        "Z" => TradeCondition3::Sold,
        _ => panic!("Wrong trade condition 3!"),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TradeCondition4 {
    NotAvailable,
    OddLotTrade,
    ContingentTrade,
}

pub fn encode_trade_condition_4(v: &str) -> TradeCondition4 {
    match v {
        " " => TradeCondition4::NotAvailable,
        "I" => TradeCondition4::OddLotTrade,
        "V" => TradeCondition4::ContingentTrade,
        _ => panic!("Wrong trade condition 4!"),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AuctionType {
    O, //Early Opening Auction (non-NYSE only)
    M, //Core Opening Auction
    H, //Reopening Auction (Halt resume)
    R, //Regulatory Imbalance (NYSE primaries only)
    C, //Closing Auction
    P, //Extreme Closing Order Imbalance - (NYSEprimaries only)
}

pub fn parse_auction_type(s: &str) -> AuctionType {
    match s {
        "O" => AuctionType::O,
        "M" => AuctionType::M,
        "H" => AuctionType::H,
        "R" => AuctionType::R,
        "C" => AuctionType::C,
        "P" => AuctionType::P,
        _ => panic!("Wrong Acution Type"),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, FromPrimitive)]
pub enum AuctionStatus {
    A = 0,
    B = 1,
    C = 2,
    D = 3,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, FromPrimitive)]
pub enum FreezeStatus {
    A = 0,
    B = 1,
}

//list of structs that we need to parse data in mmm-nyse
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AddOrder {
    // pub symbol_seq_number: u32,
    pub order_id: u64,     // Should be u32 or bigger
    pub price: PriceBasis, //need to check if this should be price8 or not
    pub volume: u32,
    pub side: Side,
    pub firm_id: ArrayString5, //The market participant’s firm ID, or space-filled if firm ID was not specified
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModifyOrder {
    // pub symbol_seq_number: u16,
    pub order_id: u64,
    pub price: PriceBasis, //need to check if this should be price8 or not
    pub volume: u32,
    pub position_change: PositionChange,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeleteOrder {
    // pub symbol_seq_number: u16,
    pub order_id: u64,
}

//this does not lose it's position since this is shown when order is partially cancelled
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplaceOrder {
    // pub symbol_seq_number: u16,
    pub order_id: u64,
    pub new_order_id: u64,
    pub price: PriceBasis, //need to check if this should be price8 or not
    pub volume: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderExecution {
    // pub symbol_seq_number: u16,
    pub order_id: u64,
    pub trade_id: u32,
    pub price: PriceBasis, //need to check if this should be price8 or not
    pub volume: u32,
    pub printable_flag: u8,
    // TODO trade condition fields are not present in our data...
    //pub trade_condition_1: TradeCondition1,
    //pub trade_condition_2: TradeCondition2,
    //pub trade_condition_3: TradeCondition3,
    //pub trade_condition_4: TradeCondition4,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NonDisplayedTrade {
    pub trade_id: u32,
    pub price: PriceBasis, //need to check if this should be price8 or not
    pub volume: u32,
    // TODO trade condition fields are not present in our data...
    //pub trade_condition_1: TradeCondition1,
    //pub trade_condition_2: TradeCondition2,
    //pub trade_condition_3: TradeCondition3,
    //pub trade_condition_4: TradeCondition4,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeCancel {
    pub trade_id: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetailPriceImprovement {
    pub rpi_indicator: Option<RetailPriceImprovementExist>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CrossTrade {
    pub cross_id: u32,
    pub price: PriceBasis, //need to check if this should be price8 or not
    pub volume: u32,
    pub cross_type: CrossType,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CrossCorrection {
    pub cross_id: u32,
    pub volume: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Imbalance {
    pub reference_price: f64,
    pub paired_qty: u32,
    pub total_imbalance_qty: u32,
    pub market_imbalance_qty: u32,
    pub auction_time: u64, //In nanosec
    pub auction_type: AuctionType,
    pub imbalance_side: Option<Side>,
    pub continous_book_clearing_price: f64,
    pub auction_interest_clearing_price: f64,
    pub ssr_filling_price: f64,
    pub indicative_match_price: f64,
    pub upper_collar: f64,
    pub lower_collar: f64,
    pub auction_status: AuctionStatus,
    pub freeze_status: FreezeStatus,
    pub unpaired_qty: u32,
    pub unpaired_side: Option<Side>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AddOrderRefresh {
    pub order_id: u16,
    pub price: PriceBasis, //need to check if this should be price8 or not
    pub volume: u32,
    pub side: Side,
    pub firm_id: ArrayString5, //The market participant’s firm ID, or space-filled if firm ID was not specified
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarketCategory {
    Nyse,
    NyseArcaEquities,
    NyseArcaOptions,
    NyseBonds,
    GlobalOTC,
    NyseAmexOptions,
    NyseAmericanEquities,
    NyseNationalEquities,
    NyseChicagoEquities,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SecurityStatusType {
    TradingHalt,
    Resume,
    ShortSaleRestrictionActivatedDay1,
    ShortSaleRestrictionContinuedDay2,
    ShortSaleRestrictionDeactivated,
    PreOpening,
    BeginAcceptingOrders,
    EarlySession,
    CoreSession,
    LateSession,
    Closed,
    PriceIndication,
    PreOpeningPriceIndication,
}

pub fn encode_security_status_type(v: &str) -> SecurityStatusType {
    match v {
        "4" => SecurityStatusType::TradingHalt,
        "5" => SecurityStatusType::Resume,
        "A" => SecurityStatusType::ShortSaleRestrictionActivatedDay1,
        "C" => SecurityStatusType::ShortSaleRestrictionContinuedDay2,
        "D" => SecurityStatusType::ShortSaleRestrictionDeactivated,
        "P" => SecurityStatusType::PreOpening,
        "B" => SecurityStatusType::BeginAcceptingOrders,
        "E" => SecurityStatusType::EarlySession,
        "O" => SecurityStatusType::CoreSession,
        "L" => SecurityStatusType::LateSession,
        "X" => SecurityStatusType::Closed,
        "I" => SecurityStatusType::PriceIndication,
        "G" => SecurityStatusType::PreOpeningPriceIndication,
        _ => panic!("Wrong security status type"),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HaltConditionType {
    SecurityNotDelayedHalted,
    NewsReleased,
    OrderImbalance,
    NewsPending,
    LULDPause,
    EquipmentChangeover,
    NoOpenResume,
    AdditionalInformationRequested,
    RegulatoryConcern,
    MergerEffective,
    ETFComponentPricesNotAvailable,
    CorporateAction,
    NewSecurityOffering,
    IntradayIndicativeValueNotAvailable,
    MarketWideCircuitBreakerHaltLevel1,
    MarketWideCircuitBreakerHaltLevel2,
    MarketWideCircuitBreakerHaltLevel3,
}

pub fn encode_halt_condition(v: &str) -> HaltConditionType {
    match v {
        "~" => HaltConditionType::SecurityNotDelayedHalted,
        "D" => HaltConditionType::NewsReleased,
        "I" => HaltConditionType::OrderImbalance,
        "P" => HaltConditionType::NewsPending,
        "M" => HaltConditionType::LULDPause,
        "X" => HaltConditionType::EquipmentChangeover,
        "Z" => HaltConditionType::NoOpenResume,
        "A" => HaltConditionType::AdditionalInformationRequested,
        "C" => HaltConditionType::RegulatoryConcern,
        "E" => HaltConditionType::MergerEffective,
        "F" => HaltConditionType::ETFComponentPricesNotAvailable,
        "N" => HaltConditionType::CorporateAction,
        "O" => HaltConditionType::NewSecurityOffering,
        "V" => HaltConditionType::IntradayIndicativeValueNotAvailable,
        "1" => HaltConditionType::MarketWideCircuitBreakerHaltLevel1,
        "2" => HaltConditionType::MarketWideCircuitBreakerHaltLevel2,
        "3" => HaltConditionType::MarketWideCircuitBreakerHaltLevel3,
        _ => panic!("Wrong halt condition!"),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarketState {
    PreOpening,
    EarlySession,
    CoreSession,
    LateSession,
    Closed,
}

pub fn parse_market_state(v: &str) -> Option<MarketState> {
    match v {
        "P" => Some(MarketState::PreOpening),
        "E" => Some(MarketState::EarlySession),
        "O" => Some(MarketState::CoreSession),
        "L" => Some(MarketState::LateSession),
        "X" => Some(MarketState::Closed),
        _ => None,
    }
}

pub fn parse_market_category(v: u16) -> MarketCategory {
    match v {
        1 => MarketCategory::Nyse,
        3 => MarketCategory::NyseArcaEquities,
        4 => MarketCategory::NyseArcaOptions,
        5 => MarketCategory::NyseBonds,
        6 => MarketCategory::GlobalOTC,
        8 => MarketCategory::NyseAmexOptions,
        9 => MarketCategory::NyseAmericanEquities,
        10 => MarketCategory::NyseNationalEquities,
        11 => MarketCategory::NyseChicagoEquities,
        _ => panic!(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExchangeCode {
    None,
    Nyse,
    NyseArca,
    NyseNational,
    NASDAQ,
    NyseAmerican,
    NasdaqOMXBX,
    Nsx,
    Finra,
    Ise,
    Edga,
    Edgx,
    Ltse,
    NyseChicago,
    Cts,
    NasdaqOmx,
    IEX,
    CBSX,
    NasdaqOmxPsx,
    BATSY,
    BATS,
    OTCBBGlobalOTC,
    OtherOTCGlobalOTC,
    GlobalOTC,
}

pub fn parse_ssr_triggering_exchange_id(v: &str) -> Option<ExchangeCode> {
    match v {
        "N" => Some(ExchangeCode::Nyse),
        "P" => Some(ExchangeCode::NyseArca),
        "C" => Some(ExchangeCode::NyseNational),
        "Q" => Some(ExchangeCode::NASDAQ),
        "A" => Some(ExchangeCode::NyseAmerican),
        "B" => Some(ExchangeCode::NasdaqOMXBX),
        "D" => Some(ExchangeCode::Finra),
        "I" => Some(ExchangeCode::Ise),
        "J" => Some(ExchangeCode::Edga),
        "K" => Some(ExchangeCode::Edgx),
        "L" => Some(ExchangeCode::Ltse),
        "M" => Some(ExchangeCode::NyseChicago),
        "S" => Some(ExchangeCode::Cts),
        "T" => Some(ExchangeCode::NasdaqOmx),
        "V" => Some(ExchangeCode::IEX),
        "W" => Some(ExchangeCode::CBSX),
        "Y" => Some(ExchangeCode::BATSY),
        "Z" => Some(ExchangeCode::BATS),
        _ => None,
    }
}

pub fn parse_exchange_code(v: &str) -> Option<ExchangeCode> {
    match v {
        "N" => Some(ExchangeCode::Nyse),
        "P" => Some(ExchangeCode::NyseArca),
        "Q" => Some(ExchangeCode::NASDAQ),
        "A" => Some(ExchangeCode::NyseAmerican),
        "U" => Some(ExchangeCode::OTCBBGlobalOTC),
        "V" => Some(ExchangeCode::OtherOTCGlobalOTC),
        "Z" => Some(ExchangeCode::BATS),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IssueClassification {
    ADR,
    CommonStock,
    Debuntures,
    ETF,
    Foreign,
    USDepositaryShares,
    Units,
    IndexLinkedNotes,
    Trust,
    OrdinaryShares,
    PreferredStock,
    Rights,
    BeneficiaryInterest,
    Test,
    ClosedEndFund,
    Warrant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SSRState {
    NoShortSaleInEffect,
    ShortSaleRestrictionInEffect,
}

pub fn parse_ssr_state(v: &str) -> SSRState {
    match v {
        "~" => SSRState::NoShortSaleInEffect,
        "E" => SSRState::ShortSaleRestrictionInEffect,
        _ => panic!("unknown ssr state!"),
    }
}

pub fn parse_issue_classifcation(v: &str) -> IssueClassification {
    match v {
        "A" => IssueClassification::ADR,
        "C" => IssueClassification::CommonStock,
        "D" => IssueClassification::Debuntures,
        "E" => IssueClassification::ETF,
        "F" => IssueClassification::Foreign,
        "H" => IssueClassification::USDepositaryShares,
        "I" => IssueClassification::Units,
        "L" => IssueClassification::IndexLinkedNotes,
        "M" => IssueClassification::Trust,
        "O" => IssueClassification::OrdinaryShares,
        "P" => IssueClassification::PreferredStock,
        "R" => IssueClassification::Rights,
        "S" => IssueClassification::BeneficiaryInterest,
        "T" => IssueClassification::Test,
        "U" => IssueClassification::ClosedEndFund,
        "W" => IssueClassification::Warrant,
        _ => {
            println!("{:?}", v);
            panic!("fail to encode issue classification");
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolIndexMapping {
    pub market_category: MarketCategory,
    pub system_id: u64,
    pub exchange_code: Option<ExchangeCode>,
    pub issue_classification: IssueClassification,
    pub prev_close_price: PriceBasis,
    pub prev_close_volume: u64,
    pub price_resolution: u64,
    pub round_lot_size: u32,
    pub round_lots_accepted: bool,
    pub mpv: u32, //The minimum increment for a trade price, in 100ths of acent
    pub unit_of_trade: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SecurityStatus {
    pub security_status: SecurityStatusType,
    pub halt_condition: HaltConditionType,
    pub price_1: PriceBasis,
    pub price_2: PriceBasis,
    pub ssr_triggering_exchange_id: Option<ExchangeCode>,
    pub ssr_triggering_volume: u64,
    pub time: u64,
    pub ssr_state: SSRState,
    pub market_state: Option<MarketState>,
}

//list of structs that we need to parse data in mmm-nyse
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StockSummary {
    // symbol_seq_number: u16,
    pub high_price: f64,
    pub low_price: f64,
    pub opening_price: f64,
    pub closing_price: f64,
    pub total_volume: u64,
}
