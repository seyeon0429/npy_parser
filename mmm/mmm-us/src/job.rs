use crate::price::PriceBasis;
use crate::util::{AskBidType, MarketType, SecurityType, Time, VolumeBasis};
use mmm_core::collections::account::Account;
use pyo3::prelude::*;

#[derive(Debug, Clone)]
pub struct MarketMeta {
    market_open: Time,
    market_close: Time,
}

/// Data specific to such stock class
#[derive(Debug, Clone)]
pub struct SecurityMeta {
    ticker: String,
    rount_lot_size: u64,
    classification: SecurityType,
    market_category: MarketType,
    prev_closing_price: PriceBasis, //previous market(mon to fri) day closing price, this will affect
}

/// Job is defined as the entire information about handling one task
/// This by itself should be enough to find out the current status of the job
/// generics represent: K, L, Q =
#[derive(Debug, Clone)]
pub struct Job {
    market_meta: MarketMeta,
    job_meta: JobMeta,
    // account: Account,
}

/// meta data about a single job
#[derive(Debug, Clone)]
pub struct JobMeta {
    market_type: MarketType,
    security: SecurityMeta, //ticker
    initial_price: Option<f64>,
    ask_bid_type: AskBidType,
    basis: VolumeBasis,
    market_start: Time,
    market_end: Time,
    trading_start: Time,
    trading_end: Time,
}

impl JobMeta {
    pub(crate) fn modify_target_basis(&mut self, basis: VolumeBasis) {
        self.basis = basis
    }

    #[allow(missing_docs)]
    pub fn market_type(&self) -> &MarketType {
        &self.market_type
    }

    #[allow(missing_docs)]
    pub fn security(&self) -> &SecurityMeta {
        &self.security
    }

    #[allow(missing_docs)]
    pub fn initial_price(&self) -> Option<f64> {
        self.initial_price
    }

    #[allow(missing_docs)]
    pub fn ask_bid_type(&self) -> &AskBidType {
        &self.ask_bid_type
    }

    #[allow(missing_docs)]
    pub fn target_basis(&self) -> &VolumeBasis {
        &self.basis
    }

    #[allow(missing_docs)]
    pub fn market_start(&self) -> Time {
        self.market_start
    }

    #[allow(missing_docs)]
    pub fn market_end(&self) -> Time {
        self.market_end
    }
    #[allow(missing_docs)]
    pub fn trading_start(&self) -> Time {
        self.trading_start
    }
    #[allow(missing_docs)]
    pub fn trading_end(&self) -> Time {
        self.trading_end
    }
}

#[pyclass]
#[derive(Clone, Debug)]
/// Job 복원에 필요한 정보들입니다
pub struct PyJobMeta {
    #[allow(dead_code)] // used in python
    market_type: String,
    issue_code: String,
    initial_price: Option<f64>,
    ask_bid_type: String,
    target_volume: u64,
    market_start_nanosecond: u64,
    market_end_nanosecond: u64,
    trading_start_nanosecond: u64,
    trading_end_nanosecond: u64,
}

#[pymethods]
impl PyJobMeta {
    /// PyJob Initialization
    #[new]
    fn new(
        market_type: String,
        issue_code: String,
        initial_price: Option<f64>,
        ask_bid_type: String,
        target_basis: VolumeBasis,
        market_start_nanosecond: u64,
        market_end_nanosecond: u64,
        trading_start_nanosecond: u64,
        trading_end_nanosecond: u64,
    ) -> PyJobMeta {
        PyJobMeta {
            market_type,
            issue_code,
            initial_price,
            ask_bid_type,
            target_volume: target_basis.get_inner(),
            market_start_nanosecond,
            market_end_nanosecond,
            trading_start_nanosecond,
            trading_end_nanosecond,
        }
    }
}

// impl PyJobMeta {
//     /// Dependent 한 조건들을 업데이트하는 메소드입니다.
//     pub fn update(&mut self, market_start: Time, market_end: Time, initial_price: Option<f64>) {
//         self.market_start_nanosecond = market_start.get_nanoseconds();
//         self.market_end_nanosecond = market_end.get_nanoseconds();
//         self.initial_price = initial_price;
//     }

//     /// PyJob 객체를 내부에서 사용하기 위한 Job 객체로 변환합니다.
//     pub fn into_job(self, total_volume: VolumeBasis, initial_price: Option<f64>) -> JobMeta {
//         let basis = match self.target_volume {
//             TargetVolume::Basis(basis) => basis,
//             TargetVolume::Ratio(ratio) => (total_volume as f64 * ratio) as VolumeBasis,
//             TargetVolume::Amount(amount) => {
//                 (amount as f64 / initial_price.expect("failed to get initial price."))
//                     as VolumeBasis
//             }
//         };
//         Job::from_basis(
//             {
//                 if self.market_type == "kospi" {
//                     KOSPI
//                 } else {
//                     KOSDAQ
//                 }
//             },
//             self.issue_code().to_string(),
//             self.initial_price,
//             {
//                 if self.ask_bid_type == "ask" {
//                     Ask
//                 } else {
//                     Bid
//                 }
//             },
//             basis,
//             Time::from_millisec(self.market_start_millisecond),
//             Time::from_millisec(self.market_end_millisecond),
//             Time::from_millisec(self.trading_start_millisecond),
//             Time::from_millisec(self.trading_end_millisecond),
//             self.tolerance,
//         )
//     }
// }
