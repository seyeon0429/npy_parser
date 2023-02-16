use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
pub type Side = mmm_core::collections::Side;

#[derive(Debug, Clone)]
pub enum SecurityType {
    ADR,
    CommonStock, //we only treat this for now
    Debentures,
    Etf,
    Foreign,
    UsDepositoryShares,
    Units,
    IndexLinkedNotes,
    Trust,
    OrdinaryShares,
    PreferedStock,
    Rights,
}

///only supports nasdaq for now
#[derive(Debug, Clone)]
pub enum MarketType {
    Nasdaq,
    Arca,
}
#[derive(Debug, Clone)]
pub enum AskBidType {
    Ask,
    Bid,
}

#[derive(Copy, Clone, Serialize, Deserialize, Hash, PartialEq, Eq, Debug)]
pub struct Time {
    inner: u64, //time in nanoseconds
}

impl Time {
    pub(crate) fn get_nanoseconds(&self) -> u64 {
        self.inner
    }
}

//separate type for volume in case "lot" is used in the future
//although online trading does not care about the concept of "lot"
#[derive(Debug, Clone)]
#[pyclass]
pub struct VolumeBasis {
    inner: u64,
}

impl VolumeBasis {
    pub(crate) fn get_inner(&self) -> u64 {
        self.inner
    }
}
