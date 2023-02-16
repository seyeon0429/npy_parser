//super class that can represent anything related to price
//does all the arithmatics in u64 which is faster and less prone to overflow
//actual price in dollars = inner / basis
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PriceBasis {
    inner: u64, //values are all converted to u64
    basis: u64, //default as 10000
}

impl Default for PriceBasis {
    fn default() -> Self {
        PriceBasis {
            inner: 0,
            basis: 10000,
        }
    }
}

impl From<u32> for PriceBasis {
    fn from(v: u32) -> Self {
        PriceBasis {
            inner: v as u64,
            basis: 10000,
        }
    }
}

impl From<f64> for PriceBasis {
    fn from(v: f64) -> Self {
        PriceBasis {
            inner: (v * 10000.0).floor() as u64,
            basis: 10000,
        }
    }
}

impl PriceBasis {
    pub fn new(value: u64, basis: u64) -> Self {
        PriceBasis {
            inner: value,
            basis: basis,
        }
    }

    pub fn inner(&self) -> u64 {
        self.inner
    }

    pub fn actual_price(&self) -> f64 {
        self.inner as f64 / self.basis as f64
    }

    // Assume basis has the form of 10^n
    pub fn change_basis(&mut self, new_basis: u64) {
        self.inner = self.inner * new_basis / self.basis;
        self.basis = new_basis;
    }
}
