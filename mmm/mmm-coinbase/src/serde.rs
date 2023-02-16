use mmm_core::{collections::book::OrderPrice, serde::empty_string_is_none};
use serde::{Deserialize, Serialize};

use crate::websocket::{Decrement, OrderProfile, PartType};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FeeRates {
    #[serde(default, with = "empty_string_is_none")]
    pub maker: Option<String>,
    #[serde(default, with = "empty_string_is_none")]
    pub taker: Option<String>,
}

impl FeeRates {
    fn maker(fee_rate: String) -> Self {
        Self {
            maker: Some(fee_rate),
            taker: None,
        }
    }
    fn taker(fee_rate: String) -> Self {
        Self {
            maker: None,
            taker: Some(fee_rate),
        }
    }
}

serde_with::serde_conv!(
    pub(crate) OptOrderProfileConv,
    Option<OrderProfile>,
    |order_profile: &Option<OrderProfile>| {
        order_profile.as_ref().map(
            |OrderProfile {
                 part_type,
                 fee_rate,
            }: &OrderProfile| match part_type {
                PartType::Maker => FeeRates::maker(fee_rate.to_string()),
                PartType::Taker => FeeRates::taker(fee_rate.to_string()),
            },
        )
    },
    |fee_rates: FeeRates| -> Result<_, std::convert::Infallible> {
        Ok(match (fee_rates.maker, fee_rates.taker) {
            (None, None) => None,
            (Some(fee_rate), None) => Some(OrderProfile {
                part_type: PartType::Maker,
                fee_rate,
            }),
            (None, Some(fee_rate)) => Some(OrderProfile {
                part_type: PartType::Maker,
                fee_rate,
            }),
            (Some(_), Some(_)) => unreachable!(),
        })
    }
);

// #[derive(Serialize, Deserialize, Debug, Clone)]
// pub struct PriceSize {
//     #[serde(default, with = "empty_string_is_none")]
//     pub price: Option<String>,
//     #[serde(default, with = "empty_string_is_none")]
//     pub remaining_size: Option<String>,
// }

// serde_with::serde_conv!(
//     pub(crate) OptLimitQuoteConv,
//     Option<LimitQuote>,
//     |limit_quote: &Option<LimitQuote>| {
//         limit_quote.as_ref().map(
//             |LimitQuote {
//                  price,
//                  remaining_size,
//             }: &LimitQuote| PriceSize {
//                 price: Some(price.to_string()),
//                 remaining_size: Some(remaining_size.to_string())
//             },
//         )
//     },
//     |price_size: PriceSize| -> Result<_, std::convert::Infallible> {
//         Ok(match (price_size.price, price_size.remaining_size) {
//             (None, None) => None,
//             (None, Some(remaining_size)) if remaining_size != "0" => None,
//             (Some(price), Some(remaining_size)) => Some(LimitQuote{price, remaining_size}),
//             _ => {
//                 unreachable!()},
//         })
//     }
// );

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
pub struct RawDecrement {
    #[serde(default, with = "empty_string_is_none")]
    price: Option<String>,
    #[serde(default, with = "empty_string_is_none")]
    old_size: Option<String>,
    #[serde(default, with = "empty_string_is_none")]
    new_size: Option<String>,
    #[serde(default, with = "empty_string_is_none")]
    old_funds: Option<String>,
    #[serde(default, with = "empty_string_is_none")]
    new_funds: Option<String>,
}

serde_with::serde_conv!(
    pub(crate) DecrementConv,
    Decrement,
    |decrement: &Decrement| {
        match decrement {
            Decrement::Limit {
                price,
                old_size,
                new_size,
            } => RawDecrement {
                price: Some(price.to_string()),
                old_size: Some(old_size.to_string()),
                new_size: Some(new_size.to_string()),
                old_funds: None,
                new_funds: None,
            },
            Decrement::MarketFunds {
                old_funds,
                new_funds,
            } => RawDecrement {
                price: None,
                old_size: None,
                new_size: None,
                old_funds: Some(old_funds.to_string()),
                new_funds: Some(new_funds.to_string()),
            },
            Decrement::MarketSize { old_size, new_size } => RawDecrement {
                price: None,
                old_size: Some(old_size.to_string()),
                new_size: Some(new_size.to_string()),
                old_funds: None,
                new_funds: None,
            },
        }
    },
    |raw_decrement: RawDecrement| -> Result<_, std::convert::Infallible> {
        let RawDecrement {
            price,
            old_size,
            new_size,
            old_funds,
            new_funds,
        } = raw_decrement;
        Ok(match (price, old_size, new_size, old_funds, new_funds) {
            (Some(price), Some(old_size), Some(new_size), None, None) => Decrement::Limit {
                price,
                old_size,
                new_size,
            },
            (None, Some(old_size), Some(new_size), None, None) => {
                Decrement::MarketSize { old_size, new_size }
            }
            (None, None, None, Some(old_funds), Some(new_funds)) => Decrement::MarketFunds {
                old_funds,
                new_funds,
            },
            _ => unreachable!(),
        })
    }
);

serde_with::serde_conv!(
    pub(crate) OrderPriceConv,
    OrderPrice<String>,
    |order_price: &OrderPrice<String>| {
        match order_price {
            OrderPrice::Market => None,
            OrderPrice::Limit(price) => Some(price.to_string()),
        }
    },
    |price: String| -> Result<_, std::convert::Infallible> {
        Ok(OrderPrice::Limit(price))
    }
);
