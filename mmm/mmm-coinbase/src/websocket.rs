// TODO create every variant of MarketQuote in sandbox
use crate::serde::{DecrementConv, OptOrderProfileConv, OrderPriceConv};
use async_trait::async_trait;
use futures::{future::ready, TryStream, TryStreamExt};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use mmm_core::{
    collections::book::OrderPrice,
    serde::{deny_empty_string, empty_string_is_none},
};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::convert::TryFrom;
use thiserror::Error;
use tokio_tungstenite::tungstenite::protocol::{
    frame::coding::CloseCode, CloseFrame, WebSocketConfig,
};
use url::Url;
use uuid::Uuid;

type TMessage = tokio_tungstenite::tungstenite::Message;
type TError = tokio_tungstenite::tungstenite::Error;
type UtcDateTime = chrono::DateTime<chrono::Utc>;

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone, Copy)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone, Copy)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    Filled,
    Canceled,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone, Copy)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum StopType {
    Entry,
    Exit,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
pub struct UserProfile {
    #[serde(deserialize_with = "deny_empty_string")]
    pub user_id: String,
    #[serde(deserialize_with = "deny_empty_string")]
    pub profile_id: String,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone, Copy)]
#[serde(deny_unknown_fields)]
pub enum PartType {
    Maker,
    Taker,
}

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub struct OrderProfile {
    pub part_type: PartType,
    pub fee_rate: String,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
pub struct LimitQuote {
    #[serde(deserialize_with = "deny_empty_string")]
    pub price: String,
    #[serde(deserialize_with = "deny_empty_string")]
    #[serde(alias = "size")]
    pub remaining_size: String,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[serde(untagged)]
pub enum MarketQuote {
    // NOTE: A market sell order can also specify the funds.
    // If funds is specified, it will limit the sell to the amount of funds specified.
    // You can use funds with sell orders to limit the amount of quote currency funds received.
    Both {
        #[serde(deserialize_with = "deny_empty_string")]
        size: String,
        #[serde(deserialize_with = "deny_empty_string")]
        funds: String,
    },
    Size {
        #[serde(deserialize_with = "deny_empty_string")]
        size: String,
    },
    Funds {
        #[serde(deserialize_with = "deny_empty_string")]
        funds: String,
    },
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
#[serde(tag = "order_type")]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum Quote {
    Limit(LimitQuote),
    Market(MarketQuote),
}

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
// NOTE: If funds and size are specified for a buy order,
// then size for the market order will be decremented internally within the matching engine and funds will remain unchanged.
// The intent is to offset your target size without limiting your buying power.
// If size is not specified, then funds will be decremented.
// For a market sell, the size will be decremented when encountering existing limit orders.
// NOTE: Any change message where the price is null indicates that the change message is for a market order.
// Change messages for limit orders will always have a price specified.
pub enum Decrement {
    Limit {
        price: String,
        old_size: String,
        new_size: String,
    },
    // CASE: (Side::Buy, MarketQuote::Funds)
    MarketFunds {
        old_funds: String,
        new_funds: String,
    },
    // CASE: (Side::Buy, MarketQuote::Both | MarketQuote::Size)
    // CASE: (Side::Sell, _)
    MarketSize {
        old_size: String,
        new_size: String,
    },
}

#[skip_serializing_none]
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
pub struct Received {
    pub sequence: u64,
    #[serde(deserialize_with = "deny_empty_string")]
    pub product_id: String,
    pub time: UtcDateTime,
    pub order_id: Uuid,
    pub side: Side,
    #[serde(flatten)]
    pub quote: Quote,
    #[serde(default, with = "empty_string_is_none")]
    pub client_oid: Option<Uuid>,
    #[serde(flatten)]
    pub user_profile: Option<UserProfile>,
}

#[skip_serializing_none]
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
// NOTE: Market orders execute immediately and no part of the market order will go on the open order book.
pub struct Open {
    pub sequence: u64,
    #[serde(deserialize_with = "deny_empty_string")]
    pub product_id: String,
    pub time: UtcDateTime,
    pub order_id: Uuid,
    pub side: Side,
    #[serde(flatten)]
    pub quote: LimitQuote,
    #[serde(flatten)]
    pub user_profile: Option<UserProfile>,
}

fn zero() -> String {
    String::from("0")
}

#[skip_serializing_none]
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
pub struct Done {
    pub sequence: u64,
    #[serde(deserialize_with = "deny_empty_string")]
    pub product_id: String,
    pub time: UtcDateTime,
    pub order_id: Uuid,
    pub side: Side,
    pub reason: Reason,
    #[serde(default = "OrderPrice::market", with = "OrderPriceConv")]
    pub price: OrderPrice<String>,
    // NOTE: market orders will not have a price and remaining_size field as they are never on the open order book at a given price.
    // BUG: `Done` message of market order which has both size and funds field has `remaining_size` field.
    // BUG: `Done` message of market order which has 0 size and 0 funds has `remaining_size` field with arbitrary value.
    #[serde(default = "zero")]
    pub remaining_size: String,
    #[serde(flatten)]
    pub user_profile: Option<UserProfile>,
}

#[skip_serializing_none]
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
pub struct Match {
    pub sequence: u64,
    #[serde(deserialize_with = "deny_empty_string")]
    pub product_id: String,
    pub time: UtcDateTime,
    pub trade_id: u64,
    pub maker_order_id: Uuid,
    pub taker_order_id: Uuid,
    pub side: Side,
    #[serde(deserialize_with = "deny_empty_string")]
    pub price: String,
    #[serde(deserialize_with = "deny_empty_string")]
    pub size: String,
    #[serde(flatten)]
    pub user_profile: Option<UserProfile>,
    #[serde(with = "OptOrderProfileConv", flatten)]
    pub order_profile: Option<OrderProfile>,
}

#[skip_serializing_none]
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
pub struct Decremented {
    pub sequence: u64,
    #[serde(deserialize_with = "deny_empty_string")]
    pub product_id: String,
    pub time: UtcDateTime,
    pub order_id: Uuid,
    pub side: Side,
    #[serde(with = "DecrementConv", flatten)]
    pub decrement: Decrement,
    #[serde(flatten)]
    pub user_profile: Option<UserProfile>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum Full {
    Received(Received),
    Open(Open),
    Done(Done),
    Match(Match),
    #[serde(rename = "change")]
    Decremented(Decremented),
}

impl Full {
    pub fn time(&self) -> &UtcDateTime {
        match &self {
            Full::Received(Received { time, .. }) => time,
            Full::Open(Open { time, .. }) => time,
            Full::Done(Done { time, .. }) => time,
            Full::Match(Match { time, .. }) => time,
            Full::Decremented(Decremented { time, .. }) => time,
        }
    }
    pub fn sequence(&self) -> u64 {
        match &self {
            Full::Received(Received { sequence, .. }) => *sequence,
            Full::Open(Open { sequence, .. }) => *sequence,
            Full::Done(Done { sequence, .. }) => *sequence,
            Full::Match(Match { sequence, .. }) => *sequence,
            Full::Decremented(Decremented { sequence, .. }) => *sequence,
        }
    }

    pub fn product_id(&self) -> &str {
        match &self {
            Full::Received(Received { product_id, .. }) => product_id,
            Full::Open(Open { product_id, .. }) => product_id,
            Full::Done(Done { product_id, .. }) => product_id,
            Full::Match(Match { product_id, .. }) => product_id,
            Full::Decremented(Decremented { product_id, .. }) => product_id,
        }
    }
}

#[skip_serializing_none]
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
#[serde(tag = "type")]
pub struct Activate {
    #[serde(deserialize_with = "deny_empty_string")]
    pub product_id: String,
    #[serde(deserialize_with = "deny_empty_string")]
    pub timestamp: String,
    pub order_id: Uuid,
    pub stop_type: StopType,
    pub side: Side,
    // TODO find possible combinations of (stop_price, size, funds)
    #[serde(deserialize_with = "deny_empty_string")]
    pub stop_price: String,
    #[serde(deserialize_with = "deny_empty_string")]
    pub size: String,
    #[serde(deserialize_with = "deny_empty_string")]
    pub funds: String,
    pub private: bool,
    #[serde(flatten)]
    pub user_profile: Option<UserProfile>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
#[serde(rename = "error")]
#[serde(tag = "type")]
pub struct ErrorMessage {
    pub message: String,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
#[serde(tag = "type")]
pub struct Subscriptions {
    pub channels: Vec<Channel>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[serde(tag = "type")]
pub struct Heartbeat {
    #[serde(rename = "sequence")]
    pub last_sequence: u64,
    pub last_trade_id: u64,
    #[serde(deserialize_with = "deny_empty_string")]
    pub product_id: String,
    pub time: UtcDateTime,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[serde(untagged)]
pub enum CBMessage {
    Full(Full),
    Heartbeat(Heartbeat),
    Activate(Activate),
    Subscriptions(Subscriptions),
    ErrorMessage(ErrorMessage),
}

#[derive(Error, Debug)]
pub enum CBError {
    #[error("failed to connect to coinbase websocket. ({0:?})")]
    ConnectionFailed(#[source] TError),
    #[error("failed to receive msg from coinbase websocket. ({0:?})")]
    ReceiveFailed(#[source] TError),
    #[error("connection closed from server side. ({0:?})")]
    ConnectionClosed(Option<(CloseCode, String)>),
    #[error("failed to send msg to coinbase websocket.")]
    SendFailed,
    #[error("invalid message type found. ({0:?})")]
    InvalidMessageType(TMessage),
    #[error("invalid message content found. ({0:})")]
    InvalidMessageContent(String),
}

pub trait CBStream: Stream<Item = Result<CBMessage, CBError>> + Send {}
impl<T> CBStream for T where T: Stream<Item = Result<CBMessage, CBError>> + Send {}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone, Copy)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum ChannelType {
    Heartbeat,
    Ticker,
    Level2,
    Matches,
    Full,
    User,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[serde(untagged)]
pub enum Channel {
    Name(ChannelType),
    WithProductIDs {
        name: ChannelType,
        product_ids: Vec<String>,
    },
}

impl Channel {
    pub fn new(name: ChannelType) -> Self {
        Channel::Name(name)
    }
    pub fn with_product_ids(name: ChannelType, product_ids: Vec<String>) -> Self {
        Channel::WithProductIDs { name, product_ids }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[serde(tag = "type", rename = "subscribe")]
pub struct Subscribe {
    pub product_ids: Vec<String>,
    pub channels: Vec<Channel>,
}

impl Subscribe {
    pub fn merge(&mut self, mut other: Subscribe) {
        self.channels.append(&mut other.channels);
        self.channels.dedup();
        self.product_ids.append(&mut other.product_ids);
        self.product_ids.dedup();
    }
}

#[async_trait]
pub trait CBSink: Sink<TMessage> + Unpin + Send + Sync {
    async fn subscribe(&mut self, subscribe: &Subscribe) -> Result<(), CBError> {
        let sub_json = serde_json::to_string(subscribe).unwrap();
        log::trace!("{:?}", sub_json);
        self.send(TMessage::Text(sub_json))
            .await
            .map_err(|_| CBError::SendFailed)?;
        Ok(())
    }
}

#[async_trait]
impl<T> CBSink for T where T: Sink<TMessage> + Unpin + Send + Sync {}

impl TryFrom<TMessage> for CBMessage {
    type Error = CBError;

    fn try_from(value: TMessage) -> Result<Self, CBError> {
        log::trace!("[RAW] {:?}", value);
        let msg = match value {
            TMessage::Text(msg) => Ok(serde_json::from_str(&msg).map_err(|e| {
                log::debug!("{:?}", e);
                CBError::InvalidMessageContent(msg)
            })?),
            TMessage::Close(msg) => Err(CBError::ConnectionClosed(
                msg.map(|CloseFrame { code, reason }| (code, reason.to_string())),
            )),
            invalid_msg => Err(CBError::InvalidMessageType(invalid_msg)),
        };
        log::trace!("[MSG] {:?}", msg);
        msg
    }
}

pub const WEBSOCKET_CONFIG: WebSocketConfig = WebSocketConfig {
    max_send_queue: None,
    max_message_size: None,
    max_frame_size: None,
    accept_unmasked_frames: false,
};

pub async fn connect_async(endpoint: &Url) -> Result<impl CBStream + CBSink, CBError> {
    let stream = tokio_tungstenite::connect_async_with_config(endpoint, Some(WEBSOCKET_CONFIG))
        .await
        .map_err(CBError::ConnectionFailed)?
        .0;
    let cb_stream = into_cb_stream(stream);
    Ok(cb_stream)
}

pub fn into_cb_stream(
    stream: impl TryStream<Ok = TMessage, Error = TError> + Sink<TMessage> + Unpin + Send + Sync,
) -> impl CBStream + CBSink {
    stream
        // NOTE: upon receiving a ping message, tungstenite cues a pong reply automatically.
        .try_filter(|msg| ready(!matches!(msg, &TMessage::Ping(_))))
        .map(|msg| match msg {
            Ok(msg) => CBMessage::try_from(msg),
            Err(e) => Err(CBError::ReceiveFailed(e)),
        })
        .sink_map_err(|_| CBError::SendFailed)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deserialize_json() {
        let done = r#"{"type":"done","side":"sell","product_id":"UNI-USD","time":"2021-08-25T17:09:14.566068Z","sequence":3585304993,"order_id":"61b0a035-f130-439c-8b8e-554f102d572d","reason":"canceled","price":"27.2005","remaining_size":"20"}"#;
        println!("{:?}", serde_json::from_str::<Done>(done).unwrap());
        serde_json::from_str::<CBMessage>(done).unwrap();

        let received = r#"{"type":"received","side":"sell","product_id":"ETH-GBP","time":"2021-08-25T18:17:39.150151Z","sequence":4931404746,"order_id":"abf98f5b-878e-4ccb-bbdc-3cb9c2ecdc67","order_type":"limit","size":"6.18088976","price":"2354.7","client_oid":"60311f97-681f-465f-8ba7-546ddc4f6a71"}"#;
        println!("{:?}", serde_json::from_str::<Received>(received).unwrap());
        serde_json::from_str::<CBMessage>(received).unwrap();

        let open = r#"{"type":"open","side":"sell","product_id":"ALGO-USD","time":"2021-08-25T18:30:31.402926Z","sequence":3091541928,"price":"1.0757","order_id":"b118f276-be5c-434b-870f-78fb8a30b553","remaining_size":"140"}"#;
        println!("{:?}", serde_json::from_str::<Open>(open).unwrap());
        serde_json::from_str::<CBMessage>(open).unwrap();
    }
}
