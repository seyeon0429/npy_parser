use std::collections::HashMap;

use futures_util::StreamExt;
use mmm_coinbase::websocket::{connect_async, CBMessage, CBSink, Channel, ChannelType, Subscribe};
use url::Url;

#[tokio::main]
async fn main() {
    env_logger::init();
    let mut stream = connect_async(&Url::parse("wss://ws-feed.pro.coinbase.com").unwrap())
        .await
        .unwrap();

    let sub = Subscribe {
        product_ids: vec!["BTC-USD".into()],
        channels: vec![Channel::Name(ChannelType::Full)],
    };

    stream.subscribe(&sub).await.unwrap();

    let mut seq_map = HashMap::new();
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(msg) => {
                if let CBMessage::Full(full) = msg {
                    let seq = full.sequence();
                    let last_seq = seq_map
                        .entry(full.product_id().to_string())
                        .or_insert(seq - 1);
                    assert_eq!(*last_seq + 1, seq);
                    *last_seq = seq;
                }
            }
            Err(e) => {
                eprintln!("[ERROR] {:?}", e);
            }
        }
    }
    panic!("[ERROR] connection dropped.")
}
