use crate::util::RateLimit;
use crate::websocket::{
    into_cb_stream, CBMessage, CBSink, CBStream, Full, Subscribe, WEBSOCKET_CONFIG,
};
use anyhow::Context;
use futures::lock::Mutex;
use futures::{stream::select_all, Stream};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::{collections::VecDeque, sync::Arc};
use tokio::net::TcpSocket;
use tokio::{
    sync::{broadcast, mpsc},
    time::Duration,
};
use tokio_stream::{wrappers::UnboundedReceiverStream, StreamExt};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Error as TError;
use url::Url;

struct Buffer<T> {
    name: String,
    read_seq: u64,
    write_seqs: Vec<u64>,
    queue: VecDeque<Option<(u64, T)>>,
}

impl<T> Buffer<T> {
    pub fn new(name: String, count: usize, sequence: u64) -> Self {
        Self {
            name,
            read_seq: sequence,
            write_seqs: vec![0; count],
            queue: Default::default(),
        }
    }

    pub fn write(&mut self, source: usize, sequence: u64, data: T)
    where
        T: Clone,
    {
        let write_seq = &mut self.write_seqs[source];
        match (*write_seq + 1).cmp(&sequence) {
            Ordering::Equal => *write_seq = sequence,
            Ordering::Less => {
                log::trace!(
                    "{}| local data loss occurred for {}. ({} -> {})",
                    source,
                    self.name,
                    write_seq,
                    sequence
                );
                *write_seq = sequence;
            }
            Ordering::Greater => log::trace!(
                "{}| local data inversion occurred. ({} -> {})",
                source,
                write_seq,
                sequence
            ),
        }

        if sequence > self.read_seq {
            let len_queue = self.queue.len();
            let len_required = (sequence - self.read_seq) as usize;
            match len_queue.cmp(&len_required) {
                std::cmp::Ordering::Greater => {
                    let index = len_required - 1;
                    let placeholder = &mut self.queue[index];
                    if placeholder.is_none() {
                        *placeholder = Some((sequence, data));
                    }
                }
                std::cmp::Ordering::Less => {
                    self.queue.extend(
                        std::iter::repeat(None)
                            .take((len_required - len_queue) - 1)
                            .chain(std::iter::once(Some((sequence, data)))),
                    );
                }
                std::cmp::Ordering::Equal => {}
            }
        }
    }

    pub fn read(&mut self) -> Option<(u64, Option<T>)> {
        match self.queue.front() {
            Some(Some(_)) => {
                let (seq, data) = self.queue.pop_front().unwrap().unwrap();
                self.read_seq = seq;
                Some((seq, Some(data)))
            }
            Some(None) => {
                if self
                    .write_seqs
                    .iter()
                    .any(|write_seq| *write_seq <= self.read_seq)
                {
                    None
                } else {
                    self.queue.pop_front();
                    self.read_seq += 1;
                    Some((self.read_seq, None))
                }
            }
            None => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Loss {
    pub product_id: String,
    pub sequence: u64,
}

async fn run_broker(
    rxs: Vec<impl Stream<Item = (usize, Full)> + Unpin>,
    tx: mpsc::UnboundedSender<Result<Full, Loss>>,
) -> Option<()> {
    let count = rxs.len();
    let mut rx = select_all(rxs);
    let mut flow_map = HashMap::new();

    loop {
        let (id, full) = rx.next().await?;
        let seq = full.sequence();
        let seq_flow = match flow_map.get_mut(full.product_id()) {
            Some(flows) => flows,
            None => flow_map
                .entry(full.product_id().to_string())
                .or_insert_with(|| Buffer::new(full.product_id().into(), count, seq - 1)),
        };

        log::trace!(
            "[Write] id: {} product: {} seq: {} write_seq: {:?}, read_seq: {:?}",
            id,
            full.product_id(),
            full.sequence(),
            seq_flow.write_seqs,
            seq_flow.read_seq
        );

        seq_flow.write(id, seq, full);

        if let Some((sequence, data)) = seq_flow.read() {
            log::trace!(
                "[Read] id: {} product: {} seq: {} loss: {} write_seq: {:?}, read_seq: {:?}",
                id,
                seq_flow.name,
                sequence,
                data.is_none(),
                seq_flow.write_seqs,
                seq_flow.read_seq
            );
            let msg = data.ok_or_else(|| Loss {
                product_id: seq_flow.name.clone(),
                sequence,
            });
            tx.send(msg).ok()?
        } else if seq_flow
            .write_seqs
            .iter()
            .all(|write_seq| *write_seq == 0 || *write_seq > seq_flow.read_seq)
        {
            log::trace!(
                "[Warning] data loss may occured. id: {} product: {} write_seq: {:?}, read_seq: {:?}",
                id,
                seq_flow.name,
                seq_flow.write_seqs,
                seq_flow.read_seq
            );
        }
    }
}

async fn highly_available_channel(
    count: usize,
) -> (
    Vec<mpsc::UnboundedSender<Full>>,
    mpsc::UnboundedReceiver<Result<Full, Loss>>,
) {
    let (txs, rxs) = (0..count)
        .into_iter()
        .map(|id| {
            let (tx, rx) = mpsc::unbounded_channel();
            let rx = UnboundedReceiverStream::new(rx).map(move |v| (id, v));
            (tx, rx)
        })
        .unzip::<_, _, Vec<_>, Vec<_>>();
    let (tx, rx) = mpsc::unbounded_channel();

    tokio::spawn(run_broker(rxs, tx));

    (txs, rx)
}

fn merge_subscribe(orig: &mut Option<Subscribe>, update: Subscribe) {
    if let Some(prev_sub) = orig {
        prev_sub.merge(update);
    } else {
        *orig = Some(update)
    }
}

async fn run_async(
    id: usize,
    endpoint: &Url,
    rate_limit: &mut Arc<Mutex<RateLimit>>,
    subscribe: &mut Option<Subscribe>,
    tx: &mut mpsc::UnboundedSender<Full>,
    rx: &mut broadcast::Receiver<Subscribe>,
    interface: SocketAddr,
) -> anyhow::Result<()> {
    let mut cbws = {
        let mut rate_limit = rate_limit.lock().await;
        rate_limit.wait().await;
        connect_async_via(id, endpoint, interface).await
    }?;
    if let Some(subscribe) = subscribe {
        cbws.subscribe(subscribe)
            .await
            .context("failed to send subscription message.")?;
    }

    loop {
        tokio::select! {
            msg = cbws.next() => {
                let msg = msg.context("failed to read from websocket.")??;
                match msg {
                    CBMessage::Full(full) => tx.send(full)?,
                    CBMessage::Subscriptions(msg) => log::trace!("{}| received `Subscriptions` ({:?})", id, msg),
                    CBMessage::ErrorMessage(error) => anyhow::bail!("received `ErrorMessage` ({:?})", error),
                    CBMessage::Heartbeat(msg) => log::info!("{}| received `Hearbeat` ({:?})", id , msg),
                    CBMessage::Activate(msg) => log::info!("{}| received `Activate` ({:?})", id, msg),
                }
            }
            sub = rx.recv() =>
            {
                let sub_update = sub.context("failed to receive subscription.")?;
                log::info!("{}| update subscription request received. ({:?})", id, sub_update);
                cbws.subscribe(&sub_update).await.context("failed to send subscription message.")?;
                merge_subscribe(subscribe, sub_update);
                log::info!("{}| subscription updated. ({:?})", id, subscribe);
            },
        }
    }
}

async fn run_async_forever(
    id: usize,
    endpoint: Url,
    mut rate_limit: Arc<Mutex<RateLimit>>,
    mut tx: mpsc::UnboundedSender<Full>,
    mut rx: broadcast::Receiver<Subscribe>,
    interface: SocketAddr,
) {
    let mut subscribe = None;
    while let Ok(sub_msg) = rx.try_recv() {
        merge_subscribe(&mut subscribe, sub_msg);
    }

    loop {
        let reason = run_async(
            id,
            &endpoint,
            &mut rate_limit,
            &mut subscribe,
            &mut tx,
            &mut rx,
            interface,
        )
        .await;
        log::info!("{}| retry run_async with reason ({:?})", id, reason);
    }
}

pub async fn highly_available_receive(
    endpoint: Url,
    redundancy: usize,
    interfaces: Option<Vec<SocketAddr>>,
) -> (
    broadcast::Sender<Subscribe>,
    mpsc::UnboundedReceiver<Result<Full, Loss>>,
) {
    let rate_limit = Arc::new(Mutex::new(RateLimit::new(Duration::from_secs(4))));

    let default_interface = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0));
    let interfaces = interfaces.unwrap_or_else(|| vec![default_interface]);
    let count = interfaces.len() * redundancy;
    let mut interfaces = interfaces.into_iter().cycle();

    let (sub_tx, mut sub_rx) = broadcast::channel(8);
    let (txs, rx) = highly_available_channel(count).await;

    for (id, tx) in txs.into_iter().enumerate() {
        tokio::spawn(run_async_forever(
            id,
            endpoint.clone(),
            rate_limit.clone(),
            tx.clone(),
            sub_rx,
            interfaces.next().unwrap(),
        ));
        sub_rx = sub_tx.subscribe();
    }

    (sub_tx, rx)
}

async fn connect_async_via(
    id: usize,
    endpoint: &Url,
    interface: SocketAddr,
) -> anyhow::Result<impl CBStream + CBSink> {
    let socket_addrs = to_socket_addrs(endpoint).await?;

    let (socket, mut socket_addrs) = if interface.is_ipv4() {
        (
            TcpSocket::new_v4()?,
            socket_addrs.filter(SocketAddr::is_ipv4).collect::<Vec<_>>(),
        )
    } else {
        (
            TcpSocket::new_v6()?,
            socket_addrs.filter(SocketAddr::is_ipv6).collect::<Vec<_>>(),
        )
    };
    socket_addrs.sort();
    let socket_addr = socket_addrs[id % socket_addrs.len()];

    log::debug!(
        "{}| connect to address {} via interface {}.",
        id,
        socket_addr,
        interface
    );

    socket.bind(interface).unwrap();
    let stream = socket.connect(socket_addr).await?;

    let stream = tokio_tungstenite::client_async_tls_with_config(
        endpoint.into_client_request()?,
        stream,
        Some(WEBSOCKET_CONFIG),
        None,
    )
    .await?
    .0;
    Ok(into_cb_stream(stream))
}

async fn to_socket_addrs(endpoint: &Url) -> Result<impl Iterator<Item = SocketAddr>, TError> {
    use tokio::net::lookup_host;
    use tokio_tungstenite::tungstenite::error::UrlError;

    let request = endpoint.into_client_request()?;

    let host = match request.uri().host() {
        Some(d) => Ok(d.to_string()),
        None => Err(TError::Url(UrlError::NoHostName)),
    }?;

    let port = request
        .uri()
        .port_u16()
        .or_else(|| match request.uri().scheme_str() {
            Some("wss") => Some(443),
            Some("ws") => Some(80),
            _ => None,
        })
        .ok_or(TError::Url(UrlError::UnsupportedUrlScheme))?;
    let addr = format!("{}:{}", host, port);
    Ok(lookup_host(addr).await?)
}
