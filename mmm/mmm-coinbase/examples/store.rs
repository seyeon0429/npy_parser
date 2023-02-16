use async_compression::tokio::write::ZstdEncoder;
use chrono::{DateTime, Utc};
use coinbase_pro_rs::{
    structs::public::{Book, BookRecordL3, Product},
    ASync, CBError, Public, MAIN_URL,
};
use futures::TryStreamExt;
use mmm_coinbase::{
    high_availability::{highly_available_receive, Loss},
    websocket::{Channel, ChannelType, Full, Subscribe},
};
use std::{
    collections::{hash_map, HashMap, HashSet},
    fmt::Debug,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
};
use structopt::StructOpt;
use tokio::{
    io::{AsyncWriteExt, BufWriter},
    sync::{broadcast, mpsc, oneshot},
    time::{sleep, Duration, Instant},
};
use tokio_stream::{wrappers::UnboundedReceiverStream, StreamExt};
use url::Url;

// seconds
const API_DELAY: Duration = Duration::from_millis(350);
const SAVE_INTERVAL: i64 = 60 * 15;

#[derive(StructOpt)]
struct Opt {
    /// destination path to save data
    dest: PathBuf,
    /// coinbase websocket endpoint
    #[structopt(default_value = "wss://ws-feed.pro.coinbase.com")]
    endpoint: Url,
    /// number of connections per interface.
    #[structopt(default_value = "6", short, long)]
    redundancy: usize,
    /// network interfaces to use
    #[structopt(short, long)]
    interfaces: Option<Vec<IpAddr>>,
    /// compression level
    #[structopt(short = "z", long = "compression", default_value = "10")]
    compression_level: u32,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let Opt {
        endpoint,
        redundancy,
        dest,
        interfaces,
        compression_level,
    } = Opt::from_args();
    let interfaces =
        interfaces.map(|ifs| ifs.into_iter().map(|ip| SocketAddr::new(ip, 0)).collect());

    let (api_tx, api_rx) = mpsc::unbounded_channel();
    tokio::spawn(async {
        let mut api = APIServer::new(api_rx).await;
        api.run().await;
    });

    let (update_tx, update_rx) = mpsc::unbounded_channel();
    let (cb_tx, cb_rx) = highly_available_receive(endpoint, redundancy, interfaces).await;

    tokio::spawn(update_subscription_loop(update_rx, api_tx.clone(), cb_tx));
    update_tx.send(()).unwrap();

    receiver_loop(cb_rx, api_tx, update_tx, &dest, compression_level).await
}

// #[instrument(fields(cb_tx = "cb_tx"))]
async fn update_subscription_loop(
    mut update_rx: mpsc::UnboundedReceiver<()>,
    api_tx: mpsc::UnboundedSender<APIRequest>,
    cb_tx: broadcast::Sender<Subscribe>,
) {
    let mut prev_pids = HashSet::new();

    while let Some(()) = update_rx.recv().await {
        let (tx, rx) = oneshot::channel();
        api_tx.send(APIRequest::GetProducts { tx }).unwrap();
        log::trace!("GetProducts request sent.");
        let updated_pids = rx
            .await
            .unwrap()
            .into_iter()
            .map(|p| p.id)
            // .filter(|pid| pid.contains("BTC") || pid.contains("ETH"))
            .collect::<HashSet<_>>();
        // let current_pids = vec!["BTC-USD".to_string()]
        //     .into_iter()
        //     .collect::<HashSet<_>>();
        log::trace!("updated_pids: {:?}", updated_pids);

        let new_pids: Vec<String> = updated_pids.difference(&prev_pids).cloned().collect();
        if !new_pids.is_empty() {
            log::info!("newly added product ids: {:?}", new_pids);

            let subscribe = Subscribe {
                product_ids: new_pids,
                channels: vec![Channel::Name(ChannelType::Full)],
            };
            cb_tx.send(subscribe).unwrap();
            log::trace!("subscription request sent.");
        } else {
            log::info!("there are no new products to subscribe.")
        }

        let old_pids: Vec<&str> = prev_pids
            .difference(&updated_pids)
            .map(|x| x.as_str())
            .collect::<Vec<&str>>();
        // TODO: unsubscribe
        if !old_pids.is_empty() {
            log::info!("removed product ids: {:?}", old_pids);
        }
        prev_pids = updated_pids;
    }
}

// #[instrument]
pub(crate) async fn receiver_loop(
    cb_rx: mpsc::UnboundedReceiver<Result<Full, Loss>>,
    api_tx: mpsc::UnboundedSender<APIRequest>,
    update_tx: mpsc::UnboundedSender<()>,
    dest: &Path,
    compression_level: u32,
) {
    let create_out_dir = |datetime: DateTime<Utc>| async move {
        let out_dir = dest
            .join(datetime.format("%Y-%m-%d").to_string())
            .join(datetime.format("%H%M%S").to_string());
        let _ = tokio::fs::create_dir_all(&out_dir).await;
        out_dir
    };

    let mut cb_rx = UnboundedReceiverStream::new(cb_rx).map_ok(|full| (Utc::now(), full));
    let mut product_map = HashMap::new();
    let mut prev_datetime = Utc::now();
    log::trace!("start_time: {:?}", prev_datetime);
    let mut out_dir = create_out_dir(prev_datetime).await;
    let save_interval = chrono::Duration::seconds(SAVE_INTERVAL);
    let mut cnt: u64 = 0;
    let mut latencies = vec![];
    let mut last_loss = Instant::now();

    loop {
        match cb_rx.next().await.unwrap() {
            Ok((machine_time, full)) => {
                let time = *full.time();

                let prev_date = prev_datetime.date();
                let date = time.date();

                if prev_date < date {
                    let prev_product_map: HashMap<_, (_, ZstdEncoder<BufWriter<_>>)> =
                        std::mem::take(&mut product_map);
                    tokio::spawn(async {
                        for (_, mut f) in prev_product_map.into_values() {
                            f.shutdown().await.unwrap();
                        }
                    });
                    log::info!("date refreshed from {:?} to {:?}", prev_date, date,);
                    prev_datetime = time;
                    out_dir = create_out_dir(time).await;
                    cnt = 0;
                    update_tx.send(()).unwrap();
                }

                let mut serialized = serde_json::to_vec(&(&machine_time, &full)).unwrap();
                serialized.push(b'\n');
                let (prev_save, writer) = match product_map.get_mut(full.product_id()) {
                    Some(v) => v,
                    None => match product_map.entry(full.product_id().to_string()) {
                        hash_map::Entry::Vacant(v) => {
                            let product_folder = out_dir.join(full.product_id());
                            let _ = tokio::fs::create_dir_all(product_folder.join("book")).await;
                            v.insert((
                                time,
                                ZstdEncoder::with_quality(
                                    BufWriter::new(
                                        tokio::fs::File::create(
                                            product_folder
                                                .join(format!("full-{}.json.zst", full.sequence())),
                                        )
                                        .await
                                        .unwrap(),
                                    ),
                                    async_compression::Level::Precise(compression_level),
                                ),
                            ))
                        }
                        hash_map::Entry::Occupied(_) => unsafe {
                            std::hint::unreachable_unchecked()
                        },
                    },
                };

                writer.write_all(&serialized).await.unwrap();

                if (time - *prev_save) > save_interval {
                    api_tx
                        .send(APIRequest::SaveBook {
                            product_id: full.product_id().to_string(),
                            minimum_sequence: None,
                            out_folder: out_dir.join(full.product_id()).join("book"),
                        })
                        .unwrap();
                    *prev_save = *prev_save + save_interval;
                }

                cnt += 1;
                latencies.push((machine_time - time).num_milliseconds());
                if cnt % 10000000 == 0 {
                    tokio::task::spawn_blocking(move || {
                        latencies.sort_unstable();
                        println!(
                            "[{}] cnt: {} latency(99%): {:?}ms",
                            machine_time,
                            cnt,
                            latencies[(latencies.len() as f32 * 0.99) as usize]
                        );
                    });
                    latencies = vec![];

                    // for (pid, fulls) in store.iter() {
                    //     log::debug!("[store] {}: {}", pid, fulls.len())
                    // }
                }
            }
            Err(Loss {
                product_id,
                sequence,
            }) => {
                if last_loss.elapsed() > Duration::from_secs(60) {
                    log::error!("loss occurred.");
                    last_loss = Instant::now();
                }

                log::trace!("data loss {} {}", product_id, sequence);

                api_tx
                    .send(APIRequest::SaveBook {
                        minimum_sequence: Some(sequence),
                        out_folder: out_dir.join(product_id.clone()).join("book"),
                        product_id,
                    })
                    .unwrap();
                log::trace!("SaveBook request sent.");
            }
        };
    }
}
// use log::instrument;

pub struct APIServer {
    last_run: Instant,
    last_save: HashMap<String, Instant>,
    inner: Public<ASync>,
    rx: mpsc::UnboundedReceiver<APIRequest>,
}

#[derive(Debug)]
enum APIRequest {
    SaveBook {
        product_id: String,
        minimum_sequence: Option<u64>,
        out_folder: PathBuf,
    },
    GetProducts {
        tx: oneshot::Sender<Vec<Product>>,
    },
}

impl Debug for APIServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("API {{ last_run: {:?} }}", self.last_run))
    }
}

impl APIServer {
    // #[instrument]
    async fn new(request_rx: mpsc::UnboundedReceiver<APIRequest>) -> Self {
        let now = Instant::now();
        let inner = Public::new(MAIN_URL);
        let mut api = APIServer {
            last_run: now - API_DELAY,
            last_save: HashMap::new(),
            inner,
            rx: request_rx,
        };

        // call `get_products` to initialize `last_save` field.
        api.get_products().await.unwrap();
        api
    }

    fn remaining_time(&self) -> Option<Duration> {
        let now = Instant::now();
        let elapsed = now - self.last_run;
        if API_DELAY > elapsed {
            Some(API_DELAY - elapsed)
        } else {
            None
        }
    }

    // #[instrument]
    async fn wait_rate_limit(&mut self) {
        if let Some(remaining) = self.remaining_time() {
            sleep(remaining).await;
        }
        self.last_run = Instant::now();
    }

    async fn get_book(&mut self, product_id: &str) -> Result<Book<BookRecordL3>, CBError> {
        self.wait_rate_limit().await;
        self.inner.get_book::<BookRecordL3>(product_id).await
    }

    // #[instrument]
    async fn get_products(&mut self) -> Result<Vec<Product>, CBError> {
        self.wait_rate_limit().await;
        let result = self.inner.get_products().await;
        if let Ok(ref products) = result {
            self.last_save = products
                .iter()
                .map(|p| {
                    (
                        p.id.to_string(),
                        self.last_save.remove(&p.id).unwrap_or_else(Instant::now),
                    )
                })
                .collect::<HashMap<String, Instant>>();
        };
        result
    }

    // #[instrument]
    async fn save_book(
        &mut self,
        product_id: &str,
        minimum_sequence: Option<u64>,
        out_folder: &Path,
    ) -> Result<(), CBError> {
        loop {
            let book = self.get_book(product_id).await?;
            let sequence = book.sequence as u64;

            if let Some(min_seq) = minimum_sequence {
                log::trace!("sequence check ({} <= {})", min_seq, sequence);
                if sequence < min_seq {
                    log::debug!("sequence check failed.");
                    continue;
                }
            }

            let out_path = out_folder.join(&format!("{}.json", sequence));
            let _ = tokio::fs::create_dir_all(&out_folder).await;
            tokio::fs::File::create(out_path)
                .await
                .unwrap()
                .write_all(&serde_json::to_vec(&book).unwrap())
                .await
                .unwrap();
            *self.last_save.get_mut(product_id).unwrap() = Instant::now();

            return Ok(());
        }
    }

    // #[instrument(level = "trace")]
    async fn run(&mut self) {
        while let Some(request) = self.rx.recv().await {
            log::trace!("new request received. ({:?})", request);
            match request {
                APIRequest::SaveBook {
                    product_id,
                    minimum_sequence,
                    out_folder,
                } => loop {
                    match self
                        .save_book(&product_id, minimum_sequence, &out_folder)
                        .await
                    {
                        Ok(_) => break,
                        Err(e) => log::error!("retry `save_book` with reason ({:?})", e),
                    }
                },
                APIRequest::GetProducts { tx: coord_tx } => loop {
                    match self.get_products().await {
                        Ok(ok) => break coord_tx.send(ok).unwrap(),
                        Err(e) => log::error!("retry `get_products with reason ({:?})", e),
                    }
                },
            }
            log::trace!("response sent.");
        }
        panic!("tx dropped.")
    }
}
