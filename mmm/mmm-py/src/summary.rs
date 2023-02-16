use std::collections::HashMap;
use mmm_nasdaq::data::OrderStatus;
use mmm_us::price::PriceBasis;
use mmm_core::collections::Side;

use mmm_nasdaq::book::{Body, Message};
use numpy::PyReadonlyArray2;
use pyo3::pyclass;

#[pyclass]
pub struct TrajectorySummary {
    #[pyo3(get)]
    reference: u64,
    #[pyo3(get)]
    trajectory: Vec<String>,
    #[pyo3(get)]
    timestamps: Vec<u64>,
    #[pyo3(get)]
    is_buy: bool,
    #[pyo3(get)]
    price: u32,
    #[pyo3(get)]
    shares: u32,
    #[pyo3(get)]
    executed_shares: Vec<u32>,
    #[pyo3(get)]
    executed_with_price_shares: Vec<u32>,
    #[pyo3(get)]
    cancelled_shares: Vec<u32>,
}

fn side_to_is_buy(side: Side) -> bool {
    matches!(side, Side::Bid)
}

impl TrajectorySummary {
    fn new_with_add(timestamp: u64, reference: u64, shares: u32, price: u32, side: Side) -> Self {
        Self {
            reference,
            trajectory: vec!["A".to_string()],
            timestamps: vec![timestamp],
            is_buy: side_to_is_buy(side),
            price,
            shares,
            executed_shares: Vec::new(),
            executed_with_price_shares: Vec::new(),
            cancelled_shares: Vec::new(),
        }
    }

    fn delete(&mut self, timestamp: u64) {
        self.trajectory.push("D".to_string());
        self.timestamps.push(timestamp);
        self.cancelled_shares
            .push(self.shares - self.executed_shares.iter().sum::<u32>());
    }
    fn cancel(&mut self, timestamp: u64, cancelled: u32) {
        self.trajectory.push("X".to_string());
        self.timestamps.push(timestamp);
        self.cancelled_shares.push(cancelled);
    }
    fn execute(&mut self, timestamp: u64, executed: u32) {
        self.trajectory.push("E".to_string());
        self.timestamps.push(timestamp);
        self.executed_shares.push(executed);
    }
    fn execute_with_price(&mut self, timestamp: u64, executed: u32) {
        self.trajectory.push("C".to_string());
        self.timestamps.push(timestamp);
        self.executed_with_price_shares.push(executed);
    }
    fn replace_with(&mut self, timestamp: u64, shares: u32, old_shares: u32, price: u32, reference: u64) -> Self {
        self.trajectory.push("U".to_string()); 
        self.timestamps.push(timestamp);
        self.cancelled_shares.push(old_shares);

        Self {
            reference,
            trajectory: vec!["U".to_string()],
            timestamps: vec![timestamp],
            is_buy: self.is_buy,
            price,
            shares,
            executed_shares: Vec::new(),
            executed_with_price_shares: Vec::new(),
            cancelled_shares: Vec::new(),
        }
    }
}

pub fn create_trajectory_summaries(
    encoded_actions: PyReadonlyArray2<u64>,
) -> Vec<TrajectorySummary> {
    let encoded_actions = encoded_actions.as_array();

    let actions = encoded_actions
        .rows()
        .into_iter()
        .map(|row| Message::from(row.as_slice().unwrap()))
        .collect::<Vec<_>>();

    let mut status_map = HashMap::new();
    let mut summaries = HashMap::new();
    let mut message_count = 0;
    for message in actions {
        match message.body {
            Body::AddOrder {
                reference,
                shares,
                price,
                side,
                ..
            } => 
            { 
                let status = OrderStatus::new(
                    PriceBasis::from(price as u32), 
                    side, 
                    shares as u32, 
                    message_count,
                    0);
                status_map.insert(reference, status.clone());
                
                assert!(summaries
                .insert(
                    reference,
                    TrajectorySummary::new_with_add(
                        message.time,
                        reference,
                        shares as u32,
                        price as u32,
                        side
                    ),
                )
                .is_none())
            }
            Body::DeleteOrder { reference } => {
                let _ = status_map.remove(&reference).unwrap();
                
                summaries.get_mut(&reference).unwrap().delete(message.time)
            }
            Body::OrderCancelled {
                reference,
                cancelled,
            } => { 
                let status = status_map.get_mut(&reference).unwrap();
                status.shares -= cancelled;
               
                summaries
                .get_mut(&reference)
                .unwrap()
                .cancel(message.time, cancelled as u32)
            }
            Body::ReplaceOrder {
                new_reference,
                shares,
                price,
                old_reference,
            } => {
                // remove old reference from status
                let old_status = status_map.remove(&old_reference).unwrap();
                let old_shares = old_status.shares;
                
                // insert summaries
                let new_order = summaries.get_mut(&old_reference).unwrap().replace_with(
                    message.time,
                    shares as u32,
                    old_shares as u32,
                    price as u32,
                    new_reference,

                );
                assert!(summaries.insert(new_reference, new_order).is_none());
                
                // insert new order status
                let status = OrderStatus {
                    price: price,
                    side: old_status.side,
                    shares: shares as u64,
                    index: message_count,
                    mpid_val: old_status.mpid_val,
                };
                status_map.insert(new_reference, status.clone());
            }
            Body::OrderExecuted {
                reference,
                executed,
            } =>{ 
                let status = status_map.get_mut(&reference).unwrap();
                status.shares -= executed;

                summaries
                .get_mut(&reference)
                .unwrap()
                .execute(message.time, executed as u32)
            }
            Body::OrderExecutedWithPrice {
                reference,
                executed,
            } => {
                let status = status_map.get_mut(&reference).unwrap();
                status.shares -= executed;
                
                summaries
                .get_mut(&reference)
                .unwrap()
                .execute_with_price(message.time, executed as u32)
            }
             _ => {}
        }
        message_count += 1;
    }

    let mut summaries = summaries.into_iter().map(|(_, s)| s).collect::<Vec<_>>();
    summaries.sort_by(|s1, s2| s1.reference.cmp(&s2.reference));
    summaries

    // let out_dir = create_folder(path, out_dir);
    // let done_file = out_dir.join(".traj.done");
    // if done_file.exists() {
    //     println!(
    //         "skip `process_trajectory_file` for {:?}. done file already exists.",
    //         path
    //     );
    //     return;
    // }

    // let mut container: HashMap<(usize, u64), TrajectorySummary> = HashMap::new();

    // let mut storable = HashMap::new();
    // for ((stock_locate, _), summary) in container {
    //     let name = name_map.get(&stock_locate).unwrap();
    //     let summary_vec = match storable.get_mut(name) {
    //         Some(summary_vec) => summary_vec,
    //         None => storable.entry(name.clone()).or_insert_with(Vec::new),
    //     };
    //     summary_vec.push(summary)
    // }

    // for (name, summaires) in storable {
    //     let serialized = serde_json::to_vec(&summaires).unwrap();
    //     dump(out_dir.join(format!("{}_traj.json.zst", name)), &serialized)
    // }
    // File::create(done_file).unwrap();
}
