use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    str::FromStr,
};

use mmm_nyse::{
    book::{Message, NyseOrderBook},
    data::{load, process_file, NUM_FIELDS},
    replay::OrderbookDepth,
};
use numpy::{PyArray1, PyReadonlyArray2, ToPyArray};
use pyo3::{prelude::pymodule, pyclass, pymethods, types::PyModule, PyResult, Python};

use crate::summary::TrajectorySummary;

type Trajectory<'py, T> = Vec<(
    usize,
    &'py PyArray1<u64>,
    u64,
    HashMap<String, HashMap<u64, T>>,
    // Vec<HashMap<String, HashMap<u64, Vec<(u64, u64, u64)>>>>,
)>;

#[pyclass]
struct TimeBasedQueueReplay(mmm_nyse::replay::TimeBasedQueueReplay);

#[pymethods]
impl TimeBasedQueueReplay {
    #[new]
    fn new(path: &str, level: usize) -> Self {
        Self::by_level(path, level)
    }
    #[staticmethod]
    fn by_level(path: &str, level: usize) -> Self {
        let path = PathBuf::from(path);
        let msgs = VecDeque::from_iter(load(&path, NUM_FIELDS));
        Self(mmm_nyse::replay::TimeBasedQueueReplay::new(
            msgs,
            OrderbookDepth::Level(level),
        ))
    }
    #[staticmethod]
    fn by_spread(path: &str, spread: u64) -> Self {
        let path = PathBuf::from(path);
        let msgs = VecDeque::from_iter(load(&path, NUM_FIELDS));
        Self(mmm_nyse::replay::TimeBasedQueueReplay::new(
            msgs,
            OrderbookDepth::Spread(spread),
        ))
    }
    fn step(&mut self) -> Option<mmm_nyse::replay::QueueResult> {
        self.0.step()
    }
}

#[pyclass]
struct TimeBasedVolumeReplay(mmm_nyse::replay::TimeBasedVolumeReplay);

#[pymethods]
impl TimeBasedVolumeReplay {
    #[new]
    fn new(path: &str, level: usize) -> Self {
        Self::by_level(path, level)
    }
    #[staticmethod]
    fn by_level(path: &str, level: usize) -> Self {
        let path = PathBuf::from(path);
        let msgs = VecDeque::from_iter(load(&path, NUM_FIELDS));
        Self(mmm_nyse::replay::TimeBasedVolumeReplay::new(
            msgs,
            OrderbookDepth::Level(level),
        ))
    }
    #[staticmethod]
    fn by_spread(path: &str, spread: u64) -> Self {
        let path = PathBuf::from(path);
        let msgs = VecDeque::from_iter(load(&path, NUM_FIELDS));
        Self(mmm_nyse::replay::TimeBasedVolumeReplay::new(
            msgs,
            OrderbookDepth::Spread(spread),
        ))
    }
    fn step(&mut self) -> Option<mmm_nyse::replay::VolumeResult> {
        self.0.step()
    }
}

fn compile_trajectory<'py, F, T>(
    py: Python<'py>,
    encoded_actions: PyReadonlyArray2<u64>,
    indicies: Vec<u64>,
    latencies: Vec<u64>,
    func: F,
    with_validation: bool,
    is_inclusive: bool,
) -> Vec<(usize, &'py PyArray1<u64>, u64, T)>
where
    F: Fn(&mut NyseOrderBook) -> T,
{
    if indicies.is_empty() {
        return Vec::new();
    }

    let encoded_actions = encoded_actions.as_array();

    let actions = encoded_actions
        .rows()
        .into_iter()
        .map(|row| Message::from(row.as_slice().unwrap()))
        .collect::<Vec<_>>();

    let indicies = indicies.into_iter().map(|v| v as usize).collect::<Vec<_>>();
    let times = indicies
        .clone()
        .into_iter()
        .zip(latencies.clone())
        .map(|(index, latency)| actions[index].time - latency)
        .collect::<Vec<_>>();

    let mut adj_idxs = VecDeque::new();
    let cmp_fn = if is_inclusive { u64::le } else { u64::lt };
    // let inc = if is_inclusive { |x:usize| x } else {|x:usize| x+1};

    for (index, time) in indicies.clone().into_iter().zip(times.clone()) {
        let pos = actions[..=index]
            .iter()
            .rposition(|a| cmp_fn(&a.time, &time))
            .map(|x| x + 1)
            .unwrap_or(0);
        adj_idxs.push_back(pos);
    }
    // println!("{:?}",adj_idxs);
    let mut book = NyseOrderBook::new(with_validation);

    let mut trajectory = Vec::with_capacity(times.len());

    let mut i = 0;
    let last_idx = *adj_idxs.back().unwrap();
    let mut adj_target = adj_idxs.pop_front().unwrap();

    while i <= last_idx {
        if i == adj_target {
            let len = trajectory.len();
            let index = indicies[len];
            let latency = latencies[len];
            let tuple = (
                index,
                encoded_actions.row(index).to_pyarray(py),
                latency,
                func(&mut book),
            );
            trajectory.push(tuple);
            if adj_idxs.is_empty() {
                break;
            }
            adj_target = adj_idxs.pop_front().unwrap();
        } else {
            let fore_action = &actions[i];
            book.handle(fore_action).unwrap();
            i += 1;
        }
    }

    trajectory
}

#[pymodule]
fn nyse_py(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<TimeBasedQueueReplay>()?;
    m.add_class::<TimeBasedVolumeReplay>()?;

    // #[pyfn(m)]
    // fn preprocess(path: &str, out_dir: &str) {
    //     process_file(
    //         PathBuf::from_str(path).unwrap(),
    //         PathBuf::from_str(out_dir).unwrap(),
    //         false,
    //     );
    // }

    #[pyfn(m)]
    fn preprocess_meta(path_list: Vec<&str>, out_dir: &str) {
        process_file(
            path_list
                .into_iter()
                .map(|s| PathBuf::from_str(s).unwrap())
                .collect(),
            PathBuf::from_str(out_dir).unwrap(),
            true,
        );
    }

    #[pyfn(m)]
    fn preprocess(path_list: Vec<&str>, out_dir: &str) {
        process_file(
            path_list
                .into_iter()
                .map(|s| PathBuf::from_str(s).unwrap())
                .collect(),
            PathBuf::from_str(out_dir).unwrap(),
            false,
        );
    }
    #[pyfn(m)]
    fn create_trajectory_summaries(
        encoded_actions: PyReadonlyArray2<u64>,
    ) -> PyResult<Vec<TrajectorySummary>> {
        Ok(crate::summary::create_trajectory_summaries(encoded_actions))
    }

    #[pyfn(m)]
    fn compile_trajectory_with_volume_level<'py>(
        py: Python<'py>,
        encoded_actions: PyReadonlyArray2<u64>,
        indicies: Vec<u64>,
        latencies: Vec<u64>,
        level: usize,
        with_validation: bool,
        is_inclusive: bool,
    ) -> PyResult<Trajectory<'py, u64>> {
        Ok(compile_trajectory(
            py,
            encoded_actions,
            indicies,
            latencies,
            |book: &mut NyseOrderBook| book.level_summary(level),
            with_validation,
            is_inclusive,
        ))
    }

    #[pyfn(m)]
    fn compile_trajectory_with_volume_spread<'py>(
        py: Python<'py>,
        encoded_actions: PyReadonlyArray2<u64>,
        indicies: Vec<u64>,
        latencies: Vec<u64>,
        spread: u64,
        with_validation: bool,
        is_inclusive: bool,
    ) -> PyResult<Trajectory<'py, u64>> {
        Ok(compile_trajectory(
            py,
            encoded_actions,
            indicies,
            latencies,
            |book: &mut NyseOrderBook| book.spread_summary(spread),
            with_validation,
            is_inclusive,
        ))
    }

    #[pyfn(m)]
    fn compile_trajectory_with_queue_level<'py>(
        py: Python<'py>,
        encoded_actions: PyReadonlyArray2<u64>,
        indicies: Vec<u64>,
        latencies: Vec<u64>,
        level: usize,
        with_validation: bool,
        is_inclusive: bool,
    ) -> PyResult<Trajectory<'py, Vec<(u64, u64, u64)>>> {
        Ok(compile_trajectory(
            py,
            encoded_actions,
            indicies,
            latencies,
            |book: &mut NyseOrderBook| book.level_snapshot(level),
            with_validation,
            is_inclusive,
        ))
    }

    #[pyfn(m)]
    fn compile_trajectory_with_queue_spread<'py>(
        py: Python<'py>,
        encoded_actions: PyReadonlyArray2<u64>,
        indicies: Vec<u64>,
        latencies: Vec<u64>,
        spread: u64,
        with_validation: bool,
        is_inclusive: bool,
    ) -> PyResult<Trajectory<'py, Vec<(u64, u64, u64)>>> {
        Ok(compile_trajectory(
            py,
            encoded_actions,
            indicies,
            latencies,
            |book: &mut NyseOrderBook| book.spread_snapshot(spread),
            with_validation,
            is_inclusive,
        ))
    }

    Ok(())
}
