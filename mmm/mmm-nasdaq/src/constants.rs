// we want 1 minute interval data
const fn min_to_ns(min: u64) -> u64 {
    min * 60 * 1000000000
}

const fn sec_to_ns(sec: u64) -> u64 {
    sec * 1_000_000_000
}

const INTERVAL_SEC: u64 = 1;
pub const INTERVAL_NS: u64 = sec_to_ns(INTERVAL_SEC);

// start and end time
const START_TIME_MIN: u64 = 4 * 60; // premarket starts at 4:00
pub const START_TIME_NS: u64 = min_to_ns(START_TIME_MIN);
const END_TIME_MIN: u64 = 20 * 60 + 1; // afterhours ends at 20:00 but we add 1 min 
                                        // because nyse has messages after 20:00
pub const END_TIME_NS: u64 = min_to_ns(END_TIME_MIN);

const REG_START_TIME_MIN: u64 = 9 * 60 + 30; //regmarket starts at 9:30
pub const REG_START_TIME_NS: u64 = min_to_ns(REG_START_TIME_MIN);
const REG_END_TIME_MIN: u64 = 16 * 60; // regmarket ends at 16:00
pub const REG_END_TIME_NS: u64 = min_to_ns(REG_END_TIME_MIN);

// total number of intervals
pub const T_N: usize = (((END_TIME_NS - START_TIME_NS - 1) / INTERVAL_NS) + 1) as usize;
// start of regular market hours index
pub const R_N: usize = (((REG_START_TIME_NS - START_TIME_NS - 1) / INTERVAL_NS) + 1) as usize;
// end of regular market hours index
pub const P_N: usize = (((REG_END_TIME_NS - START_TIME_NS - 1) / INTERVAL_NS) + 1) as usize;
