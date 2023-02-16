use crate::parser::{parse_message, Body, Message, MessageStream};
use std::{collections::HashMap, vec};
use std::{
    error::Error,
    fs::File,
    io::{BufRead, BufReader, LineWriter, Write},
    path::Path,
};

fn testgen() {
    let target = "KAIIW";
    let path = "../sample/EQY_US_ARCA_IBF_9_20211022";
    // let channel_id = parse_channel_id(&path);
    let file = File::open(path).unwrap();
    // let reader=BufReader::new(file);
    let mut outfile = File::create("./KAIIW_processed1").unwrap();

    for message in MessageStream::<File>::from_file(path)
        .unwrap()
        .get_all_records()
    {
        let message = message.unwrap();
        if message.symbol.to_string().eq(target) {
            writeln!(outfile, "{:?}", message);
        }
    }
}

mod test {
    use super::*;

    #[test]
    fn gen_test() {
        testgen();
    }
}
