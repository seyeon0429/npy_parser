use serde::{Deserialize, Serialize};

// 36 = 10(0...9) + 26(A..Z)
pub type AlphaNumeric = RadixCode<36>;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct RadixCode<const R: u64>(u64);

impl<const R: u64> RadixCode<R> {
    pub fn new(str_code: &str) -> Option<Self> {
        Some(Self(code_to_num::<R>(str_code)?))
    }
}

fn code_to_num<const R: u64>(str_code: &str) -> Option<u64> {
    str_code.chars().into_iter().try_fold(0, |sum: u64, c| {
        sum.checked_mul(R)?
            .checked_add(c.to_digit(R as u32)? as u64)
    })
}

fn num_to_code<const R: u64>(mut num: u64) -> Option<String> {
    let mut container = Vec::new();

    while num > 0 {
        let remainder = (num % R) as u32;
        let mut character = std::char::from_digit(remainder, R as u32)?;
        character.make_ascii_uppercase();
        container.push(character);
        num /= R;
    }
    Some(container.into_iter().rev().collect())
}

impl<const R: u64> std::fmt::Display for RadixCode<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str_code = num_to_code::<R>(self.0).ok_or(std::fmt::Error)?;
        f.write_str(&str_code)
    }
}
