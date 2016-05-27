use std::ops;

pub type Bytes<'a> = &'a [u8];
pub type Range = ops::Range<i64>;

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum Command<'a> {
    Append { key: Bytes<'a>, value: Bytes<'a> },
    BitCount { key: Bytes<'a>, range: Option<Range> },
    DecrBy { key: Bytes<'a>, by: i64 },
    Del { keys: Vec<Bytes<'a>> },
    Exists { keys: Vec<Bytes<'a>> },
    Get { key: Bytes<'a> },
    IncrBy { key: Bytes<'a>, by: i64 },
    Rename { key: Bytes<'a>, new_key: Bytes<'a> },
    Set { key: Bytes<'a>, value: Bytes<'a> },
    Strlen { key: Bytes<'a> },
    Type { key: Bytes<'a> },
}
