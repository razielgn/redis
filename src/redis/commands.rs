pub type Bytes<'a> = &'a [u8];

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum Command<'a> {
    Set { key: Bytes<'a>, value: Bytes<'a> },
    Get { key: Bytes<'a> },
    Exists { keys: Vec<Bytes<'a>> },
    Del { keys: Vec<Bytes<'a>> },
    Rename { key: Bytes<'a>, new_key: Bytes<'a> },
    IncrBy { key: Bytes<'a>, by: i64 },
    DecrBy { key: Bytes<'a>, by: i64 },
    Strlen { key: Bytes<'a> },
    Append { key: Bytes<'a>, value: Bytes<'a> },
}
