use redis::commands::{Bytes, Command, IntRange};
use std::borrow::Cow;
use std::collections::HashMap;
use std::default::Default;
use std::ops::Range;

#[derive(Debug)]
enum Value {
    String(Vec<u8>),
    Integer(i64),
}

#[derive(Eq, PartialEq, Debug)]
pub enum CommandError<'a> {
    UnknownCommand(Bytes<'a>),
    NoSuchKey,
    NotAnInteger,
    IntegerOverflow,
}

#[derive(PartialEq, Eq, Debug)]
pub enum Type {
    None,
    String,
}

#[derive(PartialEq, Eq, Debug)]
pub enum CommandReturn<'a> {
    Ok,
    Nil,
    Integer(i64),
    Size(usize),
    BulkString(Cow<'a, [u8]>),
    Type(Type),
}

pub type CommandResult<'a> = Result<CommandReturn<'a>, CommandError<'a>>;

#[derive(Default, Debug)]
pub struct Database {
    memory: HashMap<Vec<u8>, Value>,
}

impl<'a> Database {
    pub fn new() -> Self { Self::default() }

    pub fn apply(&mut self, command: Command) -> CommandResult {
        match command {
            Command::Append { key, value } => self.append(key, value),
            Command::BitCount { key, range } => self.bit_count(key, range),
            Command::DecrBy { key, by } => self.incr_by(key, -by),
            Command::Del { keys } => self.del(keys),
            Command::Exists { keys } => self.exists(keys),
            Command::Get { key } => self.get(key),
            Command::GetRange { key, range } => self.get_range(key, range),
            Command::IncrBy { key, by } => self.incr_by(key, by),
            Command::Rename { key, new_key } => self.rename(key, new_key),
            Command::Set { key, value } => self.set(key, value),
            Command::Strlen { key } => self.strlen(key),
            Command::Type { key } => self.type_(key),
        }
    }

    fn insert(&mut self, key: Bytes<'a>, value: Value) {
        self.memory.insert(key.to_vec(), value);
    }

    fn set(&mut self, key: Bytes<'a>, bytes: Bytes<'a>) -> CommandResult {
        self.insert(key, integer_or_string(bytes));
        Ok(CommandReturn::Ok)
    }

    fn get(&self, key: Bytes<'a>) -> CommandResult {
        match self.memory.get(key) {
            Some(&Value::String(ref value)) =>
                Ok(CommandReturn::BulkString(Cow::Borrowed(value))),
            Some(&Value::Integer(int)) => {
                let bytes = format!("{}", int).into_bytes();
                Ok(CommandReturn::BulkString(Cow::Owned(bytes)))
            }
            None =>
                Ok(CommandReturn::Nil),
        }
    }

    fn exists(&self, keys: Vec<Bytes<'a>>) -> CommandResult {
        let sum = keys.into_iter()
            .filter(|key| self.memory.contains_key(*key))
            .count();

        Ok(CommandReturn::Size(sum))
    }

    fn del(&mut self, keys: Vec<Bytes<'a>>) -> CommandResult {
        let sum = keys.into_iter()
            .filter(|key| {
                self.memory
                    .remove(*key)
                    .map_or(false, |_| true)
            })
            .count();

        Ok(CommandReturn::Size(sum))
    }

    fn rename(&mut self, key: Bytes<'a>, new_key: Bytes<'a>) -> CommandResult {
        self.memory.remove(key)
            .ok_or(CommandError::NoSuchKey)
            .map(|value| {
                self.insert(new_key, value);
                CommandReturn::Ok
            })
    }

    fn incr_by(&mut self, key: Bytes<'a>, by: i64) -> CommandResult {
        if !self.memory.contains_key(key) {
            self.insert(key, Value::Integer(by));
            return Ok(CommandReturn::Integer(by));
        }

        let value = self.memory.get_mut(key).unwrap();

        match *value {
            Value::Integer(int) =>
                int.checked_add(by)
                   .ok_or(CommandError::IntegerOverflow),
            Value::String(ref s) if s.is_empty() =>
                Ok(by),
            _ =>
                Err(CommandError::NotAnInteger),
        }.map(|int| {
            *value = Value::Integer(int);
            CommandReturn::Integer(int)
        })
    }

    fn strlen(&self, key: Bytes<'a>) -> CommandResult {
        let size = match self.memory.get(key) {
            Some(&Value::String(ref s)) =>
                s.len(),
            Some(&Value::Integer(i)) =>
                format!("{}", i).len(),
            None =>
                0,
        };

        Ok(CommandReturn::Size(size))
    }

    fn append(&mut self, key: Bytes<'a>, value: Bytes<'a>) -> CommandResult {
        if !self.memory.contains_key(key) {
            let _ = try!(self.set(key, value));
            return Ok(CommandReturn::Size(value.len()));
        }

        let old_value = self.memory.get_mut(key).unwrap();

        let size = match *old_value {
            Value::Integer(int) => {
                let mut bytes = format!("{}", int).into_bytes();
                bytes.extend_from_slice(value);
                let len = bytes.len();
                *old_value = integer_or_string(&bytes);
                len
            }
            Value::String(ref mut s) => {
                let len = s.len() + value.len();
                s.extend_from_slice(value);
                len
            }
        };

        Ok(CommandReturn::Size(size))
    }

    fn type_(&self, key: Bytes<'a>) -> CommandResult {
        match self.memory.get(key) {
            Some(&Value::String(..)) | Some(&Value::Integer(..)) =>
                Ok(CommandReturn::Type(Type::String)),
            None =>
                Ok(CommandReturn::Type(Type::None)),
        }
    }

    fn bit_count(&self, key: Bytes<'a>, _range: Option<IntRange>) -> CommandResult {
        let count_bits = |slice: &[u8]| -> usize {
            slice.iter().fold(0, |acc, c| acc + c.count_ones() as usize)
        };

        let ret = self.memory.get(key)
            .map_or(
                CommandReturn::Size(0),
                |value| {
                    let count = match *value {
                        Value::String(ref s) =>
                            count_bits(s),
                        Value::Integer(i) => {
                            let as_str = format!("{}", i);
                            count_bits(as_str.as_bytes())
                        }
                    };

                    CommandReturn::Size(count)
                }
            );

        Ok(ret)
    }

    fn get_range(&self, key: Bytes<'a>, range: IntRange) -> CommandResult {
        let s = self.memory.get(key)
            .map_or(
                Cow::Borrowed(&b""[..]),
                |value| {
                    match *value {
                        Value::String(ref s) => {
                            match range_calc(range, s.len()) {
                                Some(range) =>
                                    Cow::Borrowed(&s[range]),
                                None =>
                                    Cow::Borrowed(b""),
                            }
                        }
                        Value::Integer(n) => {
                            let s = format!("{}", n);

                            match range_calc(range, s.len()) {
                                Some(range) =>
                                    Cow::Owned(s[range].as_bytes().to_vec()),
                                None =>
                                    Cow::Borrowed(b""),
                            }
                        }
                    }
                }
            );

        Ok(CommandReturn::BulkString(s))
    }
}

fn range_calc(r: IntRange, len: usize) -> Option<Range<usize>> {
    let start =
        if r.start < 0 {
            len.checked_sub(r.start.abs() as usize).unwrap_or(0)
        } else {
            r.start.abs() as usize
        };

    let mut end =
        if r.end < 0 {
            len.checked_sub(r.end.abs() as usize - 1).unwrap_or(0)
        } else {
            r.end.abs() as usize
        };

    if end >= len {
        end = len.checked_sub(1).unwrap_or(0);
    }

    if start > end || len == 0 {
        None
    } else {
        Some(start .. end + 1)
    }
}

fn integer_or_string(bytes: Bytes) -> Value {
    let string = String::from_utf8_lossy(bytes);
    i64::from_str_radix(&string, 10)
        .ok()
        .map_or_else(
            || Value::String(bytes.to_vec()),
            Value::Integer
        )
}

#[cfg(test)]
mod test {
    use redis::commands::Command;
    use std::borrow::Cow;
    use super::{Database, CommandReturn, CommandError, Type};

    #[test]
    fn get_and_set() {
        let mut db = Database::new();

        assert_eq!(
            Ok(CommandReturn::Nil),
            db.apply(Command::Get { key: b"foo" })
        );

        assert_eq!(
            Ok(CommandReturn::Ok),
            db.apply(Command::Set { key: b"foo", value: b"bar" })
        );

        assert_eq!(
            Ok(CommandReturn::BulkString(Cow::Borrowed(b"bar"))),
            db.apply(Command::Get { key: b"foo" })
        );
    }

    #[test]
    fn exists() {
        let mut db = Database::new();

        assert_eq!(
            Ok(CommandReturn::Size(0)),
            db.apply(Command::Exists { keys: vec!(b"foo", b"bar", b"baz") })
        );

        db.apply(Command::Set { key: b"foo", value: b"foo" }).unwrap();
        db.apply(Command::Set { key: b"baz", value: b"baz" }).unwrap();

        assert_eq!(
            Ok(CommandReturn::Size(2)),
            db.apply(Command::Exists { keys: vec!(b"foo", b"bar", b"baz") })
        );
    }

    #[test]
    fn del() {
        let mut db = Database::new();

        assert_eq!(
            Ok(CommandReturn::Size(0)),
            db.apply(Command::Del { keys: vec!(b"foo", b"bar", b"baz") })
        );

        db.apply(Command::Set { key: b"foo", value: b"foo" }).unwrap();
        db.apply(Command::Set { key: b"bar", value: b"bar" }).unwrap();
        db.apply(Command::Set { key: b"baz", value: b"baz" }).unwrap();

        assert_eq!(
            Ok(CommandReturn::Size(2)),
            db.apply(Command::Del { keys: vec!(b"foo", b"baz") })
        );

        assert_eq!(
            Ok(CommandReturn::Size(1)),
            db.apply(Command::Exists { keys: vec!(b"foo", b"bar", b"baz") })
        );
    }

    #[test]
    fn rename_non_existing() {
        let mut db = Database::new();

        assert_eq!(
            Err(CommandError::NoSuchKey),
            db.apply(Command::Rename { key: b"foo", new_key: b"bar" })
        );
    }

    #[test]
    fn rename() {
        let mut db = Database::new();
        db.apply(Command::Set { key: b"foo", value: b"foo" }).unwrap();

        assert_eq!(
            Ok(CommandReturn::Ok),
            db.apply(Command::Rename { key: b"foo", new_key: b"bar" })
        );

        assert_eq!(
            Ok(CommandReturn::Nil),
            db.apply(Command::Get { key: b"foo" })
        );

        assert_eq!(
            Ok(CommandReturn::BulkString(Cow::Borrowed(b"foo"))),
            db.apply(Command::Get { key: b"bar" })
        );
    }

    #[test]
    fn strlen() {
        let mut db = Database::new();

        assert_eq!(
            Ok(CommandReturn::Size(0)),
            db.apply(Command::Strlen { key: b"foo" })
        );

        db.apply(Command::Set { key: b"foo", value: b"foo" }).unwrap();

        assert_eq!(
            Ok(CommandReturn::Size(3)),
            db.apply(Command::Strlen { key: b"foo" })
        );

        db.apply(Command::Set { key: b"foo", value: b"-9999" }).unwrap();

        assert_eq!(
            Ok(CommandReturn::Size(5)),
            db.apply(Command::Strlen { key: b"foo" })
        );
    }

    #[test]
    fn incr_by_empty_string() {
        let mut db = Database::new();
        db.apply(Command::Set { key: b"bar", value: b"" }).unwrap();

        assert_eq!(
            Ok(CommandReturn::Integer(1)),
            db.apply(Command::IncrBy { key: b"bar", by: 1 })
        );
    }

    #[test]
    fn incr_by_non_existing() {
        let mut db = Database::new();

        assert_eq!(
            Ok(CommandReturn::Integer(1)),
            db.apply(Command::IncrBy { key: b"foo", by: 1 })
        );

        assert_eq!(
            Ok(CommandReturn::BulkString(Cow::Borrowed(b"1"))),
            db.apply(Command::Get { key: b"foo" })
        );
    }

    #[test]
    fn incr_by_overflow() {
        let mut db = Database::new();

        assert_eq!(
            Ok(CommandReturn::Ok),
            db.apply(Command::Set { key: b"foo", value: b"9223372036854775807" })
        );

        assert_eq!(
            Err(CommandError::IntegerOverflow),
            db.apply(Command::IncrBy { key: b"foo", by: 1 })
        );
    }

    #[test]
    fn incr_by_not_integer() {
        let mut db = Database::new();

        db.apply(Command::Set { key: b"baz", value: b"nope" }).unwrap();

        assert_eq!(
            Err(CommandError::NotAnInteger),
            db.apply(Command::IncrBy { key: b"baz", by: 1 })
        );
    }

    #[test]
    fn decr_by_empty_string() {
        let mut db = Database::new();
        db.apply(Command::Set { key: b"bar", value: b"" }).unwrap();

        assert_eq!(
            Ok(CommandReturn::Integer(-1)),
            db.apply(Command::DecrBy { key: b"bar", by: 1 })
        );
    }

    #[test]
    fn decr_by_non_existing() {
        let mut db = Database::new();

        assert_eq!(
            Ok(CommandReturn::Integer(-1)),
            db.apply(Command::DecrBy { key: b"foo", by: 1 })
        );

        assert_eq!(
            Ok(CommandReturn::BulkString(Cow::Borrowed(b"-1"))),
            db.apply(Command::Get { key: b"foo" })
        );
    }

    #[test]
    fn decr_by_overflow() {
        let mut db = Database::new();

        assert_eq!(
            Ok(CommandReturn::Ok),
            db.apply(Command::Set { key: b"foo", value: b"-9223372036854775808" })
        );

        assert_eq!(
            Err(CommandError::IntegerOverflow),
            db.apply(Command::DecrBy { key: b"foo", by: 1 })
        );
    }

    #[test]
    fn decr_by_not_integer() {
        let mut db = Database::new();

        db.apply(Command::Set { key: b"baz", value: b"nope" }).unwrap();

        assert_eq!(
            Err(CommandError::NotAnInteger),
            db.apply(Command::DecrBy { key: b"baz", by: 1 })
        );
    }

    #[test]
    fn append_str() {
        let mut db = Database::new();

        assert_eq!(
            Ok(CommandReturn::Size(3)),
            db.apply(Command::Append { key: b"foo", value: b"bar" })
        );

        assert_eq!(
            Ok(CommandReturn::BulkString(Cow::Borrowed(b"bar"))),
            db.apply(Command::Get { key: b"foo" })
        );

        assert_eq!(
            Ok(CommandReturn::Size(6)),
            db.apply(Command::Append { key: b"foo", value: b"bar" })
        );

        assert_eq!(
            Ok(CommandReturn::BulkString(Cow::Borrowed(b"barbar"))),
            db.apply(Command::Get { key: b"foo" })
        );
    }

    #[test]
    fn append_int() {
        let mut db = Database::new();

        assert_eq!(
            Ok(CommandReturn::Size(1)),
            db.apply(Command::Append { key: b"foo", value: b"5" })
        );

        assert_eq!(
            Ok(CommandReturn::Integer(6)),
            db.apply(Command::IncrBy { key: b"foo", by: 1 })
        );

        assert_eq!(
            Ok(CommandReturn::Size(3)),
            db.apply(Command::Append { key: b"foo", value: b"28" })
        );

        assert_eq!(
            Ok(CommandReturn::Integer(629)),
            db.apply(Command::IncrBy { key: b"foo", by: 1 })
        );
    }

    #[test]
    fn type_() {
        let mut db = Database::new();

        db.apply(Command::Set { key: b"foo", value: b"bar" }).unwrap();
        db.apply(Command::Set { key: b"bar", value: b"1" }).unwrap();

        assert_eq!(
            Ok(CommandReturn::Type(Type::String)),
            db.apply(Command::Type { key: b"foo" })
        );

        assert_eq!(
            Ok(CommandReturn::Type(Type::String)),
            db.apply(Command::Type { key: b"bar" })
        );

        assert_eq!(
            Ok(CommandReturn::Type(Type::None)),
            db.apply(Command::Type { key: b"baz" })
        );
    }

    #[test]
    fn bit_count() {
        let mut db = Database::new();

        assert_eq!(
            Ok(CommandReturn::Size(0)),
            db.apply(Command::BitCount { key: b"foo", range: None })
        );

        db.apply(Command::Set { key: b"foo", value: b"bar" }).unwrap();

        assert_eq!(
            Ok(CommandReturn::Size(10)),
            db.apply(Command::BitCount { key: b"foo", range: None })
        );

        db.apply(Command::Set { key: b"foo", value: b"1234934" }).unwrap();

        assert_eq!(
            Ok(CommandReturn::Size(24)),
            db.apply(Command::BitCount { key: b"foo", range: None })
        );

        db.apply(Command::Set { key: b"foo", value: b"-1234934" }).unwrap();

        assert_eq!(
            Ok(CommandReturn::Size(28)),
            db.apply(Command::BitCount { key: b"foo", range: None })
        );
    }

    #[test]
    fn get_range_missing() {
        let mut db = Database::new();

        assert_eq!(
            Ok(CommandReturn::BulkString(Cow::Borrowed(b""))),
            db.apply(Command::GetRange { key: b"foo", range: 0..0 })
        );
    }

    #[test]
    fn get_range_string() {
        let mut db = Database::new();

        db.apply(Command::Set { key: b"foo", value: b"Lorem ipsum" }).unwrap();

        let examples = vec![
            (0..0, &b"L"[..]),
            (0..5, &b"Lorem "[..]),
            (0..-1, &b"Lorem ipsum"[..]),
            (0..-12, &b"L"[..]),
            (0..-13, &b"L"[..]),
            (-1..-5, &b""[..]),
            (-1..-5, &b""[..]),
            (-5..-1, &b"ipsum"[..]),
            (-12..0, &b"L"[..]),
        ];

        for (range, result) in examples {
            assert_eq!(
                Ok(CommandReturn::BulkString(Cow::Borrowed(result))),
                db.apply(Command::GetRange { key: b"foo", range: range })
            );
        }
    }

    #[test]
    fn get_range_empty_string() {
        let mut db = Database::new();

        db.apply(Command::Set { key: b"foo", value: b"" }).unwrap();

        assert_eq!(
            Ok(CommandReturn::BulkString(Cow::Borrowed(b""))),
            db.apply(Command::GetRange { key: b"foo", range: 5..230 })
        );
    }

    #[test]
    fn get_range_number() {
        let mut db = Database::new();

        db.apply(Command::Set { key: b"foo", value: b"-1234567890" }).unwrap();

        let examples = vec![
            (0..0, &b"-"[..]),
            (0..5, &b"-12345"[..]),
            (0..-1, &b"-1234567890"[..]),
            (0..-12, &b"-"[..]),
            (0..-13, &b"-"[..]),
            (-1..-5, &b""[..]),
            (-1..-5, &b""[..]),
            (-5..-1, &b"67890"[..]),
            (-12..0, &b"-"[..]),
        ];

        for (range, result) in examples {
            assert_eq!(
                Ok(CommandReturn::BulkString(Cow::Borrowed(result))),
                db.apply(Command::GetRange { key: b"foo", range: range })
            );
        }
    }
}
