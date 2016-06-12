#![allow(unknown_lints)]
#![allow(linkedlist)]

use redis::commands::{Bytes, Command, IntRange};
use std::borrow::Cow;
use std::collections::{HashMap, LinkedList};
use std::default::Default;
use std::ops::Range;

#[derive(Debug)]
enum Value {
    String(Vec<u8>),
    Integer(i64),
    List(LinkedList<Vec<u8>>),
}

#[derive(Eq, PartialEq, Debug)]
pub enum CommandError {
    UnknownCommand(Vec<u8>),
    BadCommandAryth(Vec<u8>),
    NoSuchKey,
    NotAnInteger,
    IntegerOverflow,
    WrongType,
}

#[derive(PartialEq, Eq, Debug)]
pub enum Type {
    None,
    String,
    List,
}

#[derive(PartialEq, Eq, Debug)]
pub enum CommandReturn<'a> {
    Ok,
    Nil,
    Integer(i64),
    Size(usize),
    BulkString(Cow<'a, [u8]>),
    Type(Type),
    Array(Vec<CommandReturn<'a>>),
}

pub type CommandResult<'a> = Result<CommandReturn<'a>, CommandError>;

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
            Command::LIndex { key, index } => self.lindex(key, index),
            Command::LLen { key } => self.llen(key),
            Command::LPop { key } => self.lpop(key),
            Command::LPush { key, values } => self.lpush(key, values),
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
            Some(_) =>
                Err(CommandError::WrongType),
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
        match self.memory.get(key) {
            Some(&Value::String(ref s)) =>
                Some(s.len()),
            Some(&Value::Integer(i)) =>
                Some(format!("{}", i).len()),
            Some(_) =>
                None,
            None =>
                Some(0),
        }.map_or(
            Err(CommandError::WrongType),
            |size| Ok(CommandReturn::Size(size))
        )
    }

    fn append(&mut self, key: Bytes<'a>, value: Bytes<'a>) -> CommandResult {
        if !self.memory.contains_key(key) {
            let _ = try!(self.set(key, value));
            return Ok(CommandReturn::Size(value.len()));
        }

        let old_value = self.memory.get_mut(key).unwrap();

        match *old_value {
            Value::Integer(int) => {
                let mut bytes = format!("{}", int).into_bytes();
                bytes.extend_from_slice(value);
                let len = bytes.len();
                *old_value = integer_or_string(&bytes);
                Some(len)
            }
            Value::String(ref mut s) => {
                let len = s.len() + value.len();
                s.extend_from_slice(value);
                Some(len)
            }
            _ =>
                None
        }.map_or(
            Err(CommandError::WrongType),
            |size| Ok(CommandReturn::Size(size))
        )
    }

    fn type_(&self, key: Bytes<'a>) -> CommandResult {
        match self.memory.get(key) {
            Some(&Value::String(..)) | Some(&Value::Integer(..)) =>
                Ok(CommandReturn::Type(Type::String)),
            Some(&Value::List(..)) =>
                Ok(CommandReturn::Type(Type::List)),
            None =>
                Ok(CommandReturn::Type(Type::None)),
        }
    }

    fn bit_count(&self, key: Bytes<'a>, range: Option<IntRange>) -> CommandResult {
        self.memory.get(key)
            .map_or(
                Ok(CommandReturn::Size(0)),
                |value| {
                    match *value {
                        Value::String(ref s) =>
                            Some(count_on_bits(s, range)),
                        Value::Integer(i) => {
                            let as_str = format!("{}", i);
                            Some(count_on_bits(as_str.as_bytes(), range))
                        }
                        _ =>
                            None
                    }.map_or(
                        Err(CommandError::WrongType),
                        |count| Ok(CommandReturn::Size(count))
                    )
                }
            )
    }

    fn get_range(&self, key: Bytes<'a>, range: IntRange) -> CommandResult {
        match self.memory.get(key) {
            Some(&Value::String(ref s)) => {
                let range = range_calc(range, s.len())
                    .map_or(
                        Cow::Borrowed(&b""[..]),
                        |range| Cow::Borrowed(&s[range])
                    );

                Ok(CommandReturn::BulkString(range))
            }
            Some(&Value::Integer(n)) => {
                let s = format!("{}", n);
                let range = range_calc(range, s.len())
                    .map_or(
                        Cow::Borrowed(&b""[..]),
                        |range| {
                            let bytes = s[range].as_bytes().to_vec();
                            Cow::Owned(bytes)
                        }
                    );

                Ok(CommandReturn::BulkString(range))
            }
            Some(_) =>
                Err(CommandError::WrongType),
            None =>
                Ok(CommandReturn::BulkString(Cow::Borrowed(&b""[..]))),
        }
    }

    fn lpush(&mut self, key: Bytes<'a>, values: Vec<Bytes<'a>>) -> CommandResult {
        if !self.memory.contains_key(key) {
            let mut list = LinkedList::new();
            push_to_list(&mut list, &values);

            self.insert(key, Value::List(list));
            return Ok(CommandReturn::Size(values.len()));
        }

        let value = self.memory.get_mut(key).unwrap();

        if let Value::List(ref mut list) = *value {
            push_to_list(list, &values);
            Ok(CommandReturn::Size(list.len()))
        } else {
            Err(CommandError::WrongType)
        }
    }

    fn llen(&mut self, key: Bytes<'a>) -> CommandResult {
        match self.memory.get(key) {
            Some(&Value::List(ref list)) =>
                Ok(CommandReturn::Size(list.len())),
            Some(_) =>
                Err(CommandError::WrongType),
            None =>
                Ok(CommandReturn::Size(0)),
        }
    }

    fn lindex(&self, key: Bytes<'a>, index: i64) -> CommandResult {
        match self.memory.get(key) {
            Some(&Value::List(ref list)) =>
                pos_calc(index, list.len())
                    .and_then(|i| list.iter().nth(i))
                    .map_or(
                        Ok(CommandReturn::Nil),
                        |value| {
                            Ok(CommandReturn::BulkString(Cow::Borrowed(value)))
                        }
                    ),
            Some(_) =>
                Err(CommandError::WrongType),
            None =>
                Ok(CommandReturn::Nil),
        }
    }

    fn lpop(&mut self, key: Bytes<'a>) -> CommandResult {
        match self.memory.get_mut(key) {
            Some(&mut Value::List(ref mut list)) =>
                match list.pop_front() {
                    Some(value) =>
                        Ok(CommandReturn::BulkString(Cow::Owned(value))),
                    None =>
                        Ok(CommandReturn::Nil),
                },
            Some(_) =>
                Err(CommandError::WrongType),
            None =>
                Ok(CommandReturn::Nil),
        }
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

fn pos_calc(index: i64, len: usize) -> Option<usize> {
    if index >= 0 {
        let index = index as usize;

        if index >= len {
            None
        } else {
            Some(index)
        }
    } else {
        len.checked_sub(index.abs() as usize)
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

fn count_on_bits(slice: &[u8], range: Option<IntRange>) -> usize {
    let folder = |sum, c: &u8| sum + c.count_ones() as usize;

    match range {
        Some(range) =>
            range_calc(range, slice.len())
                .map_or(0, |range| {
                    slice.iter()
                        .skip(range.start)
                        .take(range.end - range.start)
                        .fold(0, folder)
                }),
        None =>
            slice.iter().fold(0, folder),
    }
}

fn push_to_list(list: &mut LinkedList<Vec<u8>>, values: &[Bytes]) {
    for v in values {
        list.push_front(v.to_vec());
    }
}

#[cfg(test)]
mod test {
    use redis::commands::Command;
    use std::borrow::{Cow, Borrow};
    use std::ops::Range;
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
    fn get_wrong_type() {
        let mut db = Database::new();

        db.apply(Command::LPush { key: b"foo", values: vec![b"a"] }).unwrap();

        assert_eq!(
            Err(CommandError::WrongType),
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

    #[quickcheck]
    fn rename_non_existing(key: Vec<u8>, new_key: Vec<u8>) {
        let mut db = Database::new();

        assert_eq!(
            Err(CommandError::NoSuchKey),
            db.apply(Command::Rename { key: &key, new_key: &new_key })
        );
    }

    #[quickcheck]
    fn rename(key: Vec<u8>, new_key: Vec<u8>, value: Vec<u8>) {
        let mut db = Database::new();
        db.apply(Command::Set { key: &key, value: &value }).unwrap();

        assert_eq!(
            Ok(CommandReturn::Ok),
            db.apply(Command::Rename { key: &key, new_key: &new_key })
        );

        if key != new_key {
            assert_eq!(
                Ok(CommandReturn::Nil),
                db.apply(Command::Get { key: &key })
            );
        }

        assert_eq!(
            Ok(CommandReturn::BulkString(Cow::Borrowed(&value))),
            db.apply(Command::Get { key: &new_key })
        );
    }

    #[quickcheck]
    fn strlen(key: Vec<u8>, value: Vec<u8>) {
        let mut db = Database::new();

        db.apply(Command::Set { key: &key, value: &value }).unwrap();

        assert_eq!(
            Ok(CommandReturn::Size(value.len())),
            db.apply(Command::Strlen { key: &key })
        );
    }

    #[test]
    fn strlen_wrong_type() {
        let mut db = Database::new();

        db.apply(Command::LPush { key: b"foo", values: vec![b"a"] }).unwrap();

        assert_eq!(
            Err(CommandError::WrongType),
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

    #[quickcheck]
    fn append_str(mut value: Vec<u8>, mut append: Vec<u8>) {
        let mut db = Database::new();

        assert_eq!(
            Ok(CommandReturn::Size(value.len())),
            db.apply(Command::Append { key: b"foo", value: &value })
        );

        assert_eq!(
            Ok(CommandReturn::BulkString(Cow::Borrowed(&value))),
            db.apply(Command::Get { key: b"foo" })
        );

        assert_eq!(
            Ok(CommandReturn::Size(value.len() + append.len())),
            db.apply(Command::Append { key: b"foo", value: &append })
        );

        value.append(&mut append);

        assert_eq!(
            Ok(CommandReturn::BulkString(Cow::Borrowed(&value))),
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
    fn append_wrong_type() {
        let mut db = Database::new();

        db.apply(Command::LPush { key: b"foo", values: vec![b"a"] }).unwrap();

        assert_eq!(
            Err(CommandError::WrongType),
            db.apply(Command::Append { key: b"foo", value: b"bar" })
        );
    }

    #[test]
    fn type_() {
        let mut db = Database::new();

        db.apply(Command::Set { key: b"foo", value: b"bar" }).unwrap();
        db.apply(Command::Set { key: b"bar", value: b"1" }).unwrap();
        db.apply(Command::LPush { key: b"kak", values: vec![b"1"] }).unwrap();

        assert_eq!(
            Ok(CommandReturn::Type(Type::String)),
            db.apply(Command::Type { key: b"foo" })
        );

        assert_eq!(
            Ok(CommandReturn::Type(Type::String)),
            db.apply(Command::Type { key: b"bar" })
        );

        assert_eq!(
            Ok(CommandReturn::Type(Type::List)),
            db.apply(Command::Type { key: b"kak" })
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
    fn bit_count_range() {
        let mut db = Database::new();

        db.apply(Command::Set { key: b"foo", value: b"Lorem ipsum" }).unwrap();

        let examples = vec![
            (0..0, 3),
            (0..5, 23),
            (0..-1, 45),
            (0..-12, 3),
            (0..-13, 3),
            (-1..-5, 0),
            (-5..-1, 22),
            (-12..0, 3),
        ];

        for (range, size) in examples {
            println!("range: {:?}, size: {:?}", range, size);

            assert_eq!(
                Ok(CommandReturn::Size(size)),
                db.apply(Command::BitCount { key: b"foo", range: Some(range) })
            );
        }
    }

    #[test]
    fn bitcount_wrong_type() {
        let mut db = Database::new();

        db.apply(Command::LPush { key: b"foo", values: vec![b"a"] }).unwrap();

        assert_eq!(
            Err(CommandError::WrongType),
            db.apply(Command::BitCount { key: b"foo", range: None })
        );
    }

    #[quickcheck]
    fn get_range_missing(range: Range<i64>) {
        let mut db = Database::new();

        assert_eq!(
            Ok(CommandReturn::BulkString(Cow::Borrowed(b""))),
            db.apply(Command::GetRange { key: b"foo", range: range })
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
    fn get_range_wrong_type() {
        let mut db = Database::new();

        db.apply(Command::LPush { key: b"foo", values: vec![b"a"] }).unwrap();

        assert_eq!(
            Err(CommandError::WrongType),
            db.apply(Command::GetRange { key: b"foo", range: 0..0 })
        );
    }

    #[quickcheck]
    fn get_range_string_qc(value: Vec<u8>, range: Range<i64>) -> bool {
        let mut db = Database::new();

        db.apply(Command::Set { key: b"foo", value: &value }).unwrap();

        if let Ok(CommandReturn::BulkString(s)) =
            db.apply(Command::GetRange { key: b"foo", range: range })
        {
            contains(&value, s.borrow())
        } else {
            false
        }
    }

    #[quickcheck]
    fn get_range_empty_string(range: Range<i64>) {
        let mut db = Database::new();

        db.apply(Command::Set { key: b"foo", value: b"" }).unwrap();

        assert_eq!(
            Ok(CommandReturn::BulkString(Cow::Borrowed(b""))),
            db.apply(Command::GetRange { key: b"foo", range: range })
        );
    }

    #[test]
    fn lpush() {
        let mut db = Database::new();

        assert_eq!(
            Ok(CommandReturn::Size(2)),
            db.apply(Command::LPush {
                key: b"foo",
                values: vec![b"0", b"1"],
            })
        );

        assert_eq!(
            Ok(CommandReturn::Size(3)),
            db.apply(Command::LPush {
                key: b"foo",
                values: vec![b"2"],
            })
        );

        assert_eq!(
            Ok(CommandReturn::BulkString(Cow::Borrowed(b"2"))),
            db.apply(Command::LIndex { key: b"foo", index: 0 })
        );

        assert_eq!(
            Ok(CommandReturn::BulkString(Cow::Borrowed(b"1"))),
            db.apply(Command::LIndex { key: b"foo", index: 1 })
        );

        assert_eq!(
            Ok(CommandReturn::BulkString(Cow::Borrowed(b"0"))),
            db.apply(Command::LIndex { key: b"foo", index: 2 })
        );
    }

    #[test]
    fn lpush_wrong_type() {
        let mut db = Database::new();

        db.apply(Command::Set { key: b"foo", value: b"bar" }).unwrap();

        assert_eq!(
            Err(CommandError::WrongType),
            db.apply(Command::LPush { key: b"foo", values: vec![b"bar"] })
        );
    }

    #[quickcheck]
    fn llen(values: Vec<Vec<u8>>) {
        let mut db = Database::new();

        db.apply(
            Command::LPush {
                key: b"foo",
                values: values.iter()
                    .map(Vec::as_slice)
                    .collect(),
            }
        ).unwrap();

        assert_eq!(
            Ok(CommandReturn::Size(values.len())),
            db.apply(Command::LLen { key: b"foo" })
        );
    }

    #[test]
    fn llen_missing_key() {
        let mut db = Database::new();

        assert_eq!(
            Ok(CommandReturn::Size(0)),
            db.apply(Command::LLen { key: b"foo" })
        );
    }

    #[test]
    fn llen_wrong_type() {
        let mut db = Database::new();

        db.apply(Command::Set { key: b"foo", value: b"bar" }).unwrap();

        assert_eq!(
            Err(CommandError::WrongType),
            db.apply(Command::LLen { key: b"foo" })
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

    #[quickcheck]
    fn lindex_missing_key(key: Vec<u8>, index: i64) {
        let mut db = Database::new();

        assert_eq!(
            Ok(CommandReturn::Nil),
            db.apply(Command::LIndex { key: &key, index: index })
        );
    }

    #[test]
    fn lindex() {
        let mut db = Database::new();

        db.apply(Command::LPush {
            key: b"foo",
            values: vec![b"c", b"b", b"a"],
        }).unwrap();

        let table = vec![
            (-4, CommandReturn::Nil),
            (-3, CommandReturn::BulkString(Cow::Borrowed(b"a"))),
            (-2, CommandReturn::BulkString(Cow::Borrowed(b"b"))),
            (-1, CommandReturn::BulkString(Cow::Borrowed(b"c"))),
            ( 0, CommandReturn::BulkString(Cow::Borrowed(b"a"))),
            ( 1, CommandReturn::BulkString(Cow::Borrowed(b"b"))),
            ( 2, CommandReturn::BulkString(Cow::Borrowed(b"c"))),
            ( 3, CommandReturn::Nil),
        ];

        for (i, ret) in table {
            println!("{:?} {:?}", i, ret);

            assert_eq!(
                Ok(ret),
                db.apply(Command::LIndex { key: b"foo", index: i })
            );
        }
    }

    #[test]
    fn lindex_wrong_type() {
        let mut db = Database::new();

        db.apply(Command::Set { key: b"foo", value: b"bar" }).unwrap();

        assert_eq!(
            Err(CommandError::WrongType),
            db.apply(Command::LIndex { key: b"foo", index: 0 })
        );
    }

    #[test]
    fn lpop() {
        let mut db = Database::new();

        db.apply(Command::LPush {
            key: b"foo",
            values: vec![b"a", b"b", b"c"],
        }).unwrap();

        assert_eq!(
            Ok(CommandReturn::BulkString(Cow::Borrowed(b"c"))),
            db.apply(Command::LPop { key: b"foo" })
        );

        assert_eq!(
            Ok(CommandReturn::BulkString(Cow::Borrowed(b"b"))),
            db.apply(Command::LPop { key: b"foo" })
        );

        assert_eq!(
            Ok(CommandReturn::BulkString(Cow::Borrowed(b"a"))),
            db.apply(Command::LPop { key: b"foo" })
        );

        assert_eq!(
            Ok(CommandReturn::Nil),
            db.apply(Command::LPop { key: b"foo" })
        );
    }

    #[test]
    fn lpop_wrong_type() {
        let mut db = Database::new();

        db.apply(Command::Set { key: b"foo", value: b"bar" }).unwrap();

        assert_eq!(
            Err(CommandError::WrongType),
            db.apply(Command::LPop { key: b"foo" })
        );
    }

    fn contains<T: PartialEq + Eq>(a: &[T], b: &[T]) -> bool {
        if a.len() < b.len() {
            return false;
        }

        if starts_with(a, b) {
            return true;
        }

        contains(&a[1..], b)
    }

    fn starts_with<T: PartialEq + Eq>(a: &[T], b: &[T]) -> bool {
        if a.len() < b.len() {
            return false;
        }

        a.iter().zip(b.iter()).all(|(x, y)| x == y)
    }
}
