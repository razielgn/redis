use redis::commands::{Bytes, Command};
use std::borrow::Cow;
use std::collections::HashMap;
use std::default::Default;

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
pub enum CommandReturn<'a> {
    Ok,
    Nil,
    Integer(i64),
    Size(usize),
    BulkString(Cow<'a, [u8]>),
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
            Command::Set { key, value } => self.set(key, value),
            Command::Get { key } => self.get(key),
            Command::Exists { keys } => self.exists(keys),
            Command::Del { keys } => self.del(keys),
            Command::Rename { key, new_key } => self.rename(key, new_key),
            Command::IncrBy { key, by } => self.incr_by(key, by),
        }
    }

    fn set(&mut self, key: Bytes<'a>, value: Bytes<'a>) -> CommandResult {
        let value = self
            .to_integer(value)
            .map_or_else(
                || Value::String(value.to_vec()),
                Value::Integer
            );

        self.memory.insert(key.to_vec(), value);

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
        match self.memory.remove(key) {
            Some(value) => {
                self.memory.insert(new_key.to_vec(), value);
                Ok(CommandReturn::Ok)
            }
            None =>
                Err(CommandError::NoSuchKey)
        }
    }

    fn incr_by(&mut self, key: Bytes<'a>, by: i64) -> CommandResult {
        if !self.memory.contains_key(key) {
            self.memory.insert(key.to_vec(), Value::Integer(by));
            return Ok(CommandReturn::Integer(by));
        }

        let value = self.memory.get_mut(key).unwrap();

        let outcome = match *value {
            Value::Integer(int) =>
                int.checked_add(by)
                   .ok_or(CommandError::IntegerOverflow),
            Value::String(ref s) if s.is_empty() =>
                Ok(by),
            _ =>
                Err(CommandError::NotAnInteger),
        };

        match outcome {
            Ok(int) => {
                *value = Value::Integer(int);
                Ok(CommandReturn::Integer(int))
            }
            Err(err) =>
                Err(err),
        }
    }

    fn to_integer(&self, bytes: Bytes<'a>) -> Option<i64> {
        let string = String::from_utf8_lossy(bytes);
        i64::from_str_radix(&string, 10).ok()
    }
}

#[cfg(test)]
mod test {
    use redis::commands::Command;
    use std::borrow::Cow;
    use super::{Database, CommandReturn, CommandError};

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
}
