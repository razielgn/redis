#[macro_use]
extern crate nom;

use std::collections::HashMap;
use std::io::{self, Write};

use nom::{multispace, alphanumeric};

#[derive(PartialEq, Eq, Debug)]
pub enum Command<'a> {
    Set { key: &'a [u8], value: &'a [u8] },
    Get { key: &'a [u8] },
}

#[derive(Default, Debug)]
pub struct State {
    memory: HashMap<Vec<u8>, Vec<u8>>,
}

impl State {
    pub fn apply(self: &mut State, command: Command) -> Return {
        match command {
            Command::Set { key, value } => {
                let _ = self.memory.insert(
                    Vec::from(key),
                    Vec::from(value)
                );

                Return::Ok
            }
            Command::Get { key } => {
                match self.memory.get(key) {
                    Some(value) => Return::SimpleString(value),
                    None        => Return::Nil
                }
            }
        }
    }
}

#[derive(Eq, PartialEq, Debug)]
pub enum Error<'a> {
    UnknownCommand(&'a str),
    NotAnInteger,
}

#[derive(PartialEq, Eq, Debug)]
pub enum Return<'a> {
    Ok,
    Nil,
    SimpleString(&'a [u8]),
    Integer(i64),
    BulkString(&'a [u8]),
}

pub type CommandResult<'a> = Result<Return<'a>, Error<'a>>;

pub fn encode<T: Write>(result: &CommandResult, w: &mut T) -> io::Result<()> {
    match *result {
        Ok(ref ret) => {
            match *ret {
                Return::Ok =>
                    try!(write!(w, "+OK")),
                Return::Nil =>
                    try!(write!(w, "$-1")),
                Return::SimpleString(s) => {
                    try!(write!(w, "+"));
                    try!(w.write_all(s));
                }
                Return::BulkString(s) => {
                    try!(write!(w, "${}\r\n", s.len()));
                    try!(w.write_all(s));
                }
                Return::Integer(i) =>
                    try!(write!(w, ":{}", i)),
            }
        }
        Err(ref err) => {
        }
    }

    try!(write!(w, "\r\n"));
    Ok(())
}

#[cfg(test)]
mod resp {
    use super::{Return, encode};

    #[test]
    fn ok() {
        encodes_to(Return::Ok, "+OK\r\n");
    }

    #[test]
    fn nil() {
        encodes_to(Return::Nil, "$-1\r\n");
    }

    #[test]
    fn simple_string() {
        encodes_to(Return::SimpleString(b""), "+\r\n");
        encodes_to(Return::SimpleString(b"asd"), "+asd\r\n");
    }

    #[test]
    fn bulk_string() {
        encodes_to(Return::BulkString(b""), "$0\r\n\r\n");
        encodes_to(Return::BulkString(b"asd"), "$3\r\nasd\r\n");
    }

    #[test]
    fn integer() {
        encodes_to(Return::Integer(1238439), ":1238439\r\n");
        encodes_to(Return::Integer(-1238439), ":-1238439\r\n");
    }

    fn encodes_to(ret: Return, to: &str) {
        let mut output = Vec::new();

        assert!(encode(&Ok(ret), &mut output).is_ok());
        assert_eq!(to, String::from_utf8(output).unwrap());
    }
}

named!(pub parser<&[u8], Command>,
    alt!(
        chain!(
            tag!("GET") ~
            multispace ~
            key: alphanumeric ~
            multispace?,
            || { Command::Get { key: key } }
        )
     |  chain!(
            tag!("SET") ~
            multispace ~
            key: alphanumeric ~
            multispace ~
            value: alphanumeric ~
            multispace?,
            || { Command::Set { key: key, value: value } }
        )
    )
);

#[cfg(test)]
mod parser {
    use super::{parser, Command};
    use nom::IResult;

    #[test]
    fn _get() {
        let cmd = Command::Get { key: b"foo" };

        assert_eq!(
            IResult::Done(&[] as &[u8], cmd),
            parser(b"GET foo\n")
        );
    }

    #[test]
    fn _set() {
        let cmd = Command::Set { key: b"foo", value: b"bar" };

        assert_eq!(
            IResult::Done(&[] as &[u8], cmd),
            parser(b"SET foo bar\n")
        );
    }
}
