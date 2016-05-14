#[macro_use]
extern crate nom;

use std::collections::HashMap;
use std::default::Default;
use std::io::{self, Write};

use nom::{multispace};

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum Command<'a> {
    Set { key: &'a [u8], value: &'a [u8] },
    Get { key: &'a [u8] },
    Exists { key: &'a [u8] },
}

#[derive(Default, Debug)]
pub struct State {
    memory: HashMap<Vec<u8>, Vec<u8>>,
}

impl State {
    pub fn new() -> State { State::default() }

    pub fn apply(self: &mut State, command: Command) -> CommandResult {
        match command {
            Command::Set { key, value } => {
                let _ = self.memory.insert(
                    Vec::from(key),
                    Vec::from(value)
                );

                Ok(Return::Ok)
            }
            Command::Get { key } => {
                match self.memory.get(key) {
                    Some(value) => Ok(Return::BulkString(value)),
                    None        => Ok(Return::Nil)
                }
            }
            Command::Exists { key } => {
                if self.memory.contains_key(key) {
                    Ok(Return::Integer(1))
                } else {
                    Ok(Return::Integer(0))
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

#[cfg(test)]
mod commands {
    use super::{State, Command, Return};

    #[test]
    fn get_and_set() {
        let mut state = State::default();

        assert_eq!(
            Ok(Return::Nil),
            state.apply(Command::Get { key: b"foo" })
        );

        assert_eq!(
            Ok(Return::Ok),
            state.apply(Command::Set { key: b"foo", value: b"bar" })
        );

        assert_eq!(
            Ok(Return::BulkString(b"bar")),
            state.apply(Command::Get { key: b"foo" })
        );
    }

    #[test]
    fn exists() {
        let mut state = State::default();

        assert_eq!(
            Ok(Return::Integer(0)),
            state.apply(Command::Exists { key: b"foo" })
        );

        assert_eq!(
            Ok(Return::Ok),
            state.apply(Command::Set { key: b"foo", value: b"bar" })
        );

        assert_eq!(
            Ok(Return::Integer(1)),
            state.apply(Command::Exists { key: b"foo" })
        );
    }
}

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
        Err(_) => {}
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

fn not_multispace(c: u8) -> bool {
    match c {
        b' ' | b'\t' | b'\r' | b'\n' => false,
        _ => true,
    }
}

named!(string,
   alt!(
       delimited!(char!('"'), take_until!("\""), char!('"'))
     | take_while!(not_multispace)
   )
);

named!(get<Command>,
    chain!(
        tag!("GET") ~
        multispace ~
        key: string ~
        multispace?,
        || { Command::Get { key: key } }
    )
);

named!(exists<Command>,
    chain!(
        tag!("EXISTS") ~
        multispace ~
        key: string ~
        multispace?,
        || { Command::Exists { key: key } }
    )
);

named!(set<Command>,
    chain!(
        tag!("SET") ~
        multispace ~
        key: string ~
        multispace ~
        value: string ~
        multispace?,
        || { Command::Set { key: key, value: value } }
    )
);

named!(pub parser<Command>,
   alt!(get | set | exists)
);

#[cfg(test)]
mod parser {
    use super::{parser, Command};
    use nom::IResult;

    #[test]
    fn get_empty() {
        let empty = Command::Get { key: b"" };

        parses_to("GET \"\"\n", &empty);
    }

    #[test]
    fn get_ascii() {
        let foo = Command::Get { key: b"foo" };

        parses_to("GET foo \n", &foo);
        parses_to("GET \"foo\"\n", &foo);
    }

    #[test]
    fn get_bytes() {
        let bytes = Command::Get { key: b"\x01\x02\x03" };

        parses_to("GET \"\x01\x02\x03\"\n", &bytes);
        parses_to("GET \x01\x02\x03  \n", &bytes);
    }

    #[test]
    fn set_empty() {
        let empty = Command::Set { key: b"", value: b"" };

        parses_to("SET \"\" \"\"\n", &empty);
    }

    #[test]
    fn set_ascii() {
        let foo = Command::Set { key: b"foo", value: b"bar" };

        parses_to("SET foo   bar \n", &foo);
        parses_to("SET \"foo\" bar \n", &foo);
        parses_to("SET foo \"bar\" \n", &foo);
        parses_to("SET \"foo\"  \"bar\"\n", &foo);
    }

    #[test]
    fn set_bytes() {
        let bytes = Command::Set { key: b"\x01\x02\x03", value: b"\x01\x02\x03" };
        parses_to("SET \"\x01\x02\x03\" \"\x01\x02\x03\" \n", &bytes);
        parses_to("SET \x01\x02\x03  \x01\x02\x03 \n", &bytes);
    }

    #[test]
    fn exists() {
        let bytes = Command::Exists { key: b"foo" };
        parses_to("EXISTS foo", &bytes);
    }

    fn parses_to(i: &str, cmd: &Command) {
        assert_eq!(
            IResult::Done(&b""[..], cmd.clone()),
            parser(i.as_bytes())
        );
    }
}
