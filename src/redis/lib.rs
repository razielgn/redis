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
    Exists { keys: Vec<&'a [u8]> },
    Del { keys: Vec<&'a [u8]> },
    Rename { key: &'a [u8], new_key: &'a [u8] },
}

#[derive(Default, Debug)]
pub struct State {
    memory: HashMap<Vec<u8>, Vec<u8>>,
}

impl State {
    pub fn new() -> State { State::default() }

    pub fn apply(&mut self, command: Command) -> CommandResult {
        match command {
            Command::Set { key, value } => {
                let _ = self.memory.insert(
                    Vec::from(key),
                    Vec::from(value)
                );

                Ok(Return::Ok)
            }
            Command::Get { key } =>
                match self.memory.get(key) {
                    Some(value) => Ok(Return::BulkString(value)),
                    None        => Ok(Return::Nil)
                },
            Command::Exists { keys } => {
                let sum = keys.into_iter()
                    .filter(|key| self.memory.contains_key(*key))
                    .count();

                Ok(Return::Size(sum))
            }
            Command::Del { keys } => {
                let sum = keys.into_iter()
                    .filter(|key| self.memory.remove(*key).map_or(false, |_| true))
                    .count();

                Ok(Return::Size(sum))
            },
            Command::Rename { key, new_key } =>
                match self.memory.remove(key) {
                    Some(value) => {
                        self.memory.insert(Vec::from(new_key), value);
                        Ok(Return::Ok)
                    },
                    None =>
                        Err(Error::NoSuchKey)
                }
        }
    }
}

#[derive(Eq, PartialEq, Debug)]
pub enum Error<'a> {
    UnknownCommand(&'a [u8]),
    NoSuchKey,
    NotAnInteger,
}

#[derive(PartialEq, Eq, Debug)]
pub enum Return<'a> {
    Ok,
    Nil,
    SimpleString(&'a [u8]),
    Integer(i64),
    Size(usize),
    BulkString(&'a [u8]),
}

pub type CommandResult<'a> = Result<Return<'a>, Error<'a>>;

#[cfg(test)]
mod commands {
    use super::{State, Command, Return, Error};

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
            Ok(Return::Size(0)),
            state.apply(Command::Exists { keys: vec!(b"foo", b"bar", b"baz") })
        );

        let _ = state.apply(Command::Set { key: b"foo", value: b"foo" });
        let _ = state.apply(Command::Set { key: b"baz", value: b"baz" });

        assert_eq!(
            Ok(Return::Size(2)),
            state.apply(Command::Exists { keys: vec!(b"foo", b"bar", b"baz") })
        );
    }

    #[test]
    fn del() {
        let mut state = State::default();

        assert_eq!(
            Ok(Return::Size(0)),
            state.apply(Command::Del { keys: vec!(b"foo", b"bar", b"baz") })
        );

        let _ = state.apply(Command::Set { key: b"foo", value: b"foo" });
        let _ = state.apply(Command::Set { key: b"bar", value: b"bar" });
        let _ = state.apply(Command::Set { key: b"baz", value: b"baz" });

        assert_eq!(
            Ok(Return::Size(2)),
            state.apply(Command::Del { keys: vec!(b"foo", b"baz") })
        );

        assert_eq!(
            Ok(Return::Size(1)),
            state.apply(Command::Exists { keys: vec!(b"foo", b"bar", b"baz") })
        );
    }

    #[test]
    fn rename() {
        let mut state = State::default();

        assert_eq!(
            Err(Error::NoSuchKey),
            state.apply(Command::Rename { key: b"foo", new_key: b"bar" })
        );

        let _ = state.apply(Command::Set { key: b"foo", value: b"foo" });

        assert_eq!(
            Ok(Return::Ok),
            state.apply(Command::Rename { key: b"foo", new_key: b"bar" })
        );

        assert_eq!(
            Ok(Return::Nil),
            state.apply(Command::Get { key: b"foo" })
        );

        assert_eq!(
            Ok(Return::BulkString(b"foo")),
            state.apply(Command::Get { key: b"bar" })
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
                Return::Size(u) =>
                    try!(write!(w, ":{}", u)),
            }
        }
        Err(ref err) => {
            try!(write!(w, "-ERR "));

            match *err {
                Error::NoSuchKey =>
                    try!(write!(w, "no such key")),
                Error::UnknownCommand(cmd) => {
                    try!(write!(w, "unknown command '"));
                    try!(w.write_all(cmd));
                    try!(write!(w, "'"));
                }
                Error::NotAnInteger =>
                    try!(write!(w, "value is not an integer or out of range")),
            }
        }
    }

    try!(write!(w, "\r\n"));
    Ok(())
}

#[cfg(test)]
mod resp {
    use super::{Return, encode, Error, CommandResult};

    #[test]
    fn ok() {
        encodes_to(Ok(Return::Ok), "+OK\r\n");
    }

    #[test]
    fn nil() {
        encodes_to(Ok(Return::Nil), "$-1\r\n");
    }

    #[test]
    fn simple_string() {
        encodes_to(Ok(Return::SimpleString(b"")), "+\r\n");
        encodes_to(Ok(Return::SimpleString(b"asd")), "+asd\r\n");
    }

    #[test]
    fn bulk_string() {
        encodes_to(Ok(Return::BulkString(b"")), "$0\r\n\r\n");
        encodes_to(Ok(Return::BulkString(b"asd")), "$3\r\nasd\r\n");
    }

    #[test]
    fn integer() {
        encodes_to(Ok(Return::Integer(1238439)), ":1238439\r\n");
        encodes_to(Ok(Return::Integer(-1238439)), ":-1238439\r\n");
    }

    #[test]
    fn size() {
        encodes_to(Ok(Return::Size(1238439)), ":1238439\r\n");
    }

    #[test]
    fn no_such_key() {
        encodes_to(Err(Error::NoSuchKey), "-ERR no such key\r\n");
    }

    #[test]
    fn not_an_integer() {
        encodes_to(
            Err(Error::NotAnInteger),
            "-ERR value is not an integer or out of range\r\n"
        );
    }

    #[test]
    fn unknown_command() {
        encodes_to(
            Err(Error::UnknownCommand(b"asd")),
            "-ERR unknown command 'asd'\r\n"
        );
    }

    fn encodes_to(ret: CommandResult, to: &str) {
        let mut output = Vec::new();

        assert!(encode(&ret, &mut output).is_ok());
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

named!(rename<Command>,
    chain!(
        tag!("RENAME") ~
        multispace ~
        key: string ~
        multispace? ~
        new_key: string ~
        multispace?,
        || { Command::Rename { key: key, new_key: new_key } }
    )
);

named!(exists<Command>,
    chain!(
        tag!("EXISTS") ~
        multispace ~
        keys: separated_nonempty_list!(multispace, string) ~
        multispace?,
        || { Command::Exists { keys: keys } }
    )
);

named!(del<Command>,
    chain!(
        tag!("DEL") ~
        multispace ~
        keys: separated_nonempty_list!(multispace, string) ~
        multispace?,
        || { Command::Del { keys: keys } }
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
   alt!(get | set | exists | del | rename)
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
        let cmd = Command::Exists { keys: vec!(b"foo", b"bar") };
        parses_to("EXISTS  foo   bar ", &cmd);
    }

    #[test]
    fn del() {
        let cmd = Command::Del { keys: vec!(b"foo", b"bar") };
        parses_to("DEL  foo   bar ", &cmd);
    }

    #[test]
    fn rename() {
        let cmd = Command::Rename { key: b"foo", new_key: b"bar" };
        parses_to("RENAME foo bar", &cmd);
    }

    fn parses_to(i: &str, cmd: &Command) {
        assert_eq!(
            IResult::Done(&b""[..], cmd.clone()),
            parser(i.as_bytes())
        );
    }
}
