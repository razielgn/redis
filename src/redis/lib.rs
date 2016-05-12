use std::collections::HashMap;
use std::io::{self, Write};

type BinaryString = Vec<u8>;

#[derive(PartialEq, Eq, Debug)]
enum Command {
    Set { key: BinaryString, value: BinaryString },
    Get { key: BinaryString },
}

#[derive(Default, Debug)]
struct State {
    memory: HashMap<BinaryString, BinaryString>,
}

fn apply(state: &mut State, command: Command) -> Return {
    match command {
        Command::Set { key, value } => {
            let _ = state.memory.insert(key, value);
            Return::Ok
        }
        Command::Get { key } => {
            match state.memory.get(&key) {
                Some(value) => Return::SimpleString(value),
                None        => Return::Nil
            }
        }
    }
}

#[derive(Eq, PartialEq, Debug)]
enum Error<'a> {
    UnknownCommand(&'a str),
    NotAnInteger,
}

#[derive(PartialEq, Eq, Debug)]
enum Return<'a> {
    Ok,
    Nil,
    SimpleString(&'a BinaryString),
    Integer(i64),
    BulkString(&'a BinaryString),
}

type CommandResult<'a> = Result<Return<'a>, Error<'a>>;

fn encode<T: Write>(result: &CommandResult, w: &mut T) -> io::Result<()> {
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
        let a = Vec::from("");
        encodes_to(Return::SimpleString(&a), "+\r\n");

        let b = Vec::from("asd");
        encodes_to(Return::SimpleString(&b), "+asd\r\n");
    }

    #[test]
    fn bulk_string() {
        let a = Vec::from("");
        encodes_to(Return::BulkString(&a), "$0\r\n\r\n");

        let b = Vec::from("asd");
        encodes_to(Return::BulkString(&b), "$3\r\nasd\r\n");
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
