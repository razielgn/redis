use std::io::{self, Write};
use redis::database::{CommandError, CommandReturn, CommandResult, Type};

pub fn encode<T: Write>(result: &CommandResult, w: &mut T) -> io::Result<()> {
    match *result {
        Ok(ref ret) => {
            match *ret {
                CommandReturn::Ok =>
                    try!(write!(w, "+OK")),
                CommandReturn::Nil =>
                    try!(write!(w, "$-1")),
                CommandReturn::BulkString(ref s) => {
                    try!(write!(w, "${}\r\n", s.len()));
                    try!(w.write_all(s));
                }
                CommandReturn::Integer(i) =>
                    try!(write!(w, ":{}", i)),
                CommandReturn::Size(u) =>
                    try!(write!(w, ":{}", u)),
                CommandReturn::Type(Type::None) =>
                    try!(write!(w, "+none")),
                CommandReturn::Type(Type::String) =>
                    try!(write!(w, "+string")),
            }
        }
        Err(ref err) => {
            try!(write!(w, "-ERR "));

            match *err {
                CommandError::NoSuchKey =>
                    try!(write!(w, "no such key")),
                CommandError::UnknownCommand(cmd) => {
                    try!(write!(w, "unknown command '"));
                    try!(w.write_all(cmd));
                    try!(write!(w, "'"));
                }
                CommandError::NotAnInteger =>
                    try!(write!(w, "value is not an integer or out of range")),
                CommandError::IntegerOverflow =>
                    try!(write!(w, "increment or decrement would overflow")),
            }
        }
    }

    try!(write!(w, "\r\n"));
    Ok(())
}

#[cfg(test)]
mod test {
    use redis::database::{CommandError, CommandReturn, CommandResult, Type};
    use std::borrow::Cow;
    use super::{encode};

    #[test]
    fn ok() {
        encodes_to(Ok(CommandReturn::Ok), "+OK\r\n");
    }

    #[test]
    fn nil() {
        encodes_to(Ok(CommandReturn::Nil), "$-1\r\n");
    }

    #[test]
    fn bulk_string() {
        encodes_to(
            Ok(CommandReturn::BulkString(Cow::Borrowed(b""))),
            "$0\r\n\r\n"
        );
        encodes_to(
            Ok(CommandReturn::BulkString(Cow::Borrowed(b"asd"))),
            "$3\r\nasd\r\n"
        );
    }

    #[test]
    fn integer() {
        encodes_to(Ok(CommandReturn::Integer(1238439)), ":1238439\r\n");
        encodes_to(Ok(CommandReturn::Integer(-1238439)), ":-1238439\r\n");
    }

    #[test]
    fn size() {
        encodes_to(Ok(CommandReturn::Size(1238439)), ":1238439\r\n");
    }

    #[test]
    fn type_() {
        encodes_to(Ok(CommandReturn::Type(Type::None)), "+none\r\n");
        encodes_to(Ok(CommandReturn::Type(Type::String)), "+string\r\n");
    }

    #[test]
    fn no_such_key() {
        encodes_to(Err(CommandError::NoSuchKey), "-ERR no such key\r\n");
    }

    #[test]
    fn not_an_integer() {
        encodes_to(
            Err(CommandError::NotAnInteger),
            "-ERR value is not an integer or out of range\r\n"
        );
    }

    #[test]
    fn unknown_command() {
        encodes_to(
            Err(CommandError::UnknownCommand(b"asd")),
            "-ERR unknown command 'asd'\r\n"
        );
    }

    #[test]
    fn integer_overflow() {
        encodes_to(
            Err(CommandError::IntegerOverflow),
            "-ERR increment or decrement would overflow\r\n"
        );
    }

    fn encodes_to(ret: CommandResult, to: &str) {
        let mut output = Vec::new();

        assert!(encode(&ret, &mut output).is_ok());
        assert_eq!(to, String::from_utf8(output).unwrap());
    }
}
