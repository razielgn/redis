use std::io::{self, Write};
use redis::database::{CommandError, CommandReturn, CommandResult, Type};

pub fn encode<T: Write>(result: &CommandResult, w: &mut T) -> io::Result<()> {
    match *result {
        Ok(ref ret)  => encode_return(ret, w),
        Err(ref err) => encode_error(err, w),
    }
}

fn encode_return<T: Write>(ret: &CommandReturn, w: &mut T) -> io::Result<()> {
    match *ret {
        CommandReturn::Ok =>
            try!(write!(w, "+OK\r\n")),
        CommandReturn::Nil =>
            try!(write!(w, "$-1\r\n")),
        CommandReturn::BulkString(ref s) => {
            try!(write!(w, "${}\r\n", s.len()));
            try!(w.write_all(s));
            try!(write!(w, "\r\n"));
        }
        CommandReturn::Integer(i) =>
            try!(write!(w, ":{}\r\n", i)),
        CommandReturn::Size(u) =>
            try!(write!(w, ":{}\r\n", u)),
        CommandReturn::Type(Type::None) =>
            try!(write!(w, "+none\r\n")),
        CommandReturn::Type(Type::String) =>
            try!(write!(w, "+string\r\n")),
        CommandReturn::Type(Type::List) =>
            try!(write!(w, "+list\r\n")),
        CommandReturn::Array(ref v) => {
            try!(write!(w, "*{}\r\n", v.len()));

            for m in v {
                try!(encode_return(m, w));
            }
        }
    }

    Ok(())
}

fn encode_error<T: Write>(err: &CommandError, w: &mut T) -> io::Result<()> {
    try!(write!(w, "-"));

    match *err {
        CommandError::NoSuchKey =>
            try!(write!(w, "ERR no such key")),
        CommandError::WrongType =>
            try!(write!(w, "WRONGTYPE Operation against a key holding the wrong kind of value")),
        CommandError::UnknownCommand(cmd) => {
            try!(write!(w, "ERR unknown command '"));
            try!(w.write_all(cmd));
            try!(write!(w, "'"));
        }
        CommandError::NotAnInteger =>
            try!(write!(w, "ERR value is not an integer or out of range")),
        CommandError::IntegerOverflow =>
            try!(write!(w, "ERR increment or decrement would overflow")),
    }

    write!(w, "\r\n")
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
        encodes_to(Ok(CommandReturn::Type(Type::List)), "+list\r\n");
    }

    #[test]
    fn array() {
        encodes_to(Ok(CommandReturn::Array(vec![])), "*0\r\n");

        let array = vec![
            CommandReturn::Ok,
            CommandReturn::Nil,
            CommandReturn::Integer(-25),
            CommandReturn::Size(42),
            CommandReturn::BulkString(Cow::Borrowed(b"foo")),
            CommandReturn::BulkString(Cow::Owned(b"bar".to_vec())),
            CommandReturn::Type(Type::String),
        ];

        encodes_to(
            Ok(CommandReturn::Array(array)),
            "*7\r\n+OK\r\n$-1\r\n:-25\r\n:42\r\n$3\r\nfoo\r\n$3\r\nbar\r\n+string\r\n"
        );
    }

    #[test]
    fn no_such_key() {
        encodes_to(Err(CommandError::NoSuchKey), "-ERR no such key\r\n");
    }

    #[test]
    fn wrong_type() {
        encodes_to(
            Err(CommandError::WrongType),
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
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
