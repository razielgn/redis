use nom::{crlf, digit};
use redis::commands::Bytes;
use redis::database::{CommandError, CommandReturn, CommandResult, Type};
use std::io::{self, Write};
use std::str::{self, FromStr};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Value<'a> {
    SimpleString(Bytes<'a>),
    Error(Bytes<'a>),
    Integer(i64),
    BulkString(Bytes<'a>),
    Array(Vec<Value<'a>>),
    Null,
}

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
        CommandError::UnknownCommand(ref cmd) => {
            try!(write!(w, "ERR unknown command '"));
            try!(w.write_all(cmd));
            try!(write!(w, "'"));
        }
        CommandError::BadCommandAryth(ref cmd) => {
            try!(write!(w, "ERR wrong number of arguments for '"));
            try!(w.write_all(cmd));
            try!(write!(w, "' command"));
        }
        CommandError::NotAnInteger =>
            try!(write!(w, "ERR value is not an integer or out of range")),
        CommandError::IntegerOverflow =>
            try!(write!(w, "ERR increment or decrement would overflow")),
    }

    write!(w, "\r\n")
}

named!(integer<i64>,
    chain!(
        sign: one_of!("-+")? ~
        digits: map_res!(
            map_res!(digit, str::from_utf8),
            |s| {
                let sign = sign.unwrap_or('+');
                i64::from_str_radix(&format!("{}{}", sign, s), 10)
            }
        ),
        || { digits }
    )
);

named!(size<usize>,
    map_res!(
        map_res!(digit, str::from_utf8),
        FromStr::from_str
    )
);

named!(bulk_string,
    chain!(
        size: size ~
        crlf ~
        bulk: take!(size) ~
        crlf,
        || bulk
    )
);

named!(binary_string,
    terminated!(
       is_not!("\n\r"),
       crlf
    )
);

named!(array<Vec<Value> >,
    chain!(
        size: size ~
        crlf ~
        values: count!(decode, size),
        || values
    )
);

named!(null, tag!(b"-1\r\n"));

named!(pub decode<Value>,
    switch!(take!(1),
        b"+" => map!(binary_string, Value::SimpleString)
      | b"-" => map!(binary_string, Value::Error)
      | b":" => map!(terminated!(integer, crlf), Value::Integer)
      | b"$" => alt!(
                    map!(null, |_| Value::Null)
                  | map!(bulk_string, Value::BulkString)
                )
      | b"*" => alt!(
                    map!(null, |_| Value::Null)
                  | map!(array, Value::Array)
                )
    )
);

#[cfg(test)]
mod test {
    mod decode {
        use nom::IResult;
        use quickcheck::TestResult;
        use std::io::Write;
        use super::super::{decode, Value};

        #[quickcheck]
        fn simple_strings(s: String) -> TestResult {
            if s.is_empty() || s.contains('\n') || s.contains('\r') {
                return TestResult::discard();
            }

            parses_to(
                format!("+{}\r\n", s).as_bytes(),
                &Value::SimpleString(s.as_bytes())
            );

            TestResult::passed()
        }

        #[test]
        fn simple_strings_no_parse() {
            doesnt_parse(b"+\r\n");
            doesnt_parse(b"+OK\nNOT\r\n");
            doesnt_parse(b"+OK\rNOT\r\n");
        }

        #[quickcheck]
        fn integers(n: i64) {
            parses_to(
                &format!(":{}\r\n", n).as_bytes(),
                &Value::Integer(n)
            );
        }

        #[quickcheck]
        fn errors(s: String) -> TestResult {
            if s.is_empty() || s.contains('\n') || s.contains('\r') {
                return TestResult::discard();
            }

            parses_to(
                format!("-{}\r\n", s).as_bytes(),
                &Value::Error(s.as_bytes())
            );

            TestResult::passed()
        }

        #[test]
        fn errors_no_parse() {
            doesnt_parse(b"-\r\n");
            doesnt_parse(b"-OK\nNOT\r\n");
            doesnt_parse(b"-OK\rNOT\r\n");
        }

        #[quickcheck]
        fn bulk_string(s: Vec<u8>) {
            let mut input = Vec::new();
            write!(&mut input, "${}\r\n", s.len()).unwrap();
            input.append(&mut s.clone());
            write!(&mut input, "\r\n").unwrap();

            parses_to(&input, &Value::BulkString(&s));
        }

        #[test]
        fn bulk_string_no_parse() {
            doesnt_parse(b"$89kkkkkk");
            doesnt_parse(b"$1\r\nfoo");
        }

        #[test]
        fn bulk_string_null() {
            parses_to(b"$-1\r\n", &Value::Null);
        }

        #[test]
        fn array() {
            parses_to(b"*-1\r\n", &Value::Null);
            parses_to(b"*0\r\n", &Value::Array(vec![]));
            parses_to(
                b"*6\r\n+OK\r\n:-42\r\n$-1\r\n-ERR\r\n$6\r\nfoobar\r\n*1\r\n:1\r\n",
                &Value::Array(vec![
                    Value::SimpleString(b"OK"),
                    Value::Integer(-42),
                    Value::Null,
                    Value::Error(b"ERR"),
                    Value::BulkString(b"foobar"),
                    Value::Array(vec![
                        Value::Integer(1),
                    ]),
                ])
            );
        }

        fn parses_to(i: &[u8], v: &Value) {
            assert_eq!(
                IResult::Done(&b""[..], v.clone()),
                decode(i)
            );
        }

        fn doesnt_parse(i: &[u8]) {
            let result = decode(i);
            println!("{:?}", result);
            assert!(result.is_err());
        }
    }

    mod encode {
        use redis::database::{CommandError, CommandReturn, CommandResult, Type};
        use std::borrow::Cow;
        use super::super::{encode};

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
                Err(CommandError::UnknownCommand(b"asd".to_vec())),
                "-ERR unknown command 'asd'\r\n"
            );
        }

        #[test]
        fn bad_command_aryth() {
            encodes_to(
                Err(CommandError::BadCommandAryth(b"asd".to_vec())),
                "-ERR wrong number of arguments for 'asd' command\r\n"
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
}
