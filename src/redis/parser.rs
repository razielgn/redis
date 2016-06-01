use nom::{multispace, digit, alpha};
use redis::commands::{Command, IntRange};
use std::str;

fn not_multispace(c: u8) -> bool {
    match c {
        b' ' | b'\t' | b'\r' | b'\n' => false,
        _ => true,
    }
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

named!(string,
   alt!(
       delimited!(char!('"'), take_until!("\""), char!('"'))
     | take_while!(not_multispace)
   )
);

named!(range<IntRange>,
    chain!(
        multispace ~
        start: integer ~
        multispace ~
        end: integer,
        || start..end
    )
);

named!(key_value<(&[u8], &[u8])>,
    chain!(
        key: string ~
        multispace ~
        value: string,
        || (key, value)
    )
);

named!(key_values<(&[u8], Vec<&[u8]>)>,
    chain!(
        key: string ~
        multispace ~
        values: separated_nonempty_list!(multispace, string),
        || (key, values)
    )
);

named!(key_range_opt<(&[u8], Option<IntRange>)>,
    tuple!(string, opt!(range))
);

named!(key_range<(&[u8], IntRange)>,
    tuple!(string, range)
);

named!(key_int<(&[u8], i64)>,
    chain!(
        key: string ~
        multispace ~
        by: integer,
        || (key, by)
    )
);

named!(keys<Vec<&[u8]> >,
    separated_nonempty_list!(multispace, string)
);

named!(command,
    chain!(
        command: alpha ~
        multispace,
        || command
    )
);

named!(pub parse<Command>,
    chain!(
        multispace? ~
        command: switch!(command,
            b"GET"      => map!(string, |k| Command::Get { key: k })
          | b"TYPE"     => map!(string, |k| Command::Type { key: k })
          | b"STRLEN"   => map!(string, |k| Command::Strlen { key: k })
          | b"LLEN"     => map!(string, |k| Command::LLen { key: k })
          | b"INCR"     => map!(string, |k| Command::IncrBy { key: k, by: 1 })
          | b"DECR"     => map!(string, |k| Command::DecrBy { key: k, by: 1 })
          | b"BITCOUNT" => map!(key_range_opt, |(k, r)| Command::BitCount { key: k, range: r })
          | b"GETRANGE" => map!(key_range, |(k, r)| Command::GetRange { key: k, range: r })
          | b"INCRBY"   => map!(key_int, |(k, by)| Command::IncrBy { key: k, by: by })
          | b"DECRBY"   => map!(key_int, |(k, by)| Command::DecrBy { key: k, by: by })
          | b"SET"      => map!(key_value, |(k, v)| Command::Set { key: k, value: v })
          | b"APPEND"   => map!(key_value, |(k, v)| Command::Append { key: k, value: v })
          | b"RENAME"   => map!(key_value, |(k1, k2)| Command::Rename { key: k1, new_key: k2 })
          | b"LPUSH"    => map!(key_values, |(k, vs)| Command::LPush { key: k, values: vs })
          | b"EXISTS"   => map!(keys, |keys| Command::Exists { keys: keys })
          | b"DEL"      => map!(keys, |keys| Command::Del { keys: keys })
        ) ~
        multispace?,
        || command
    )
);

#[cfg(test)]
mod test {
    use nom::IResult;
    use redis::commands::Command;
    use super::parse;

    #[test]
    fn get_empty() {
        let empty = Command::Get { key: b"" };

        parses_to("GET \"\"\n", &empty);
        parses_to("   GET \"\"\n", &empty);
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

    #[test]
    fn strlen() {
        let cmd = Command::Strlen { key: b"foo" };
        parses_to("STRLEN foo", &cmd);
    }

    #[test]
    fn incr() {
        let cmd = Command::IncrBy { key: b"foo", by: 1 };
        parses_to("INCR foo", &cmd);
    }

    #[test]
    fn decr() {
        let cmd = Command::DecrBy { key: b"foo", by: 1 };
        parses_to("DECR foo", &cmd);
    }

    #[test]
    fn incr_by() {
        parses_to(
            &format!("INCRBY foo {}", i64::max_value()),
            &Command::IncrBy { key: b"foo", by: i64::max_value() }
        );
        parses_to(
            &format!("INCRBY foo +{}", i64::max_value()),
            &Command::IncrBy { key: b"foo", by: i64::max_value() }
        );
        parses_to(
            &format!("INCRBY foo {}", i64::min_value()),
            &Command::IncrBy { key: b"foo", by: i64::min_value() }
        );
    }

    #[test]
    fn decr_by() {
        parses_to(
            &format!("DECRBY foo {}", i64::max_value()),
            &Command::DecrBy { key: b"foo", by: i64::max_value() }
        );
        parses_to(
            &format!("DECRBY foo +{}", i64::max_value()),
            &Command::DecrBy { key: b"foo", by: i64::max_value() }
        );
        parses_to(
            &format!("DECRBY foo {}", i64::min_value()),
            &Command::DecrBy { key: b"foo", by: i64::min_value() }
        );
    }

    #[test]
    fn append() {
        parses_to(
            "APPEND foo bar",
            &Command::Append { key: b"foo", value: b"bar" }
        );
    }

    #[test]
    fn type_() {
        parses_to(
            "TYPE foo",
            &Command::Type { key: b"foo" }
        );
    }

    #[test]
    fn bit_count() {
        parses_to(
            "BITCOUNT foo",
            &Command::BitCount { key: b"foo", range: None }
        );

        parses_to(
            "BITCOUNT foo -1 25",
            &Command::BitCount { key: b"foo", range: Some(-1..25) }
        );

        parses_to(
            "BITCOUNT foo 1 -25",
            &Command::BitCount { key: b"foo", range: Some(1..-25) }
        );
    }

    #[test]
    fn get_range() {
        parses_to(
            "GETRANGE foo -1 25",
            &Command::GetRange { key: b"foo", range: -1..25 }
        );

        parses_to(
            "GETRANGE foo 1 -25",
            &Command::GetRange { key: b"foo", range: 1..-25 }
        );
    }

    #[test]
    fn lpush() {
        parses_to(
            "LPUSH foo br bazz 234 -4",
            &Command::LPush {
                key: b"foo",
                values: vec![
                    &b"br"[..],
                    &b"bazz"[..],
                    &b"234"[..],
                    &b"-4"[..]
                ],
            }
        );
    }

    #[test]
    fn llen() {
        parses_to("LLEN foo", &Command::LLen { key: b"foo" });
    }

    fn parses_to(i: &str, cmd: &Command) {
        assert_eq!(
            IResult::Done(&b""[..], cmd.clone()),
            parse(i.as_bytes())
        );
    }
}
