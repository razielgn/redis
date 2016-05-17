use nom::{multispace, digit};
use redis::commands::Command;
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

named!(append<Command>,
    chain!(
        tag!("APPEND") ~
        multispace ~
        key: string ~
        multispace ~
        value: string ~
        multispace?,
        || { Command::Append { key: key, value: value } }
    )
);

named!(incr<Command>,
    chain!(
        tag!("INCR") ~
        multispace ~
        key: string ~
        multispace?,
        || { Command::IncrBy { key: key, by: 1 } }
    )
);

named!(strlen<Command>,
    chain!(
        tag!("STRLEN") ~
        multispace ~
        key: string ~
        multispace?,
        || { Command::Strlen { key: key } }
    )
);

named!(incr_by<Command>,
    chain!(
        tag!("INCRBY") ~
        multispace ~
        key: string ~
        multispace? ~
        by: integer ~
        multispace?,
        || { Command::IncrBy { key: key, by: by } }
    )
);

named!(decr<Command>,
    chain!(
        tag!("DECR") ~
        multispace ~
        key: string ~
        multispace?,
        || { Command::DecrBy { key: key, by: 1 } }
    )
);

named!(decr_by<Command>,
    chain!(
        tag!("DECRBY") ~
        multispace ~
        key: string ~
        multispace? ~
        by: integer ~
        multispace?,
        || { Command::DecrBy { key: key, by: by } }
    )
);

named!(pub parse<Command>,
   alt!(get | set | exists | del | rename | incr | incr_by | decr | decr_by |
        strlen | append)
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

    fn parses_to(i: &str, cmd: &Command) {
        assert_eq!(
            IResult::Done(&b""[..], cmd.clone()),
            parse(i.as_bytes())
        );
    }
}
