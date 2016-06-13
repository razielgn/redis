use redis::database::CommandError;
use std::ascii::AsciiExt;
use std::ops::Range;
use std::str;

pub type Bytes<'a> = &'a [u8];
pub type IntRange = Range<i64>;

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum Command<'a> {
    Append { key: Bytes<'a>, value: Bytes<'a> },
    BitCount { key: Bytes<'a>, range: Option<IntRange> },
    DecrBy { key: Bytes<'a>, by: i64 },
    Del { keys: &'a [Bytes<'a>] },
    Exists { keys: &'a [Bytes<'a>] },
    Get { key: Bytes<'a> },
    GetRange { key: Bytes<'a>, range: IntRange },
    IncrBy { key: Bytes<'a>, by: i64 },
    LIndex { key: Bytes<'a>, index: i64 },
    LLen { key: Bytes<'a> },
    LPop { key: Bytes<'a> },
    LPush { key: Bytes<'a>, values: &'a [Bytes<'a>] },
    Rename { key: Bytes<'a>, new_key: Bytes<'a> },
    Set { key: Bytes<'a>, value: Bytes<'a> },
    Strlen { key: Bytes<'a> },
    Type { key: Bytes<'a> },
}

fn slice_to_i64(s: Bytes) -> Option<i64> {
    str::from_utf8(s)
        .ok()
        .and_then(|s| i64::from_str_radix(s, 10).ok())
}

macro_rules! key_value {
    ( $cmd:ident, $slice:ident, $f:expr ) => {
        match &$slice[1..] {
            &[key, value] => Ok($f(key, value)),
            _             => Err(BadCommandAryth($cmd)),
        }
    };
}

macro_rules! key_int {
    ( $cmd:ident, $slice:ident, $f:expr ) => {
        match &$slice[1..] {
            &[key, value] =>
                slice_to_i64(value)
                    .ok_or(NotAnInteger)
                    .map(|i| $f(key, i)),
            _ =>
                Err(BadCommandAryth($cmd)),
        }
    };
}

macro_rules! string {
    ( $cmd:ident, $slice:ident, $f:expr ) => {
        match &$slice[1..] {
            &[key] => Ok($f(key)),
            _      => Err(BadCommandAryth($cmd)),
        }
    };
}

macro_rules! keys {
    ( $cmd:ident, $slice:ident, $f:expr ) => {
        match &$slice[1..] {
            &[]   => Err(BadCommandAryth($cmd)),
            keys  => Ok($f(keys))
        }
    };
}

macro_rules! key_range_opt {
    ( $cmd:ident, $slice:ident, $f:expr ) => {
        match &$slice[1..] {
            &[key] =>
                Ok($f(key, None)),
            &[key, from, to] => {
                match (slice_to_i64(from), slice_to_i64(to)) {
                    (Some(from), Some(to)) =>
                        Ok($f(key, Some(from..to))),
                    (None, _) | (_, None) =>
                        Err(NotAnInteger),
                }
            }
            _ =>
                Err(BadCommandAryth($cmd)),
        }
    };
}

macro_rules! key_range {
    ( $cmd:ident, $slice:ident, $f:expr ) => {
        match &$slice[1..] {
            &[key, from, to] => {
                match (slice_to_i64(from), slice_to_i64(to)) {
                    (Some(from), Some(to)) =>
                        Ok($f(key, from..to)),
                    (None, _) | (_, None) =>
                        Err(NotAnInteger),
                }
            }
            _ =>
                Err(BadCommandAryth($cmd)),
        }
    };
}

macro_rules! key_values {
    ( $cmd:ident, $slice:ident, $f:expr ) => {
        match &$slice[1..] {
            &[key, ref values..] => Ok($f(key, values)),
            _                    => Err(BadCommandAryth($cmd)),
        }
    };
}

impl<'a> Command<'a> {
    pub fn from_slice(s: &'a [Bytes<'a>]) -> Result<Command<'a>, CommandError> {
        use redis::database::CommandError::*;
        use self::Command::*;

        if s.is_empty() {
            return Err(UnknownCommand(b"".to_vec()));
        }

        let cmd = s[0].to_ascii_lowercase();

        match cmd.as_slice() {
            b"append"   => key_value!(cmd, s, |k, v| Append { key: k, value: v }),
            b"rename"   => key_value!(cmd, s, |k, nk| Rename { key: k, new_key: nk }),
            b"set"      => key_value!(cmd, s, |k, v| Set { key: k, value: v }),
            b"decr"     => string!(cmd, s, |k| DecrBy { key: k, by: 1 }),
            b"get"      => string!(cmd, s, |k| Get { key: k }),
            b"incr"     => string!(cmd, s, |k| IncrBy { key: k, by: 1 }),
            b"llen"     => string!(cmd, s, |k| LLen { key: k }),
            b"lpop"     => string!(cmd, s, |k| LPop { key: k }),
            b"strlen"   => string!(cmd, s, |k| Strlen { key: k }),
            b"type"     => string!(cmd, s, |k| Type { key: k }),
            b"del"      => keys!(cmd, s, |ks| Del { keys: ks }),
            b"exists"   => keys!(cmd, s, |ks| Exists { keys: ks }),
            b"decrby"   => key_int!(cmd, s, |k, i| DecrBy { key: k, by: i }),
            b"incrby"   => key_int!(cmd, s, |k, i| IncrBy { key: k, by: i }),
            b"lindex"   => key_int!(cmd, s, |k, i| LIndex { key: k, index: i }),
            b"bitcount" => key_range_opt!(cmd, s, |k, r| BitCount { key: k, range: r }),
            b"getrange" => key_range!(cmd, s, |k, r| GetRange { key: k, range: r }),
            b"lpush"    => key_values!(cmd, s, |k, vs| LPush { key: k, values: vs }),
            _           => Err(UnknownCommand(s[0].to_vec())),
        }
    }
}

#[cfg(test)]
mod test {
    use redis::database::CommandError::*;
    use super::Command::*;
    use super::Command;

    #[test]
    fn append() {
        assert_eq!(
            Ok(Append { key: b"foo", value: b"bar" }),
            Command::from_slice(&[b"append", b"foo", b"bar"])
        );
    }

    #[test]
    fn set() {
        assert_eq!(
            Ok(Set { key: b"foo", value: b"bar" }),
            Command::from_slice(&[b"set", b"foo", b"bar"])
        );
    }

    #[test]
    fn get() {
        assert_eq!(
            Ok(Get { key: b"foo" }),
            Command::from_slice(&[b"get", b"foo"])
        );
    }

    #[test]
    fn exists() {
        assert_eq!(
            Ok(Exists { keys: &[b"foo"] }),
            Command::from_slice(&[b"exists", b"foo"])
        );

        assert_eq!(
            Ok(Exists { keys: &[b"foo", b"bar"] }),
            Command::from_slice(&[b"exists", b"foo", b"bar"])
        );
    }

    #[test]
    fn del() {
        assert_eq!(
            Ok(Del { keys: &[b"foo"] }),
            Command::from_slice(&[b"del", b"foo"])
        );

        assert_eq!(
            Ok(Del { keys: &[b"foo", b"bar"] }),
            Command::from_slice(&[b"del", b"foo", b"bar"])
        );
    }

    #[test]
    fn rename() {
        assert_eq!(
            Ok(Rename { key: b"foo", new_key: b"bar" }),
            Command::from_slice(&[b"rename", b"foo", b"bar"])
        );
    }

    #[test]
    fn strlen() {
        assert_eq!(
            Ok(Strlen { key: b"foo" }),
            Command::from_slice(&[b"strlen", b"foo"])
        );
    }

    #[test]
    fn incr() {
        assert_eq!(
            Ok(IncrBy { key: b"foo", by: 1 }),
            Command::from_slice(&[b"incr", b"foo"])
        );
    }

    #[quickcheck]
    fn incr_by(by: i64) {
        let as_str = format!("{}", by);

        assert_eq!(
            Ok(IncrBy { key: b"foo", by: by }),
            Command::from_slice(&[b"incrby", b"foo", as_str.as_bytes()])
        );
    }

    #[quickcheck]
    fn decr_by(by: i64) {
        let as_str = format!("{}", by);

        assert_eq!(
            Ok(DecrBy { key: b"foo", by: by }),
            Command::from_slice(&[b"decrby", b"foo", as_str.as_bytes()])
        );
    }

    #[quickcheck]
    fn lindex(i: i64) {
        let as_str = format!("{}", i);

        assert_eq!(
            Ok(LIndex { key: b"foo", index: i }),
            Command::from_slice(&[b"lindex", b"foo", as_str.as_bytes()])
        );
    }

    #[test]
    fn decr() {
        assert_eq!(
            Ok(DecrBy { key: b"foo", by: 1 }),
            Command::from_slice(&[b"decr", b"foo"])
        );
    }

    #[test]
    fn type_() {
        assert_eq!(
            Ok(Type { key: b"foo" }),
            Command::from_slice(&[b"type", b"foo"])
        );
    }

    #[test]
    fn llen() {
        assert_eq!(
            Ok(LLen { key: b"foo" }),
            Command::from_slice(&[b"llen", b"foo"])
        );
    }

    #[test]
    fn lpop() {
        assert_eq!(
            Ok(LPop { key: b"foo" }),
            Command::from_slice(&[b"lpop", b"foo"])
        );
    }

    #[test]
    fn bitcount() {
        assert_eq!(
            Ok(BitCount { key: b"foo", range: None }),
            Command::from_slice(&[b"bitcount", b"foo"])
        );

        assert_eq!(
            Ok(BitCount { key: b"foo", range: Some(-1..1) }),
            Command::from_slice(&[b"bitcount", b"foo", b"-1", b"1"])
        );
    }

    #[test]
    fn bitcount_error() {
        assert_eq!(
            Err(NotAnInteger),
            Command::from_slice(&[b"bitcount", b"foo", b"bar", b"1"])
        );

        assert_eq!(
            Err(NotAnInteger),
            Command::from_slice(&[b"bitcount", b"foo", b"-1", b"bar"])
        );
    }

    #[test]
    fn get_range() {
        assert_eq!(
            Ok(GetRange { key: b"foo", range: -1..1 }),
            Command::from_slice(&[b"getrange", b"foo", b"-1", b"1"])
        );
    }

    #[test]
    fn lpush() {
        assert_eq!(
            Ok(LPush { key: b"foo", values: &[b"a", b"b", b"c"] }),
            Command::from_slice(&[b"lpush", b"foo", b"a", b"b", b"c"])
        );
    }
}
