use nom::{multispace, crlf};

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

named!(pub tokenize<Vec<&[u8]> >,
    chain!(
        l: separated_list!(multispace, string) ~
        crlf,
        || l
    )
);

#[cfg(test)]
mod test {
    use nom::IResult;
    use super::tokenize;
    use redis::commands::Bytes;

    #[test]
    fn example() {
        tokenizes_to(&[b"set"], b"set\r\n");
        tokenizes_to(&[b"set", b"foo", b"-4"], b"set foo -4\r\n");
        tokenizes_to(&[b"set", b"foo", b"bar"], b"set \"foo\" \"bar\"\r\n");
    }

    fn tokenizes_to(expected: &[Bytes], i: Bytes) {
        let actual = tokenize(i);

        if let IResult::Done(&[], tokenized) = actual {
            assert_eq!(expected, tokenized.as_slice());
        } else {
            panic!(format!("{:?}", actual));
        }
    }
}
