extern crate redis;
extern crate nom;

use std::io;

use redis::{State, parser, encode};

use nom::IResult;

fn main() {
    let input = io::stdin();

    let mut state = State::new();

    loop {
        let mut output = io::stdout();

        let mut line = String::new();
        match input.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {
                match parser(line.as_bytes()) {
                    IResult::Done(_, cmd) => {
                        let res = state.apply(cmd);
                        encode(&res, &mut output).unwrap();
                    }
                    IResult::Error(err) =>
                        println!("Error: {:?}", err),
                    IResult::Incomplete(needed) =>
                        println!("Incomplete: {:?}", needed),
                }
            },
            Err(error) =>
                panic!("{:?}", error),
        }
    }
}
