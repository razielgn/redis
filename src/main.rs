extern crate redis;
extern crate nom;

use std::io;
use std::io::Write;
use std::default::Default;

use redis::{State, parser};

use nom::IResult;

fn main() {
    let input = io::stdin();

    let mut state = State::default();

    loop {
        print!("> ");
        let _ = io::stdout().flush();

        let mut line = String::new();
        match input.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {
                println!("echo: {:?}", line);
                match parser(line.as_bytes()) {
                    IResult::Done(_, cmd) => {
                        let res = state.apply(cmd);
                        println!("{:?}", res);
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
