use nom::IResult;
use redis::database::Database;
use redis::parser::parse;
use redis::resp::encode;
use std::io;

pub fn repl() {
    let input = io::stdin();
    let mut database = Database::new();

    loop {
        let mut output = io::stdout();

        let mut line = String::new();
        match input.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {
                match parse(line.as_bytes()) {
                    IResult::Done(_, cmd) => {
                        let res = database.apply(cmd);
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
