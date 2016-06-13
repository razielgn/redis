use nom::IResult;
use redis::commands::{Command, Bytes};
use redis::database::Database;
use redis::resp::{Value, decode, encode};
use std::io::{Read};
use std::net::{TcpListener, TcpStream, Shutdown};
use std::sync::{Arc, Mutex};
use std::thread;

pub fn listen() {
    let address = "127.0.0.1:9876";
    let listener = TcpListener::bind(address).unwrap();

    println!("Listening on {}", address);

    let database = Arc::new(Mutex::new(Database::new()));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let database = database.clone();
                thread::spawn(move || handle_client(stream, database));
            }
            Err(e) => {
                println!("Error connecting: {}", e);
            }
        }
    }
}

fn handle_client(mut stream: TcpStream, database: Arc<Mutex<Database>>) {
    loop {
        let mut buffer = [0; 1024];

        let size = stream.read(&mut buffer[..]).unwrap();

        match decode(&buffer[0..size]) {
            IResult::Done(_, Value::Array(array)) => {
                let a: Vec<Bytes> = array.into_iter()
                    .filter_map(|value| {
                        if let Value::BulkString(s) = value {
                            Some(s)
                        } else {
                            None
                        }
                    })
                    .collect();

                match Command::from_slice(&a) {
                    Ok(cmd) => {
                        let mut database = database.lock().unwrap();
                        let res = database.apply(cmd);
                        encode(&res, &mut stream).unwrap();
                    }
                    Err(err) => {
                        encode(&Err(err), &mut stream).unwrap();
                    }
                };
            }
            IResult::Done(_, _) => {
                break;
            }
            IResult::Error(_) => {
                break;
            }
            IResult::Incomplete(_) => {
                break;
            }
        }
    }

    stream.shutdown(Shutdown::Both).unwrap();
}
