use nom::IResult;
use redis::commands::Command;
use redis::database::Database;
use redis::line::tokenize;
use redis::resp::{decode_string_array, encode};
use std::io::{Read};
use std::net::{TcpListener, TcpStream};
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

        let tokenized = match buffer.first() {
            Some(&b'*') =>
                match decode_string_array(&buffer[0..size]) {
                    IResult::Done(_, tokenized) => tokenized,
                    _ => break
                },
            Some(_) =>
                match tokenize(&buffer[0..size]) {
                    IResult::Done(_, tokenized) => tokenized,
                    _ => break
                },
            _ =>
                break
        };

        match Command::from_slice(&tokenized) {
            Ok(command) => {
                let mut database = database.lock().unwrap();
                let res = database.apply(command);
                encode(&res, &mut stream).unwrap();
            }
            Err(err) => {
                encode(&Err(err), &mut stream).unwrap();
            }
        }
    }
}
