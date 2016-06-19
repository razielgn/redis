use mioco::tcp::{TcpListener, TcpStream};
use mioco;
use nom::IResult;
use redis::commands::Command;
use redis::database::Database;
use redis::line::tokenize;
use redis::resp::{decode_string_array, encode};
use std::io::{self, Read};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

pub fn listen_async() {
    mioco::start(|| -> io::Result<()> {
        let address = SocketAddr::from_str("127.0.0.1:9876").unwrap();
        let listener = try!(TcpListener::bind(&address));
        let database = Arc::new(Mutex::new(Database::new()));

        println!("Starting Redis on {:?}", try!(listener.local_addr()));

        loop {
            let conn = try!(listener.accept());
            let database = database.clone();

            mioco::spawn(move || handle_client(conn, database));
        }
    }).unwrap().unwrap();
}

fn handle_client(mut stream: TcpStream, database: Arc<Mutex<Database>>) -> io::Result<()> {
    let mut buffer = [0; 1024 * 16];

    loop {
        let size = try!(stream.read(&mut buffer[..]));

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
                try!(encode(&res, &mut stream));
            }
            Err(err) => {
                try!(encode(&Err(err), &mut stream));
            }
        }
    }

    Ok(())
}
