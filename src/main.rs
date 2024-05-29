use std::{
    error::Error,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

fn main() -> Result<(), Box<dyn Error>> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                handle_request(&stream)?;
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    Ok(())
}

fn handle_request(mut stream: &TcpStream) -> Result<(), Box<dyn Error>> {
    let mut buf = [0u8; 1024];
    let bytes_read = stream.read(&mut buf)?;

    if bytes_read > 0 {
        match &buf[..bytes_read] {
            b"PING" => return ping(stream),
            _ => return Ok(()),
        }
    }

    Ok(())
}

fn ping(mut stream: &TcpStream) -> Result<(), Box<dyn Error>> {
    stream.write_all(b"+PONG\r\n")?;

    Ok(())
}
