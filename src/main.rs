use std::{
    error::Error,
    io::{Read, Write},
    net::TcpListener,
};

fn main() -> Result<(), Box<dyn Error>> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let mut buf = [0u8; 1024];
                let bytes_read = stream.read(&mut buf)?;
                if bytes_read > 0 {
                    println!("{} bytes read on the stream", bytes_read);
                    handle_request(&buf[..bytes_read], &mut stream)?;
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    Ok(())
}

fn handle_request(req: &[u8], output: &mut impl Write) -> Result<(), Box<dyn Error>> {
    match req {
        b"PING" => return ping(output),
        _ => {
            println!("req: {:?}", req);
            return Ok(());
        }
    }
}

fn ping(output: &mut impl Write) -> Result<(), Box<dyn Error>> {
    output.write_all(b"+PONG\r\n")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ping_test() {
        let mut output = Vec::<u8>::new();
        assert!(handle_request(b"PING", &mut output).is_ok());
        assert_eq!(b"+PONG\r\n", output[..].as_ref());
    }
}
