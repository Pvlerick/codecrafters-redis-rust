use std::error::Error;

use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (socket, _) = listener.accept().await?;

        println!("accepted new connection");

        tokio::spawn(async move {
            process(socket).await;
        });
    }
}

async fn process(mut socket: TcpStream) {
    let mut buf = [0u8; 256];

    loop {
        match socket.read(&mut buf).await {
            Ok(bytes_read) => {
                if bytes_read > 0 {
                    println!("{} bytes read on the stream", bytes_read);

                    handle_request(&buf[..bytes_read], &mut socket)
                        .await
                        .unwrap();
                } else {
                    break;
                }
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                break;
            }
        }
    }
}

async fn handle_request<T>(req: &[u8], output: &mut T) -> Result<(), Box<dyn Error>>
where
    T: AsyncWrite + std::marker::Unpin,
{
    let request = RequestType::parse(req)?;

    match request {
        RequestType::Ping => ping(output).await,
        RequestType::Echo(msg) => echo(msg, output).await,
        RequestType::NotImplemented => not_implemented(output).await,
    }
}

enum RequestType<'a> {
    Ping,
    Echo(&'a [u8]),
    NotImplemented,
}

impl<'a> RequestType<'a> {
    fn parse(body: &'a [u8]) -> Result<RequestType<'a>, Box<dyn Error>> {
        println!("body: {:?}", body);
        println!("body (utf8): {}", String::from_utf8_lossy(body));

        match body {
            [b'*', array @ ..] => {
                let (array_len, array_content) = take_until_crlf(array);
                let _array_len = parse_bytes_to_usize(array_len);
                match array_content {
                    [b'$', value @ ..] => {
                        let (value_len, tail) = take_until_crlf(value);
                        let value_len = parse_bytes_to_usize(value_len);
                        let value = &tail[..value_len];
                        return match value {
                            [b'P', b'I', b'N', b'G'] => Ok(RequestType::Ping),
                            [b'E', b'C', b'H', b'O'] => {
                                let (msg_len, msg_content) = take_until_crlf(&tail[value_len..]);
                                let _msg_len = parse_bytes_to_usize(msg_len);
                                Ok(RequestType::Echo(&msg_content))
                            }
                            _ => Ok(RequestType::NotImplemented),
                        };
                    }
                    _ => return Ok(RequestType::NotImplemented),
                }
            }
            _ => Ok(RequestType::NotImplemented),
        }
    }
}

async fn ping<T>(output: &mut T) -> Result<(), Box<dyn Error>>
where
    T: AsyncWrite + std::marker::Unpin,
{
    output.write_all(b"+PONG\r\n").await?;

    Ok(())
}

async fn echo<T>(msg: &[u8], output: &mut T) -> Result<(), Box<dyn Error>>
where
    T: AsyncWrite + std::marker::Unpin,
{
    println!("echo: {:?}", msg);
    output.write_all(msg).await?;

    Ok(())
}

async fn not_implemented<T>(output: &mut T) -> Result<(), Box<dyn Error>>
where
    T: AsyncWrite + std::marker::Unpin,
{
    output.write_all(b"-Error not implemented\r\n").await?;

    Ok(())
}

fn take_until_crlf(slice: &[u8]) -> (&[u8], &[u8]) {
    let mut prev_char_is_cr = false;
    for i in 0..slice.len() {
        if slice[i] == b'\n' && prev_char_is_cr {
            return (&slice[0..i - 1], &slice[i + 1..]);
        } else if slice[i] == b'\r' {
            prev_char_is_cr = true;
        } else {
            prev_char_is_cr = false;
        }
    }

    (slice, &[])
}

fn parse_bytes_to_usize(input: &[u8]) -> usize {
    input
        .iter()
        .fold(0usize, |a, i| a * 10 + ((*i as usize) - 48))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn single_ping() {
        let mut output = Vec::<u8>::new();
        assert!(handle_request(b"*1\r\n$4\r\nPING\r\n", &mut output)
            .await
            .is_ok());
        assert_eq!(b"+PONG\r\n", output[..].as_ref());
    }

    #[tokio::test]
    async fn single_echo() {
        let mut output = Vec::<u8>::new();
        assert!(
            handle_request(b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n", &mut output)
                .await
                .is_ok()
        );
        assert_eq!(b"$3\r\nhey\r\n", output[..].as_ref());
    }

    #[test]
    fn take_until_crlf_contains_crlf() {
        let (before, after) = take_until_crlf(b"Hello\r\nWorld!\r\n");
        assert_eq!(b"Hello", before);
        assert_eq!(b"World!\r\n", after);
    }

    #[test]
    fn take_until_crlf_no_crlf() {
        let (before, after) = take_until_crlf(b"Hello\nWorld!\r");
        assert_eq!(b"Hello\nWorld!\r", before);
        assert_eq!(&[] as &[u8], after);
    }

    #[test]
    fn parse_bytes_to_usize_test() {
        assert_eq!(5, parse_bytes_to_usize(&[53]));
        assert_eq!(12, parse_bytes_to_usize(&[49, 50]));
        assert_eq!(1548, parse_bytes_to_usize(&[49, 53, 52, 56]));
    }
}
