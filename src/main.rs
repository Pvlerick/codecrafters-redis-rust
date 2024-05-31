use std::{collections::HashMap, error::Error, sync::Arc, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    time::Instant,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let state = Arc::new(Mutex::new(HashMap::new()));
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (socket, _) = listener.accept().await?;

        println!("accepted new connection");

        let state_ref = state.clone();
        tokio::spawn(async move {
            process(socket, state_ref).await;
        });
    }
}

struct Entry {
    value: Vec<u8>,
    expiry: Option<Expiry>,
}

struct Expiry {
    created_at: Instant,
    expires_after: Duration,
}

impl Entry {
    fn new(value: &[u8], expires_after: Option<Duration>) -> Entry {
        Entry {
            value: value.to_vec(),
            expiry: expires_after.map_or(None, |i| {
                Some(Expiry {
                    created_at: Instant::now(),
                    expires_after: i,
                })
            }),
        }
    }
}

async fn process(mut socket: TcpStream, state: Arc<Mutex<HashMap<Vec<u8>, Entry>>>) {
    let mut buf = [0u8; 256];

    loop {
        match socket.read(&mut buf).await {
            Ok(bytes_read) => {
                if bytes_read > 0 {
                    println!("{} bytes read on the stream", bytes_read);

                    handle_request(&buf[..bytes_read], state.clone(), &mut socket)
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

async fn handle_request<T>(
    req: &[u8],
    state: Arc<Mutex<HashMap<Vec<u8>, Entry>>>,
    output: &mut T,
) -> Result<(), Box<dyn Error>>
where
    T: AsyncWrite + Send + std::marker::Unpin,
{
    let request = RequestType::parse(req)?;

    match request {
        RequestType::Ping => ping(output).await,
        RequestType::Echo(msg) => echo(msg, output).await,
        RequestType::Set(key, value, expiry) => set(state, key, value, expiry, output).await,
        RequestType::Get(key) => get(state, key, output).await,
        RequestType::NotImplemented => not_implemented(output).await,
    }
}

enum RequestType<'a> {
    Ping,
    Echo(&'a [u8]),
    Set(&'a [u8], &'a [u8], Option<Duration>),
    Get(&'a [u8]),
    NotImplemented,
}

impl<'a> RequestType<'a> {
    fn parse(body: &'a [u8]) -> Result<RequestType<'a>, Box<dyn Error>> {
        // println!("body: {:?}", body);
        // println!("body (utf8): {}", String::from_utf8_lossy(body));

        match body {
            [b'*', array @ ..] => {
                let (array_len, array_content) = take_until_crlf(array);
                let _array_len = parse_bytes_to_usize(array_len);
                match array_content {
                    [b'$', ..] => {
                        let (value, tail) = get_string(array_content)?;
                        return match value {
                            [b'P', b'I', b'N', b'G'] => Ok(RequestType::Ping),
                            [b'E', b'C', b'H', b'O'] => Ok(RequestType::Echo(&tail[2..])),
                            [b'S', b'E', b'T'] => {
                                let (key, tail) = get_string(tail)?;
                                let (value, tail) = get_string(tail)?;
                                let (expiry_type, tail) = get_string(tail)?;
                                match expiry_type {
                                    [b'p', b'x'] => {
                                        let (expiry, _) = get_string(tail)?;
                                        Ok(RequestType::Set(
                                            key,
                                            value,
                                            Some(Duration::from_millis(
                                                String::from_utf8_lossy(expiry).parse()?,
                                            )),
                                        ))
                                    }
                                    _ => Ok(RequestType::Set(key, value, None)),
                                }
                            }
                            [b'G', b'E', b'T'] => {
                                let (key, _) = get_string(tail)?;
                                Ok(RequestType::Get(key))
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
    output.write_all(msg).await?;

    Ok(())
}

async fn set<T>(
    state: Arc<Mutex<HashMap<Vec<u8>, Entry>>>,
    key: &[u8],
    value: &[u8],
    expires_after: Option<Duration>,
    output: &mut T,
) -> Result<(), Box<dyn Error>>
where
    T: AsyncWrite + std::marker::Unpin,
{
    state
        .lock()
        .await
        .entry(key.to_vec())
        .and_modify(|i| *i = Entry::new(value, expires_after))
        .or_insert_with(|| Entry::new(value, expires_after));

    output.write_all(b"+OK\r\n").await?;

    Ok(())
}

async fn get<T>(
    state: Arc<Mutex<HashMap<Vec<u8>, Entry>>>,
    key: &[u8],
    output: &mut T,
) -> Result<(), Box<dyn Error>>
where
    T: AsyncWrite + Send + std::marker::Unpin,
{
    match state.lock().await.get(key) {
        Some(entry)
            if entry
                .expiry
                .as_ref()
                .map_or(true, |i| i.created_at.elapsed() <= i.expires_after) =>
        {
            let len = usize_to_ascii_bytes(entry.value.len());
            let mut buf = Vec::<u8>::with_capacity(5 + len.len() + entry.value.len());
            buf.extend_from_slice(b"$");
            buf.extend_from_slice(len.as_slice());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(&entry.value);
            buf.extend_from_slice(b"\r\n");
            output.write_all(buf.as_slice()).await?;
        }
        _ => output.write_all(b"$-1\r\n").await?,
    }
    Ok(())
}

async fn not_implemented<T>(output: &mut T) -> Result<(), Box<dyn Error>>
where
    T: AsyncWrite + std::marker::Unpin,
{
    output.write_all(b"-Error not implemented\r\n").await?;

    Ok(())
}

fn get_string(input: &[u8]) -> Result<(&[u8], &[u8]), Box<dyn Error>> {
    match input {
        [b'$', tail @ ..] | [b'\r', b'\n', b'$', tail @ ..] => {
            let (head, tail) = take_until_crlf(tail);
            let string_len = parse_bytes_to_usize(head);
            Ok((&tail[..string_len], &tail[string_len..]))
        }
        [b'\r', b'\n'] => Ok((&[], &[])),
        [] => Ok((&[], &[])),
        _ => Err("input is not a string - no leading $ found".into()),
    }
}

fn take_until_crlf(input: &[u8]) -> (&[u8], &[u8]) {
    let mut prev_char_is_cr = false;
    for i in 0..input.len() {
        if input[i] == b'\n' && prev_char_is_cr {
            return (&input[0..i - 1], &input[i + 1..]);
        } else if input[i] == b'\r' {
            prev_char_is_cr = true;
        } else {
            prev_char_is_cr = false;
        }
    }

    (input, &[])
}

fn parse_bytes_to_usize(input: &[u8]) -> usize {
    input
        .iter()
        .fold(0usize, |a, i| a * 10 + ((*i as usize) - 48))
}

fn usize_to_ascii_bytes(input: usize) -> Vec<u8> {
    input.to_string().into()
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use super::*;

    #[tokio::test]
    async fn single_ping() {
        let state = Arc::new(Mutex::new(HashMap::new()));
        let mut output = Vec::<u8>::new();
        assert!(handle_request(b"*1\r\n$4\r\nPING\r\n", state, &mut output)
            .await
            .is_ok());
        assert_eq!(b"+PONG\r\n", output[..].as_ref());
    }

    #[tokio::test]
    async fn single_echo() {
        let state = Arc::new(Mutex::new(HashMap::new()));
        let mut output = Vec::<u8>::new();
        assert!(
            handle_request(b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n", state, &mut output)
                .await
                .is_ok()
        );
        assert_eq!(b"$3\r\nhey\r\n", output[..].as_ref());
    }

    #[tokio::test]
    async fn single_set() {
        let state = Arc::new(Mutex::new(HashMap::new()));
        let mut output = Vec::<u8>::new();
        assert!(handle_request(
            b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$4\r\nbarr\r\n",
            state.clone(),
            &mut output
        )
        .await
        .is_ok());
        assert_eq!(b"+OK\r\n", output[..].as_ref());
    }

    #[tokio::test]
    async fn set_then_get() {
        let state = Arc::new(Mutex::new(HashMap::new()));
        let mut output = Vec::<u8>::new();
        assert!(handle_request(
            b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$4\r\nbarr\r\n",
            state.clone(),
            &mut output
        )
        .await
        .is_ok());
        assert_eq!(b"+OK\r\n", output[..].as_ref());
        output = Vec::new();
        assert!(handle_request(
            b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
            state.clone(),
            &mut output
        )
        .await
        .is_ok());
        assert_eq!(b"$4\r\nbarr\r\n", output[..].as_ref());
    }

    #[tokio::test]
    async fn set_with_expiry_then_get_within_timeframe() {
        let state = Arc::new(Mutex::new(HashMap::new()));
        let mut output = Vec::<u8>::new();
        assert!(handle_request(
            b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$4\r\nbarr\r\n$2\r\npx\r\n$3\r\n100\r\n",
            state.clone(),
            &mut output
        )
        .await
        .is_ok());
        assert_eq!(b"+OK\r\n", output[..].as_ref());
        thread::sleep(Duration::from_millis(15));
        output = Vec::new();
        assert!(handle_request(
            b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
            state.clone(),
            &mut output
        )
        .await
        .is_ok());
        assert_eq!(b"$4\r\nbarr\r\n", output[..].as_ref());
    }

    #[tokio::test]
    async fn set_with_expiry_then_get_after_timeframe() {
        let state = Arc::new(Mutex::new(HashMap::new()));
        let mut output = Vec::<u8>::new();
        assert!(handle_request(
            b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$4\r\nbarr\r\n$2\r\npx\r\n$2\r\n10\r\n",
            state.clone(),
            &mut output
        )
        .await
        .is_ok());
        assert_eq!(b"+OK\r\n", output[..].as_ref());
        thread::sleep(Duration::from_millis(15));
        output = Vec::new();
        assert!(handle_request(
            b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
            state.clone(),
            &mut output
        )
        .await
        .is_ok());
        assert_eq!(b"$-1\r\n", output[..].as_ref());
    }

    #[test]
    fn get_string_ok() {
        let input = b"$4\r\nECHO\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let (token, tail) = get_string(input).unwrap();
        assert_eq!(b"ECHO", token);
        let (token, tail) = get_string(tail).unwrap();
        assert_eq!(b"foo", token);
        let (token, tail) = get_string(tail).unwrap();
        assert_eq!(b"bar", token);
        let (token, _) = get_string(tail).unwrap();
        assert_eq!(&[] as &[u8], token);
    }

    #[test]
    fn get_string_err() {
        assert!(get_string(b"4\r\nECHO\r\n").is_err());
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

    #[test]
    fn usize_to_ascii_bytes_test() {
        assert_eq!(vec![53], usize_to_ascii_bytes(5));
        assert_eq!(vec![53, 48], usize_to_ascii_bytes(50));
    }
}
