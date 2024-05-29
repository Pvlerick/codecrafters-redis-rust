use std::error::Error;

use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (mut stream, _) = listener.accept().await?;
        println!("accepted new connection");

        let mut buf = [0u8; 1024];
        let bytes_read = stream.read(&mut buf).await?;

        if bytes_read > 0 {
            println!("{} bytes read on the stream", bytes_read);
            handle_request(&buf[..bytes_read], &mut stream).await?;
        }
    }
}

async fn handle_request<T>(req: &[u8], output: &mut T) -> Result<(), Box<dyn Error>>
where
    T: AsyncWrite + std::marker::Unpin,
{
    match req {
        b"*1\r\n$4\r\nPING\r\n" => return ping(output).await,
        _ => {
            println!("req: {:?}", req);
            return Ok(());
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ping_test() {
        let mut output = Vec::<u8>::new();
        assert!(handle_request(b"*1\r\n$4\r\nPING\r\n", &mut output)
            .await
            .is_ok());
        assert_eq!(b"+PONG\r\n", output[..].as_ref());
    }
}
