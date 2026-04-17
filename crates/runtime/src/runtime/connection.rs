use cryptofeed_core::error::{Error, Result};
use futures::StreamExt;
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio_tungstenite::{
    connect_async,
    tungstenite::Message,
    MaybeTlsStream,
    WebSocketStream,
};
use url::Url;

pub struct WsConnection {
    pub url: Url,
}

impl WsConnection {
    pub fn new(url: Url) -> Self {
        Self { url }
    }

    pub async fn connect(&self) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
        let (stream, _) = connect_async(self.url.as_str())
            .await
            .map_err(|e| Error::Transport(e.to_string()))?;
        Ok(stream)
    }
}

pub async fn next_text_message(
    stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
) -> Result<Option<String>> {
    while let Some(message) = stream.next().await {
        let message = message.map_err(|e| Error::Transport(e.to_string()))?;
        match message {
            Message::Text(text) => return Ok(Some(text.to_string())),
            Message::Binary(_) | Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => continue,
            Message::Close(_) => return Ok(None),
        }
    }

    Ok(None)
}

pub async fn next_text_message_or_shutdown(
    stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Option<String>> {
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                match changed {
                    Ok(()) if *shutdown.borrow() => return Ok(None),
                    Ok(()) => continue,
                    Err(_) => return Ok(None),
                }
            }
            message = stream.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => return Ok(Some(text.to_string())),
                    Some(Ok(Message::Binary(_) | Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => continue,
                    Some(Ok(Message::Close(_))) => return Ok(None),
                    Some(Err(e)) => return Err(Error::Transport(e.to_string())),
                    None => return Ok(None),
                }
            }
        }
    }
}
