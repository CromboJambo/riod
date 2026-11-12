//! Unix socket JSON-RPC 2.0 transport for communicating with the riod daemon.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{Read, Write};
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Errors from the riod client.
#[derive(Debug, Error)]
pub enum ClientError {
    #[error("Failed to connect to riod socket at {path}: {source}")]
    ConnectFailed {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("Failed to send request: {0}")]
    SendFailed(String),

    #[error("Failed to read response: {0}")]
    ReadFailed(String),

    #[error("Invalid JSON-RPC response: {0}")]
    InvalidResponse(String),

    #[error("JSON-RPC error {code}: {message}")]
    RpcError {
        code: i64,
        message: String,
        data: Option<String>,
    },

    #[error("Failed to serialize request: {0}")]
    Serialize(#[from] serde_json::Error),

    #[error("Failed to deserialize response: {0}")]
    Deserialize(String),

    #[error("Daemon returned no data: {0}")]
    NoData(String),

    #[error("Async task failed: {0}")]
    Async(String),
}

/// A JSON-RPC 2.0 request.
#[derive(Debug, Serialize)]
struct Request<'a> {
    jsonrpc: &'static str,
    method: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<Value>,
    id: u64,
}

/// A JSON-RPC 2.0 response.
#[derive(Debug, Deserialize)]
struct Response {
    jsonrpc: String,
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<RpcError>,
    #[allow(dead_code)]
    id: u64,
}

/// A JSON-RPC 2.0 error object.
#[derive(Debug, Deserialize)]
struct RpcError {
    code: i64,
    message: String,
    #[serde(default)]
    data: Option<Value>,
}

/// Low-level transport for sending JSON-RPC requests over a Unix socket.
///
/// Each call opens a new connection to the daemon, sends one request,
/// reads one response, and closes the connection. This is safe for
/// concurrent use from multiple threads.
#[derive(Clone)]
pub struct Transport {
    socket_path: PathBuf,
}

impl Transport {
    /// Create a new transport targeting the given socket path.
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
        }
    }

    /// Get the socket path this transport connects to.
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Send a JSON-RPC request and return the result value.
    pub fn call(&self, method: &str, params: Option<Value>) -> Result<Value, ClientError> {
        let request = Request {
            jsonrpc: "2.0",
            method,
            params,
            id: 1,
        };

        // Send the request (newline-terminated, daemon reads line-by-line)
        let mut payload = serde_json::to_string(&request)?;
        payload.push('\n');

        // Connect to the Unix socket
        let mut stream =
            UnixStream::connect(&self.socket_path).map_err(|e| ClientError::ConnectFailed {
                path: self.socket_path.display().to_string(),
                source: e,
            })?;

        // Send the request
        stream
            .write_all(payload.as_bytes())
            .map_err(|e| ClientError::SendFailed(e.to_string()))?;
        stream
            .flush()
            .map_err(|e| ClientError::SendFailed(e.to_string()))?;

        // Read the response line (daemon sends one line per request and keeps connection open)
        let mut buffer = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            match stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => {
                    buffer.extend_from_slice(&chunk[..n]);
                    // Check if we've received a complete JSON line
                    if let Some(pos) = buffer.iter().position(|&b| b == b'\n') {
                        buffer.truncate(pos + 1);
                        break;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(e) => return Err(ClientError::ReadFailed(e.to_string())),
            }
        }

        // Also shut down the write side so the daemon knows we're done with this request
        let _ = stream.shutdown(Shutdown::Write);

        let response: Response = serde_json::from_slice(&buffer).map_err(|e| {
            ClientError::InvalidResponse(format!(
                "{} (raw: {})",
                e,
                String::from_utf8_lossy(&buffer)
            ))
        })?;

        if response.jsonrpc != "2.0" {
            return Err(ClientError::InvalidResponse(format!(
                "Unexpected jsonrpc version: {}",
                response.jsonrpc
            )));
        }

        if let Some(err) = response.error {
            return Err(ClientError::RpcError {
                code: err.code,
                message: err.message,
                data: err.data.map(|v| v.to_string()),
            });
        }

        Ok(response.result.unwrap_or(Value::Null))
    }
}

impl Default for Transport {
    fn default() -> Self {
        Self::new("/run/riod.sock")
    }
}
