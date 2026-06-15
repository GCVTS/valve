use crate::start::generate_random_port;

// for manager struct
use async_trait::async_trait;
use deadpool::managed;

use std::{
    io::{BufRead, BufReader},
    process::{Command, Stdio},
    time::Duration,
};

use axum::{
    body::Body,
    extract::{Extension, State},
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
};

use hyper::{client::HttpConnector, Uri};
type Client = hyper::client::Client<HttpConnector, Body>;

// Define the Plumber Struct
pub struct Plumber {
    pub host: String,
    pub port: u16,
    pub process: std::process::Child,
}

// Plumber methods for spawning, checking alive status and killing
impl Plumber {
    pub fn spawn(host: &str, filepath: &str) -> Self {
        let port = generate_random_port(host);

        #[cfg(debug_assertions)]
        println!("about to spawn plumber");

        let process = spawn_plumber(host, port, filepath);

        println!("Spawning plumber API at {host}:{port}");

        Self {
            host: host.to_string(),
            port,
            process,
        }
    }

    pub fn is_alive(&mut self) -> bool {
        let status = self.process.try_wait();
        match status {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(_) => false,
        }
    }

    pub async fn proxy_request(&mut self, client: Client, req: Request<Body>) -> Response {
        // Split the request apart so we can buffer the body. hyper consumes the
        // request body when it sends, so retrying an attempt requires a fresh
        // copy of the body for each try.
        let (mut parts, body) = req.into_parts();

        // Rewrite the URI to point at this pooled worker.
        let mut uri = parts.uri.clone().into_parts();
        uri.authority = Some(
            format!("{}:{}", self.host, self.port)
                .as_str()
                .parse()
                .unwrap(),
        );

        #[cfg(debug_assertions)]
        println!("about to proxy");
        // TODO enable https or other schemes
        // can the scheme figured out from the pr_host?
        uri.scheme = Some("http".parse().unwrap());
        parts.uri = match Uri::from_parts(uri) {
            Ok(u) => u,
            Err(e) => {
                eprintln!("valve proxy: could not build upstream uri: {e}");
                return error_response(StatusCode::BAD_GATEWAY, "invalid upstream uri");
            }
        };

        // Buffer the request body once so every retry can rebuild the request.
        let body_bytes = match hyper::body::to_bytes(body).await {
            Ok(b) => b,
            Err(e) => {
                eprintln!("valve proxy: failed to read request body: {e}");
                return error_response(StatusCode::BAD_GATEWAY, "could not read request body");
            }
        };

        // Bounded retry. This absorbs the brief window where a freshly spawned
        // worker is not yet accepting connections. Workers that stay dead are
        // evicted by `PrManager::recycle`, so the pool hands out live ones.
        const MAX_ATTEMPTS: u32 = 3;
        let mut last_err: Option<hyper::Error> = None;

        for attempt in 0..MAX_ATTEMPTS {
            let mut builder = Request::builder()
                .method(parts.method.clone())
                .uri(parts.uri.clone())
                .version(parts.version);
            if let Some(headers) = builder.headers_mut() {
                *headers = parts.headers.clone();
            }

            let attempt_req = match builder.body(Body::from(body_bytes.clone())) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("valve proxy: failed to build upstream request: {e}");
                    return error_response(StatusCode::BAD_GATEWAY, "could not build request");
                }
            };

            match client.request(attempt_req).await {
                Ok(resp) => return resp.into_response(),
                Err(e) => {
                    if attempt + 1 < MAX_ATTEMPTS {
                        // brief linear backoff before retrying the worker
                        tokio::time::sleep(Duration::from_millis(50 * (attempt as u64 + 1))).await;
                    }
                    last_err = Some(e);
                }
            }
        }

        eprintln!(
            "valve proxy: upstream worker {}:{} unavailable after {MAX_ATTEMPTS} attempts: {}",
            self.host,
            self.port,
            last_err
                .map(|e| e.to_string())
                .unwrap_or_else(|| "unknown error".to_string()),
        );
        error_response(StatusCode::BAD_GATEWAY, "upstream worker unavailable")
    }
}

// Build a small error response using the crate's local Body/Response types.
// Constructing a response from a valid status code and an in-memory body is
// infallible, so this never panics on a live worker failure.
fn error_response(status: StatusCode, msg: &str) -> Response {
    Response::builder()
        .status(status)
        .body(Body::from(msg.to_owned()))
        .unwrap()
        .into_response()
}

// This struct will contain the iterator that is used in the axum
// app to cycle through ports. though that might not be necessary
// since the Plumber struct contains the port
// the plumber struct will be returned by the pool and
// can be used in the axum route directly

pub struct PrManager {
    //    ports: Arc<Mutex<Cycle<std::vec::IntoIter<u16>>>>
    pub host: String,
    pub pr_file: String,
}

#[derive(Debug)]
pub enum Error {
    Fail,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Fail => write!(f, "plumber manager failure"),
        }
    }
}

impl std::error::Error for Error {}

#[async_trait]
impl managed::Manager for PrManager {
    type Type = Plumber;
    type Error = Error;

    async fn create(&self) -> Result<Plumber, Error> {
        let host = self.host.as_str();
        let filepath = self.pr_file.as_str();
        Ok(Plumber::spawn(host, filepath))
    }

    async fn recycle(&self, conn: &mut Plumber) -> managed::RecycleResult<Error> {
        // Liveness probe: if the worker is not accepting TCP connections, return
        // an error so deadpool discards it and spawns a fresh one via `create`,
        // rather than handing a dead worker back out to a request.
        let probe = tokio::time::timeout(
            Duration::from_millis(250),
            tokio::net::TcpStream::connect((conn.host.as_str(), conn.port)),
        )
        .await;

        match probe {
            Ok(Ok(_stream)) => Ok(()),
            Ok(Err(e)) => Err(managed::RecycleError::Message(format!(
                "plumber worker {}:{} not reachable: {e}",
                conn.host, conn.port
            ))),
            Err(_elapsed) => Err(managed::RecycleError::Message(format!(
                "plumber worker {}:{} health check timed out",
                conn.host, conn.port
            ))),
        }
    }

    fn detach(&self, obj: &mut Plumber) {
        let _killed_process = obj.process.kill();
    }
}

// spawn plumber
use std::process::Child;
pub fn spawn_plumber(host: &str, port: u16, filepath: &str) -> Child {
    // start the R processes
    let mut pr_child = Command::new("R")
        .arg("-e")
        // the defines the R command that is used to start plumber
        .arg(format!(
            "plumber::plumb('{filepath}')$run(host = '{host}', port = {port})"
        ))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to start R process");

    #[cfg(debug_assertions)]
    println!("theoretically have spawned plumber");

    // capture stderr
    let stderr = pr_child.stderr.take().expect("stdout to be read");
    let reader = BufReader::new(stderr);

    // read lines from buffer. when "Running swagger" is captured
    // then we sleep for 1/10th of a second to let the api start and continue
    for line in reader.lines().map_while(Result::ok) {
        if line.contains("Running swagger") || line.contains("Running rapidoc") {
            std::thread::sleep(Duration::from_millis(100));
            break;
        }
    }

    pr_child
}

type Pool = managed::Pool<PrManager>;
pub async fn plumber_handler(
    State(client): State<Client>,
    Extension(pr_pool): Extension<Pool>,
    req: Request<Body>,
) -> Response {
    #[cfg(debug_assertions)]
    println!("accessing handler");

    match pr_pool.get().await {
        Ok(mut pr) => pr.proxy_request(client, req).await,
        Err(e) => {
            eprintln!("valve: could not acquire a plumber worker from the pool: {e}");
            error_response(StatusCode::BAD_GATEWAY, "no plumber worker available")
        }
    }
}
