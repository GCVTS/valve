use crate::plumber::*;

use hyper::client::HttpConnector;
use rand::Rng;
type Client = hyper::client::Client<HttpConnector, Body>;

use axum::{body::Body, extract::Extension, response::Redirect, routing::get};

use std::time::Duration;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::{net::TcpListener, sync::Arc};

use deadpool::managed;
type Pool = managed::Pool<PrManager>;

pub async fn valve_start(
    filepath: String,
    host: String,
    port: u16,
    n_min: usize,
    n_max: usize,
    check_interval: i32,
    max_age: i32,
) {
    // determines how often to check connects
    let interval = Duration::from_secs(check_interval.try_into().unwrap());
    // determines how old a connection can be before being killed
    let max_age = Duration::from_secs(max_age.try_into().unwrap());

    let filepath = Arc::new(filepath);
    let axum_host = Arc::new(host);
    let axum_port = port;

    // spawn client used for proxying
    let c = Client::new();

    // create Pool manager
    let plumber_manager = PrManager {
        host: axum_host.to_string(),
        pr_file: filepath.to_string(),
    };

    // Build the Plumber API connection Pool
    let pool = Pool::builder(plumber_manager)
        .max_size(n_max)
        .build()
        .unwrap();

    // Pre-warm the pool so `n_min` live workers are serving from the moment
    // valve boots, not only after on-demand scaling. We acquire the workers
    // concurrently and hold them all at once -- since each held worker is
    // checked out, the pool is forced to create a fresh one for the next
    // acquire -- then drop them to return them to the idle pool. `n_min` is
    // capped at `n_max` (the pool's max size) and floored at 1 so at least the
    // first worker is always spawned.
    let warm_target = n_min.min(n_max).max(1);
    let mut warming = Vec::with_capacity(warm_target);
    for _ in 0..warm_target {
        let pool = pool.clone();
        warming.push(tokio::spawn(async move { pool.get().await }));
    }
    let mut warm = Vec::with_capacity(warm_target);
    for handle in warming {
        match handle.await {
            Ok(Ok(obj)) => warm.push(obj),
            Ok(Err(e)) => eprintln!("valve: failed to pre-spawn a plumber worker: {e}"),
            Err(e) => eprintln!("valve: pre-spawn task panicked: {e}"),
        }
    }
    if warm.is_empty() {
        panic!("valve: could not spawn any plumber workers at startup");
    }
    // Return the pre-warmed workers to the idle pool.
    drop(warm);

    // define the APP
    let app = axum::Router::new()
        .route("/", get(|| async { Redirect::permanent("/__docs__/") }))
        .route("/*key", axum::routing::any(plumber_handler))
        .with_state(c)
        .layer(Extension(pool.clone()));

    // This thread is used to check if there are expired threads
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;

            // Never prune below `n_min`: cap how many workers a single pass may
            // remove so the pool can't drop under the configured minimum, even
            // when several workers expire in the same pass.
            let removable = pool.status().size.saturating_sub(n_min);
            if removable == 0 {
                continue;
            }

            let removed = AtomicUsize::new(0);
            pool.retain(|pr, metrics| {
                let expired = metrics.last_used() >= max_age;
                if expired && removed.load(Ordering::Relaxed) < removable {
                    removed.fetch_add(1, Ordering::Relaxed);
                    println!("Killing plumber API at {}:{}", pr.host, pr.port);
                    false
                } else {
                    true
                }
            });
        }
    });

    // Start the Axum server
    let full_axum_host = format!("{axum_host}:{axum_port}");
    axum::Server::try_bind(&full_axum_host.as_str().parse().unwrap())
        .unwrap()
        .serve(app.into_make_service())
        .await
        .unwrap();
}

// from chatGPT
// these functions generate random ports and
// check if they are in use
pub fn generate_random_port(host: &str) -> u16 {
    let mut rng = rand::thread_rng();
    loop {
        let port: u16 = rng.gen_range(1024..=65535);
        if is_port_available(host, port) {
            return port;
        }
    }
}

// checks to see if the port is available
fn is_port_available(host: &str, port: u16) -> bool {
    match TcpListener::bind(format!("{host}:{port}")) {
        Ok(listener) => {
            // The port is available, so we close the listener and return true
            drop(listener);
            true
        }
        Err(_) => false, // The port is not available
    }
}
