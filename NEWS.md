# valve 0.1.4

* Valve no longer panics its Tokio runtime worker threads when a pooled plumber
  worker is dead or not yet accepting connections. Previously, under concurrent
  load exceeding the number of ready workers, requests could stall (~2s) and
  return empty/dropped replies as runtime threads were killed.
  - Proxying now buffers the request body and performs a bounded retry, falling
    back to a clean `502 Bad Gateway` instead of unwrapping the transport error.
  - The connection pool now health-checks workers on checkout (`recycle`) with a
    short TCP liveness probe, so dead workers are evicted and respawned instead
    of being handed back out. The pool self-heals after a worker dies.
  - Failing to acquire a worker from the pool now returns `502` rather than
    panicking the request handler.
* Spawned plumber worker processes are no longer orphaned. Worker teardown now
  lives in a `Drop` implementation, so the underlying R process is terminated on
  every path that removes a worker — eviction, pruning, pool resize, and pool
  shutdown — not only the few paths where `deadpool` calls `detach`.

# valve 0.1.2.9000


* Adds `n_min` argument (default 1). Specifies the minimum number of plumber APIs always running. Previously, some requests might fail if all connections had gone stale and been pruned. 
  - Valve will now always spawn the first connection automatically. Additional connections will be spawned on-demand. Once `n_min` has been reached, the number of connections will never be lower. 
