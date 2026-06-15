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
* Worker processes are now cleaned up even when valve is *force-killed* (where no
  Rust destructor runs). On Windows each worker is enrolled in a kill-on-close
  Job Object, so the OS terminates them when the valve process dies by any means
  (including `TerminateProcess`). On Linux each worker arms `PR_SET_PDEATHSIG`
  (`SIGKILL`) so the kernel kills it if valve exits. (macOS still relies on the
  graceful `Drop` cleanup.)
* `n_min` is now honored from startup and as a hard floor. Valve pre-warms the
  pool to `n_min` live workers before it starts accepting requests (previously
  only a single worker was spawned at boot and the rest scaled up on demand).
  Pruning also now removes at most `size - n_min` workers per pass; previously
  the idle prune removed every expired worker in a single pass once the pool was
  larger than `n_min`, so a quiet pool could be pruned all the way to zero. The
  worker count never drops below `n_min`.

# valve 0.1.2.9000


* Adds `n_min` argument (default 1). Specifies the minimum number of plumber APIs always running. Previously, some requests might fail if all connections had gone stale and been pruned. 
  - Valve will now always spawn the first connection automatically. Additional connections will be spawned on-demand. Once `n_min` has been reached, the number of connections will never be lower. 
