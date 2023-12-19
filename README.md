The `workloads` project simulates errors propagation between multiple services depending on their settings.

In current version, it can simulate:
 - A chain of calls of arbitrary length: several deployments, where for success each of the requests should go downstream until the very end, and then return.
 - Arbitrary number of clients and services in every deployments.
 - Services that always success and services that always fail.
 - Retries with backoff.
 - Performance degradation when service calculates results for more than X requests simultaneously.
 - Different types of concurrency limiters:
   - Additive increase multiplicative decrease (triggered either by errors or by latency)
   - Static
   - Unlimited
 - Different reactions on reaching the limits (waiting or discarding).
 - Different load balancing strategies (round-robin or least busy).
