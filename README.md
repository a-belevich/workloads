The `workloads` project simulates errors propagation between multiple services depending on their settings.

In current version, it can simulate:
 - A chain of calls of arbitrary length: several deployments, where for success each of the requests should go downstream until the very end, and then return.
 - Arbitrary number of clients and services in every deployment.
 - Services that can:
   - Always success
   - Always fail
   - Return a random error in configured percentage of cases.
   - TODO: Returns a deterministic error in configured percentage of cases (requests with the same ID will deterministically succeed or fail).
   - Returns an error once per configured period.
 - Retries with backoff.
 - Performance degradation when service calculates results for more than X requests simultaneously.
 - Different types of concurrency limiters:
   - Additive increase multiplicative decrease (triggered either by errors or by latency)
   - Static
   - Unlimited
 - Different reactions on reaching the limits (waiting or discarding).
 - Different load balancing strategies:
   - Request-level round-robin (emulates round-robin envoy)
   - Request-level least busy (emulates envoy with Least Requests).
   - TODO: ClusterIP (connection-level round robin).


Running locally:
 - Install Java 21 and Maven.
 - To compile the code, `mvn package`.
 - To run the code, `java -jar workloads-1.0-SNAPSHOT-jar-with-dependencies.jar `