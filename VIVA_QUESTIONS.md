# Viva Questions - Project 3

1. **Why did you choose TCP?**  TCP provides reliable, ordered delivery for job requests, acknowledgements, worker assignments, and results. Its connection state also helps the broker detect worker disconnections.
2. **What is a job broker?**  It is the central application service that accepts submissions, queues jobs, selects workers, and returns completed results.
3. **How are messages framed over TCP?**  Each JSON message ends in a newline, allowing the receiver to read one complete message at a time from the TCP stream.
4. **Why is a newline delimiter necessary?**  TCP is a byte stream and does not preserve application message boundaries. The delimiter defines where one JSON message ends.
5. **How are workers registered?**  Each worker opens a persistent TCP connection and sends a `register` message with a unique worker ID.
6. **Why use heartbeats?**  They provide a lightweight liveness signal so the broker can detect silent/disconnected workers.
7. **What happens if a worker crashes mid-job?**  The broker observes EOF or heartbeat timeout, marks the worker offline, puts the in-flight job back at the front of the queue, and schedules another attempt on a live worker.
8. **Why do jobs have an attempt number?**  It distinguishes reassignments and prevents a stale result from an earlier attempt being accepted as the current result.
9. **Does the design guarantee exactly-once execution?**  No. It provides at-least-once attempts: a crash near completion can cause repeated computation. Exactly-once effects need durable state and an idempotency/transaction design.
10. **What does Mininet do?**  It emulates end hosts and links/switches so networking applications can be tested on one Linux machine.
11. **What does the SDN controller do?**  It programs OpenFlow switch forwarding rules and recalculates routes when a link is unavailable.
12. **What is OpenFlow 1.3 used for here?**  It is the protocol between Ryu and the Open vSwitch instances for installing and removing IPv4 forwarding flow entries.
13. **What is the difference between the control plane and data plane?**  The control plane decides forwarding behavior; the data plane forwards packets using the rules installed in the switches.
14. **How does alternate-path rerouting work?**  The controller marks an edge unavailable, computes a shortest path over the remaining graph, deletes old project routes, then installs replacement flow rules.
15. **What is the role of `topology.py`?**  It creates the Mininet hosts/switches/links with explicit ports and installs permanent neighbor entries for deterministic unicast routing.
16. **Why install permanent neighbor entries?**  To avoid ARP broadcast flooding in the looped multi-path topology; the controller focuses on IPv4 route flows.
17. **What is the broker's scheduling policy?**  FIFO pending jobs are assigned to an online idle worker selected using reported load, completed-job count, and worker ID as tie-breakers.
18. **How do you test dynamic SDN?**  Disable a primary link using the controller REST endpoint (and optionally the Mininet physical link), inspect `/sdn/state`, then verify connectivity and job completion.
19. **Which performance metrics do you collect?**  Completion/failure count, total wall time, throughput in jobs/second, average latency, p95 latency, and OpenFlow port packet/byte counters.
20. **What is the limitation of this prototype?**  A single broker is a single point of failure; the application has in-memory job state, only example job types, and no authentication/encryption.
21. **How would you improve the project?**  Add durable queue storage, TLS/authentication, multiple broker replicas, worker capacity limits, priorities, idempotent execution, and automated controller policies tied to broker telemetry.
22. **Why compare primary and alternate paths?**  It provides a baseline and checks whether traffic remains functional when the controller selects an alternate path; results should be measured rather than assumed.
