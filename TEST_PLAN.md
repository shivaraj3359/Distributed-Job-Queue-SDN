# Project 3 Test Plan and Results Sheet

Record actual measurements from the Ubuntu VM. Do not write expected values as measured results.

## Functional and failure tests

| Test ID | Test | Procedure | Expected result | Actual result / evidence |
|---|---|---|---|---|
| T1 | Mininet setup | Run `sudo python3 topology.py`, then `pingall` | All five hosts can reach each other; 0% packet loss | |
| T2 | Switch/controller connection | Read Ryu logs and `/sdn/state` | s1-s4 connected; routes installed | |
| T3 | Worker registration | Start broker, w1, w2; inspect broker log | Both workers register | |
| T4 | TCP job/ack/result | Submit sum job with 5, 10, 15 | Ack received; result sum = 30 | |
| T5 | Concurrent clients | Launch at least three client commands concurrently | Each valid job receives one accepted result | |
| T6 | Worker monitoring | Run client `--type status` | Workers, load/state, queue and completion counters appear | |
| T7 | SDN path failover | Disable s1-s2 via REST and Mininet, inspect state, run `pingall` and a sum job | c1-to-broker path changes to s1-s4-s2; job still completes | |
| T8 | Worker crash/requeue | Submit sleep job; kill the worker assigned attempt 1 | Broker logs failover and another worker completes attempt 2 | |
| T9 | Port counters | Read `/sdn/state` after traffic has run for at least 5 seconds | Broker/worker switch-port counters appear or update | |
| T10 | Cleanup | Exit Mininet and run `sudo mn -c` | No stale Mininet topology remains | |

## Performance measurements

Run the same workload at least three times per condition. Keep job count, concurrency, job type, and sleep duration identical for baseline and rerouted-path tests.

| Condition | Run | Jobs requested | Concurrency | Completed | Failed | Wall time (s) | Throughput (jobs/s) | Average latency (s) | p95 latency (s) | Notes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Primary paths up | 1 | | | | | | | | | |
| Primary paths up | 2 | | | | | | | | | |
| Primary paths up | 3 | | | | | | | | | |
| Alternate path active | 1 | | | | | | | | | |
| Alternate path active | 2 | | | | | | | | | |
| Alternate path active | 3 | | | | | | | | | |

## What to explain in the results

- Did the job completion rate change between baseline and rerouted conditions?
- How did average and p95 latency change? Mention run-to-run variation.
- Did any submitted jobs fail during the link failure, or did the alternate path preserve connectivity?
- How many attempts were needed for the killed-worker test?
- What do the port counters indicate about broker/worker traffic?
- Explain why a small Mininet topology is not a substitute for a production-network performance test.
