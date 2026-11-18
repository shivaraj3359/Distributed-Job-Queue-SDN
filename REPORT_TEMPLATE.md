# Distributed Job Queue and Worker Management - Project Report

## 1. Abstract
Briefly describe the TCP-based distributed processing service, worker scheduling, Mininet topology, and OpenFlow path control. Add measured outcomes only after you run the experiments.

## 2. Problem statement and objectives
Use the project statement from the course workbook. State objectives: concurrent client submissions, job acknowledgement, worker registration/monitoring, queue/scheduling, dynamic SDN routes, failover, and performance evaluation.

## 3. System architecture
Insert a labelled architecture diagram showing clients c1/c2, broker (10.0.0.10:5000), workers w1/w2, switches s1-s4, primary/alternate paths, Ryu OpenFlow port 6653, and REST API port 8080.

## 4. Protocol and message format
Document newline-delimited JSON over TCP. Include examples for `register`, `heartbeat`, `submit`, `ack`, `job`, `result`, and `status`. Explain job ID and attempt number.

## 5. Design and implementation
### 5.1 Broker
Explain concurrent connection threads, FIFO queue, idle/low-load selection, heartbeat timeout, job acknowledgement, result matching, and requeue behavior.
### 5.2 Worker
Explain persistent TCP registration, periodic heartbeats, job execution, result reporting, and reconnection.
### 5.3 Client
Explain submission/status operation and waiting for acknowledgement plus result.
### 5.4 SDN controller
Explain OpenFlow 1.3, host-pair IPv4 forwarding entries, path computation, REST link control, port-status notifications, and port counters.
### 5.5 Mininet topology
Explain five hosts, four OVS switches, two primary direct links and alternate paths.

## 6. Implementation setup
List Ubuntu version, Python version, Mininet/Open vSwitch version, Ryu version, the installation commands, and how the demo is launched. Include commands sufficient to reproduce the test.

## 7. Test cases and results
Copy and complete `TEST_PLAN.md`. Include screenshots/log snippets for the baseline, reroute, and worker-failure tests.

## 8. Performance evaluation
Report baseline and alternate-path workload settings. Include completed/failed jobs, wall time, throughput, average latency, p95 latency, and port counters. Discuss observed differences without inventing causality not shown by the data.

## 9. Failure handling and limitations
Explain job requeue after worker TCP disconnect, attempt matching, controller path recomputation, and at-least-once execution. Mention that the prototype uses a single broker (no replicated broker state) and simulated/example jobs rather than arbitrary distributed executables.

## 10. Conclusion and future work
Summarize the validated behavior. Possible extensions: persistent job storage, worker capacity greater than one, authentication, TLS, multiple broker replicas, queue prioritization, and automated integration between worker telemetry and SDN traffic policy.

## 11. References
- Mininet Walkthrough: https://mininet.org/walkthrough/
- Mininet installation guide: https://mininet.org/download/
- Ryu documentation / installation: https://ryu.readthedocs.io/en/latest/getting_started.html
- Open vSwitch OpenFlow FAQ: https://docs.openvswitch.org/en/stable/faq/openflow/
