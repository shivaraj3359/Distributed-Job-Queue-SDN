# Project 3 - Distributed Job Queue and Worker Management

**Course:** Computer Networks / SDN Socket Programming  
**Transport:** TCP  
**SDN controller:** Ryu + OpenFlow 1.3  
**Network emulator:** Mininet + Open vSwitch  
**Recommended environment:** Ubuntu 22.04 LTS, Python 3.10

## 1. Problem statement

Create a distributed job-processing system where clients submit jobs through TCP sockets and worker nodes execute them. The SDN controller monitors forwarding paths and supports rerouting during link failures. The broker registers and monitors workers, queues and schedules jobs, and redistributes a job when the worker executing it disconnects.

## 2. Architecture

```text
 Client c1 (10.0.0.1) ----\
                           s1 ===== primary ===== s2 ---- Broker (10.0.0.10:5000)
 Client c2 (10.0.0.2) ----/  \                  /  \
                             s4 ----------------   s3 ---- Worker w1 (10.0.0.11)
                              \_____________________---- Worker w2 (10.0.0.12)

Switch graph edges: s1-s2, s1-s4, s2-s4, s2-s3, s3-s4
Primary paths: client <-> broker via s1-s2; broker <-> workers via s2-s3
Alternate paths: client <-> broker via s1-s4-s2; broker <-> workers via s2-s4-s3
```

- **Clients:** open TCP connections and submit JSON jobs to the broker.
- **Broker:** accepts concurrent TCP connections, acknowledges submissions, holds a FIFO queue, chooses an idle/low-load worker, tracks heartbeats, and returns results.
- **Workers:** register over persistent TCP connections, send heartbeats, execute assigned jobs, and return results.
- **Ryu controller:** installs OpenFlow 1.3 IPv4 forwarding rules for each host pair. When a link is disabled or reports link-down, it recalculates shortest paths and replaces the forwarding rules. The REST endpoint also lets you mark a path unavailable for a controlled demonstration.
- **Mininet:** emulates five end hosts and four Open vSwitch switches with both primary and alternate paths.

The two failure demonstrations are intentionally separate: (1) a **worker-process failure** causes the broker to requeue the in-flight job; (2) a **network-link failure** causes the controller to reroute packets through the alternate path.

## 3. Files in this project

| File | Purpose |
|---|---|
| `broker.py` | Concurrent TCP broker, FIFO job queue, worker registry, heartbeats, rescheduling, status response |
| `worker.py` | TCP worker agent with heartbeat sender, job execution, and reconnect loop |
| `client.py` | Submit a job or request worker/queue status over TCP |
| `benchmark.py` | Concurrent performance test; reports completion count, failures, throughput, average latency, and p95 latency |
| `topology.py` | Reproducible Mininet topology and permanent neighbor/ARP entries |
| `sdn_controller.py` | Ryu OpenFlow 1.3 forwarding, port-stat polling, path calculation, REST state/link-control API |
| `requirements-controller.txt` | Controller dependencies for the recommended Python version |
| `TEST_PLAN.md` | Test matrix and results table to complete during experiments |
| `REPORT_TEMPLATE.md` | Suggested report structure |
| `VIVA_QUESTIONS.md` | Preparation questions with short model answers |

## 4. Before starting

Use the Ubuntu VM specified in your course Mininet manual. Ubuntu 22.04 / Python 3.10 is recommended for this Ryu 4.34 setup. Avoid using Python 3.12 for the controller environment unless you deliberately set up a compatible maintained fork; old Ryu dependencies can fail on newer Python versions.

Open **three terminals** in the Ubuntu VM:

- **Terminal A:** Ryu controller.
- **Terminal B:** Mininet topology and CLI.
- **Terminal C:** controller REST checks and log inspection.

All commands below are entered in Ubuntu, not Windows CMD.

## 5. Installation, step by step

### Step 1 - Update Ubuntu and install Mininet / Open vSwitch

```bash
sudo apt update
sudo apt upgrade -y
sudo apt install -y python3 python3-venv python3-pip git mininet openvswitch-switch curl iproute2
sudo systemctl enable --now openvswitch-switch
sudo mn -c
```

If `systemctl` is unavailable for the Open vSwitch service, try:

```bash
sudo service openvswitch-switch start
```

Verify Mininet and Python:

```bash
python3 --version
mn --version
sudo ovs-vsctl show
```

For Ubuntu 22.04, Python is normally 3.10.x. Confirm that Mininet starts before proceeding:

```bash
sudo mn --test pingall --topo single,3
```

Expected: the default test finishes with `0% dropped` / no packet loss. If this test fails, fix the base Mininet/Open vSwitch installation before debugging the project.

### Step 2 - Extract the project folder

Extract the ZIP into your home directory so the folder is:

```text
~/project3_distributed_job_queue/
```

Then run:

```bash
cd ~/project3_distributed_job_queue
chmod +x *.py
python3 -m py_compile broker.py worker.py client.py benchmark.py topology.py sdn_controller.py
```

Expected: the syntax-check command exits without an error message.

### Step 3 - Create a separate Python environment for Ryu

Do not install the old Ryu dependency stack into Ubuntu's system Python. Use a virtual environment:

```bash
python3 -m venv ~/ryu-venv
source ~/ryu-venv/bin/activate
python -m pip install --upgrade "pip<24.1" "setuptools==57.5.0" wheel
python -m pip install "eventlet==0.33.3" "dnspython==2.6.1"
python -m pip install "ryu==4.34"
ryu-manager --version
```

Expected: the final command reports Ryu 4.34 or its version string. If installation fails, check `python3 --version` first and confirm you are inside `~/ryu-venv`. Keep Mininet and the topology script on the system Python; only use this virtual environment for `ryu-manager`.

### Step 4 - Start the SDN controller (Terminal A)

```bash
cd ~/project3_distributed_job_queue
source ~/ryu-venv/bin/activate
ryu-manager --verbose --ofp-tcp-listen-port 6653 --wsapi-port 8080 sdn_controller.py
```

Keep this terminal open. Expected log messages include:

```text
Project 3 SDN controller started; OpenFlow 1.3 / REST port 8080
```

Before Mininet starts, the REST state will show zero connected switches; that is normal.

### Step 5 - Start the Mininet topology (Terminal B)

Open a second terminal:

```bash
cd ~/project3_distributed_job_queue
sudo mn -c
sudo python3 topology.py
```

Wait until the prompt appears:

```text
mininet>
```

In Terminal A, you should see `s1`, `s2`, `s3`, and `s4` connect. Once all four connect, the controller should report `20/20 host pairs reachable`.

At the `mininet>` prompt, verify the network:

```text
nodes
net
pingall
```

Expected: hosts `c1`, `c2`, `broker`, `w1`, `w2`; switches `s1` through `s4`; and a successful `pingall` with 0% packet loss. Do not continue to application tests if the controller has not connected all switches or `pingall` fails.

### Step 6 - Start the broker inside the broker host's network namespace

At the `mininet>` prompt, start the broker in the background. Use the full path to your extracted folder. If your Ubuntu username differs, replace `/home/<USER>/project3_distributed_job_queue` with your actual path (check it in a normal terminal with `realpath ~/project3_distributed_job_queue`).

```text
broker python3 /home/<USER>/project3_distributed_job_queue/broker.py > /tmp/p3-broker.log 2>&1 &
```

The broker IP is `10.0.0.10`, and the TCP port is `5000`.

### Step 7 - Start both workers

At the same `mininet>` prompt:

```text
w1 python3 /home/<USER>/project3_distributed_job_queue/worker.py --id w1 --broker 10.0.0.10 > /tmp/p3-w1.log 2>&1 &
w2 python3 /home/<USER>/project3_distributed_job_queue/worker.py --id w2 --broker 10.0.0.10 > /tmp/p3-w2.log 2>&1 &
```

Give the processes a moment to connect. In Terminal C, check their logs:

```bash
cat /tmp/p3-broker.log
cat /tmp/p3-w1.log
cat /tmp/p3-w2.log
```

Expected broker log includes `Registered w1` and `Registered w2`. Each worker log should include `Connected; waiting for jobs` and `Registration confirmed`.

### Step 8 - Submit your first job

At the `mininet>` prompt:

```text
c1 python3 /home/<USER>/project3_distributed_job_queue/client.py --host 10.0.0.10 --type sum --values 5 10 15
```

Expected response shape (the job ID and timing change on every run):

```json
{
  "type": "ack",
  "job_id": "<generated-id>",
  "state": "queued",
  "queue_position": 1
}
{
  "type": "result",
  "state": "completed",
  "worker_id": "w1 or w2",
  "attempts": 1,
  "result": {"sum": 30.0, "count": 3}
}
```

You may see the acknowledgement and result printed as two separate JSON objects. That is expected: the broker acknowledges the queue submission before returning the completed result.

## 6. Application commands and expected results

Run these from `mininet>` after the broker and workers are running. Replace the folder path as above.

**Check worker and queue status**

```text
c1 python3 /home/<USER>/project3_distributed_job_queue/client.py --host 10.0.0.10 --type status
```

Expected status JSON includes `workers`, each worker's `status` (`idle` or `busy`), `jobs_completed`, `queued_jobs`, `active_jobs`, `total_submitted`, and `total_completed`.

**Sum job**

```text
c1 python3 /home/<USER>/project3_distributed_job_queue/client.py --host 10.0.0.10 --type sum --values 2 4 6 8
```

Expected result: `{"sum": 20.0, "count": 4}`.

**Factorial job**

```text
c2 python3 /home/<USER>/project3_distributed_job_queue/client.py --host 10.0.0.10 --type factorial --n 5
```

Expected result: `{"n": 5, "factorial": 120}`.

**Uppercase job**

```text
c1 python3 /home/<USER>/project3_distributed_job_queue/client.py --host 10.0.0.10 --type uppercase --text "hello sdn"
```

Expected result: `{"text": "HELLO SDN"}`.

**Long job for failure testing**

```text
c1 python3 /home/<USER>/project3_distributed_job_queue/client.py --host 10.0.0.10 --type sleep --seconds 5 --timeout 30
```

Expected result: `{"slept_seconds": 5.0}` if the assigned worker remains connected. This job is also useful for testing reassignment after killing a worker.

## 7. Demonstrate concurrent clients

At the `mininet>` prompt, launch several clients in the background:

```text
c1 python3 /home/<USER>/project3_distributed_job_queue/client.py --host 10.0.0.10 --type sleep --seconds 3 --timeout 30 > /tmp/p3-client1.log 2>&1 &
c2 python3 /home/<USER>/project3_distributed_job_queue/client.py --host 10.0.0.10 --type sum --values 1 2 3 4 5 > /tmp/p3-client2.log 2>&1 &
c1 python3 /home/<USER>/project3_distributed_job_queue/client.py --host 10.0.0.10 --type factorial --n 8 --timeout 30 > /tmp/p3-client3.log 2>&1 &
```

From Terminal C, inspect results and broker scheduling logs:

```bash
cat /tmp/p3-client1.log /tmp/p3-client2.log /tmp/p3-client3.log
tail -n 40 /tmp/p3-broker.log
```

Expected: multiple job IDs, successful results, and `[SCHEDULE] Job ... -> w1/w2` records. The broker handles each client connection in a separate thread and assigns no more than one active job at a time to each worker.

## 8. Test dynamic SDN path rerouting

The controller REST server runs in the Ubuntu host namespace, so run the `curl` commands below in **Terminal C**, not at `mininet>`.

### Baseline controller state

```bash
curl -s http://127.0.0.1:8080/sdn/state | python3 -m json.tool
```

Expected: four connected switches, five inter-switch links marked `up: true`, and 20 directed host-pair routes.

### Disable the primary client-to-broker path

First ask the SDN controller to exclude that edge from route computation:

```bash
curl -s -X POST http://127.0.0.1:8080/sdn/link/s1/s2/down | python3 -m json.tool
curl -s http://127.0.0.1:8080/sdn/state | python3 -m json.tool
```

The route `10.0.0.1->10.0.0.10` should now be `s1 -> s4 -> s2`. This REST operation is a reliable controller-level failure simulation; it deliberately makes the controller stop using the edge even before physically disabling it.

Now physically disable the Mininet link as well. In Terminal B at `mininet>`:

```text
link s1 s2 down
pingall
```

Expected: the controller routes over the alternate path, and hosts remain reachable. If the `pingall` command is run immediately during a route change, rerun it once after the controller reports the new routes.

Check the application end-to-end while the primary link is down:

```text
c1 python3 /home/<USER>/project3_distributed_job_queue/client.py --host 10.0.0.10 --type sum --values 10 20 30
```

Expected result: `{"sum": 60.0, "count": 3}`.

### Restore the primary path

At `mininet>`:

```text
link s1 s2 up
```

Then in Terminal C:

```bash
curl -s -X POST http://127.0.0.1:8080/sdn/link/s1/s2/up | python3 -m json.tool
curl -s http://127.0.0.1:8080/sdn/state | python3 -m json.tool
```

Expected: `s1-s2` is marked `up` and routes use the direct primary link again.

### Optional second link-failure demonstration

Use the broker-to-worker primary link:

1. Terminal C: `curl -s -X POST http://127.0.0.1:8080/sdn/link/s2/s3/down | python3 -m json.tool`
2. Terminal B: `link s2 s3 down`, then `pingall`.
3. Check `/sdn/state`: broker-to-worker path should use `s2 -> s4 -> s3`.
4. Restore physical and controller state: at `mininet>` type `link s2 s3 up`; in Terminal C run `curl -s -X POST http://127.0.0.1:8080/sdn/link/s2/s3/up | python3 -m json.tool`.

Do not leave a link marked down for the next test. Restore it and confirm `/sdn/state` shows `up: true`.

## 9. Demonstrate worker failure and job redistribution

This test proves broker-side fault tolerance, separate from SDN link rerouting.

1. In Terminal C, watch broker logs:

   ```bash
   tail -f /tmp/p3-broker.log
   ```

2. At `mininet>`, submit an eight-second job in the background:

   ```text
   c1 python3 /home/<USER>/project3_distributed_job_queue/client.py --host 10.0.0.10 --type sleep --seconds 8 --timeout 30 > /tmp/p3-failover-client.log 2>&1 &
   ```

3. Wait until broker logs show which worker received `attempt=1`, for example `[SCHEDULE] Job <id> -> w1 attempt=1`.
4. In another normal Ubuntu terminal, read the selected worker log (for example `cat /tmp/p3-w1.log`) and find its `PID=...` line. Kill **that exact worker PID**, not the broker or Mininet process:

   ```bash
   sudo kill -9 <PID_FROM_WORKER_LOG>
   ```

5. Watch broker logs for `[FAILOVER] ... job requeued`, followed by a second `[SCHEDULE]` line using the other worker with `attempt=2`.
6. Read the client result:

   ```bash
   cat /tmp/p3-failover-client.log
   ```

Expected: the same job ID completes on the surviving worker with `attempts: 2`. The `worker_id` should be the other worker. The broker implements at-least-once execution: if a worker finishes computation just as its TCP session fails, a job can be repeated, but only the currently assigned attempt is accepted as the result.

Restart the killed worker from the Mininet CLI if you want to continue testing:

```text
w1 python3 /home/<USER>/project3_distributed_job_queue/worker.py --id w1 --broker 10.0.0.10 > /tmp/p3-w1.log 2>&1 &
```

## 10. Performance evaluation

Run the supplied benchmark from a Mininet host. Example using 20 jobs and four simultaneous clients:

```text
c1 python3 /home/<USER>/project3_distributed_job_queue/benchmark.py --host 10.0.0.10 --jobs 20 --concurrency 4 --type sum
```

A workload with a visible queue can be created with short sleep jobs:

```text
c1 python3 /home/<USER>/project3_distributed_job_queue/benchmark.py --host 10.0.0.10 --jobs 20 --concurrency 8 --type sleep --seconds 0.2
```

Each run prints measured values: completed jobs, failures, wall-clock time, throughput (jobs/second), average end-to-end latency, p95 latency, and which workers executed jobs. Use `TEST_PLAN.md` to record measurements.

For a fair comparison:

1. Restore all primary links and run the same benchmark at least three times; record a baseline/static-path sample.
2. Disable one primary edge with the REST API (and optionally the Mininet link), run the exact same benchmark, and record the dynamic rerouting sample.
3. Compare completion/failure counts, average/p95 latency, throughput, and packet counters from `/sdn/state`.
4. Do not invent or pre-fill results. Use the values printed by your actual Ubuntu VM.

## 11. Collect evidence for the report/demo

Save screenshots or terminal copies showing:

- `pingall` success before failures and after rerouting.
- Broker log with worker registration, job acknowledgement/scheduling, and completed result.
- Client result for a normal job.
- Controller `/sdn/state` before/after a link failure, including the changed path.
- Broker `[FAILOVER]` log and the client's `attempts: 2` result after killing a worker.
- At least three benchmark runs for baseline and dynamic route conditions.

Useful commands from Terminal C:

```bash
cat /tmp/p3-broker.log
tail -n 50 /tmp/p3-broker.log
cat /tmp/p3-w1.log
cat /tmp/p3-w2.log
curl -s http://127.0.0.1:8080/sdn/state | python3 -m json.tool
```

## 12. Common problems and fixes

| Symptom | Check / fix |
|---|---|
| `mn: command not found` | Install Mininet with `sudo apt install -y mininet`; verify `mn --version`. |
| Open vSwitch service error | `sudo systemctl enable --now openvswitch-switch`; then `sudo mn -c`. |
| Ryu install error / setuptools error | Use Ubuntu 22.04 / Python 3.10, activate `~/ryu-venv`, pin `setuptools==57.5.0` before `pip install ryu==4.34`. |
| Ryu listens on wrong port | Confirm controller launched with `--ofp-tcp-listen-port 6653`; topology connects to `127.0.0.1:6653`. |
| Mininet switch not connecting | Keep Terminal A running; check Ryu logs, `sudo ovs-vsctl show`, and port 6653 arguments; clean with `sudo mn -c` and restart. |
| `pingall` fails | Check all four switches connected, all controller links marked up, and permanent neighbor entries were installed by `topology.py`; rerun `pingall` after the controller finishes installing routes. |
| Client says connection refused | Start `broker.py` inside the `broker` Mininet host first. Verify the broker log and port `5000`. |
| Workers do not register | Check broker IP `10.0.0.10`, broker TCP port `5000`, and worker log files. |
| `worker_id already connected` | Do not start two copies with the same ID. Stop the earlier worker; wait for disconnection; start one copy. |
| Rerouted path still shows down | Use the matching `/sdn/link/s1/s2/up` REST call after physically bringing the Mininet link up. Verify `/sdn/state`. |
| `curl` to REST fails | Ryu must be running with `--wsapi-port 8080`; run curl from Ubuntu's normal shell, not from inside `c1` or the Mininet CLI. |
| Background worker killed / log is empty | Check the path in redirection and launch the worker using the absolute project path; inspect `/tmp/p3-w1.log` / `/tmp/p3-w2.log`. |

## 13. Clean shutdown

1. At `mininet>` type `exit`.
2. In Terminal A press `Ctrl+C` to stop Ryu.
3. Run from a normal Ubuntu shell:

   ```bash
   sudo mn -c
   ```

The Python socket application uses only the standard library; Ryu is the controller-specific dependency. Mininet/Open vSwitch require root privileges for topology creation.

## 14. Rubric coverage

| Rubric requirement | Project evidence |
|---|---|
| Problem understanding & architecture | This README and `REPORT_TEMPLATE.md` |
| TCP socket implementation | Concurrent broker, TCP clients, persistent worker TCP connections, newline-delimited JSON protocol |
| Mininet + SDN implementation | `topology.py`, four OVS switches, Ryu OpenFlow 1.3 controller |
| Socket-SDN integration | Client/broker/worker packets traverse OpenFlow-installed routes; the same socket workload is rerun during path change |
| Complete application functionality | Job acknowledgement, queueing, scheduling, worker status, result messages, supported job types |
| Dynamic SDN | REST-controlled link availability and OpenFlow port-status handling recompute paths |
| Challenging scenario / robustness | Worker killed during an assigned job; broker requeues the job to a live worker; optional physical link failure |
| Performance evaluation | `benchmark.py`, throughput/latency/failure metrics, baseline vs reroute comparison |
| Documentation / final demo / viva | README, report template, test plan, viva questions, actual terminal evidence |

Before submission, complete all tests on your Ubuntu VM and place your own measured values/screenshots in the report. The code has been syntax-checked and its TCP queue/worker failover logic has been exercised locally; Mininet/Ryu end-to-end behavior still needs to be verified in your VM.
