# Distributed Job Queue and Worker Management using TCP Socket Programming and SDN

**Project repository:**
https://github.com/shivaraj3359/Distributed-Job-Queue-SDN\
**Platform used for implementation:** Ubuntu running in VirtualBox\
**Main technologies:** Python, TCP sockets, Mininet, Open vSwitch, Ryu,
OpenFlow 1.3

## 1. Project overview

This project implements a distributed job-processing system in which
clients submit jobs to a central broker. The broker maintains a queue,
tracks registered workers, assigns pending jobs to available workers,
and returns job results to clients. A Ryu Software-Defined Networking
(SDN) controller manages the Mininet network and provides a REST
interface for viewing network state and triggering link-state changes.

The project demonstrates two forms of resilience:

-   **Worker failure recovery:** if a worker disconnects while
    processing a job, the broker detects the lost worker and places the
    unfinished job back in the queue so another worker can execute it.
-   **Network path management:** the Ryu controller manages OpenFlow
    switches and responds to topology/link events so that network paths
    can be updated when a link becomes unavailable.

The implementation is intended as a teaching/demo system. A requeued job
may execute more than once if a worker fails after performing work but
before the broker receives the completion message; therefore, the system
demonstrates retry/recovery rather than guaranteed exactly-once
execution.

## 2. System architecture

The system contains the following logical components:

  -----------------------------------------------------------------------
  Component                           Purpose
  ----------------------------------- -----------------------------------
  Client (`client.py`)                Submits jobs and requests
                                      broker/worker status.

  Broker (`broker.py`)                Accepts client and worker TCP
                                      connections, queues jobs, schedules
                                      work, tracks workers, and collects
                                      results.

  Worker (`worker.py`)                Registers with the broker, sends
                                      heartbeats, receives assigned jobs,
                                      executes them, and reports results.

  SDN controller                      Ryu application that communicates
  (`sdn_controller.py`)               with OpenFlow switches, tracks
                                      topology information, and exposes
                                      REST endpoints.

  Mininet topology (`topology.py`)    Creates the virtual hosts,
                                      switches, and links used for
                                      network experiments.

  Benchmark (`benchmark.py`)          Runs multiple jobs and reports
                                      performance measurements.
  -----------------------------------------------------------------------

### Logical network

The Mininet environment uses two clients, one broker, two workers, and
four Open vSwitch switches.

  Node                    IP address / service
  ----------------------- ------------------------------
  Client 1 (`c1`)         `10.0.0.1`
  Client 2 (`c2`)         `10.0.0.2`
  Broker                  `10.0.0.10`, TCP port `5000`
  Worker 1 (`w1`)         `10.0.0.11`
  Worker 2 (`w2`)         `10.0.0.12`
  Ryu OpenFlow listener   TCP port `6653`
  Ryu REST API            TCP port `8080`

The topology uses switches `s1`--`s4` and multiple inter-switch links.
This provides alternate paths for testing link-state changes. The
controller is configured to communicate with the Mininet switches using
OpenFlow 1.3.

> Use the IP addresses and ports configured in the project files if they
> differ from this table.

## 3. How the system works

1.  The Ryu controller starts and listens for OpenFlow switch
    connections.
2.  Mininet starts the virtual hosts, switches, and links, and the
    switches connect to the controller.
3.  The broker starts and listens for TCP connections on
    `10.0.0.10:5000`.
4.  Each worker connects to the broker and registers its worker ID and
    hostname. Workers send periodic heartbeats and wait for jobs.
5.  A client sends a job request to the broker.
6.  The broker adds the job to its pending queue and assigns it to an
    available worker.
7.  The worker executes the requested operation and sends the result to
    the broker.
8.  The broker records the completion and makes the result available to
    the client.
9.  If a worker disconnects during a job, the broker detects the lost
    worker and requeues the unfinished job. Another available worker can
    then process it.
10. The SDN controller monitors switch/link information and provides
    REST operations for inspecting controller state and changing link
    state during experiments.

## 4. Software and environment

The implementation was run in an Ubuntu virtual machine. The Ryu
environment used a separate Python 3.10 virtual environment
(`ryu310-venv`) because the system Python version on the machine was
newer than the version used for the Ryu setup.

The system Python installation should be left unchanged. Activate the
Ryu virtual environment only when running the Ryu controller.

Required software:

-   Ubuntu Linux
-   Python 3 and Python 3.10 with `venv`
-   Mininet
-   Open vSwitch
-   Ryu SDN framework
-   Git
-   `curl` and standard networking utilities

Python dependencies for the controller are recorded in
`requirements-controller.txt` where applicable.

## 5. Get the source code

Clone the public repository:

``` bash
git clone https://github.com/shivaraj3359/Distributed-Job-Queue-SDN.git
cd Distributed-Job-Queue-SDN
```

If the repository is already cloned, enter the project directory:

``` bash
cd ~/project3_distributed_job_queue
```

## 6. Start the system

Use separate terminal tabs/windows so that the controller, Mininet, and
broker logs remain visible. The evidence document supplied with this
project labels these as Terminal A, Terminal B, Terminal C, and Terminal
4.

### Terminal A --- Start the Ryu controller

``` bash
cd ~/project3_distributed_job_queue
source ~/ryu310-venv/bin/activate
ryu-manager --verbose --ofp-tcp-listen-port 6653 --wsapi-port 8080 sdn_controller.py
```

Keep this terminal running. The controller log should show the Ryu
application starting and OpenFlow events as switches connect.

### Terminal B --- Start the Mininet topology

Open a second terminal:

``` bash
cd ~/project3_distributed_job_queue
sudo mn -c
sudo python3 topology.py
```

Wait for Mininet to finish creating the topology and for the switches to
connect to the controller. At the Mininet prompt, useful inspection
commands include:

``` text
nodes
net
dump
pingall
```

`pingall` checks reachability among the Mininet hosts. A successful
result should show that the hosts can communicate in the current
topology.

### Start the broker

At the Mininet prompt, start the broker on the broker host:

``` text
broker python3 /home/YOUR_USERNAME/project3_distributed_job_queue/broker.py > /tmp/p3-broker.log 2>&1 &
```

Replace `YOUR_USERNAME` with the Ubuntu username. For the environment
used in the demonstration, the home directory was `/home/shivaraj`.

The broker log can be inspected from another terminal:

``` bash
tail -f /tmp/p3-broker.log
```

### Start both workers

At the Mininet prompt, start the two workers:

``` text
w1 python3 /home/YOUR_USERNAME/project3_distributed_job_queue/worker.py --id 10.0.0.11 --broker 10.0.0.10 > /tmp/p3-w1.log 2>&1 &
w2 python3 /home/YOUR_USERNAME/project3_distributed_job_queue/worker.py --id 10.0.0.12 --broker 10.0.0.10 > /tmp/p3-w2.log 2>&1 &
```

Replace `YOUR_USERNAME` with the actual Ubuntu username. Confirm
registration by checking the worker logs:

``` bash
tail -n 20 /tmp/p3-w1.log
tail -n 20 /tmp/p3-w2.log
```

The logs should indicate that the workers connected and that
registration was confirmed. The broker log should also show worker
registration.

## 7. Submit jobs

Run client commands at the Mininet prompt. The following examples use
the broker address shown in the implementation evidence.

### Sum job

``` text
c1 python3 /home/YOUR_USERNAME/project3_distributed_job_queue/client.py --host 10.0.0.10 --type sum --values 5 10 15
```

### Factorial job

``` text
c1 python3 /home/YOUR_USERNAME/project3_distributed_job_queue/client.py --host 10.0.0.10 --type factorial --n 5
```

### Uppercase text job

``` text
c1 python3 /home/YOUR_USERNAME/project3_distributed_job_queue/client.py --host 10.0.0.10 --type uppercase --text "hello sdn"
```

### Worker/broker status

``` text
c1 python3 /home/YOUR_USERNAME/project3_distributed_job_queue/client.py --host 10.0.0.10 --type status
```

The client receives an acknowledgement followed by a result response.
The result includes information such as job ID, completion state, worker
ID, number of attempts, queue wait time, execution result, and elapsed
time.

## 8. SDN controller and REST interface

The Ryu application exposes a REST interface on port `8080`. The
controller state can be inspected from the Ubuntu host while the
controller is running:

``` bash
curl -s http://127.0.0.1:8080/sdn/state | python3 -m json.tool
```

The state response is used to inspect the controller and its view of the
connected switches, links, and paths.

For the link-failure demonstration, the project REST interface supports
link-state requests. Example commands for the `s1`--`s2` link are:

``` bash
curl -s -X POST http://127.0.0.1:8080/sdn/link/s1/s2/down | python3 -m json.tool
```

Restore the link with:

``` bash
curl -s -X POST http://127.0.0.1:8080/sdn/link/s1/s2/up | python3 -m json.tool
```

The corresponding Mininet link command can also be used at the Mininet
prompt:

``` text
link s1 s2 down
link s1 s2 up
```

After changing a link, use `pingall` and inspect the controller
state/logs to observe the network's response. Run only one method of
changing a link at a time, and use the actual switch/link names present
in `topology.py`.

## 9. Worker failure and job recovery test

This test demonstrates that the broker can recover an unfinished job
after a worker is lost.

1.  Confirm that the broker and both workers are running and registered.

2.  Submit a long-running sleep job from a client. For example:

    ``` text
    c1 python3 /home/YOUR_USERNAME/project3_distributed_job_queue/client.py --host 10.0.0.10 --type sleep --seconds 10
    ```

3.  Watch the broker log:

    ``` bash
    tail -f /tmp/p3-broker.log
    ```

4.  While the job is assigned to a worker, identify that worker's
    process:

    ``` bash
    ps aux | grep '[w]orker.py'
    ```

5.  Terminate only the worker process that received the job, using its
    actual process ID:

    ``` bash
    sudo kill WORKER_PID
    ```

    Replace `WORKER_PID` with the PID shown by `ps`. Do not kill the
    broker, Ryu controller, or Mininet process.

6.  Observe the broker log. The expected sequence is:

    -   the worker connection is lost;
    -   the broker reports that the worker was lost while handling the
        job;
    -   the job is requeued;
    -   the other worker receives the job with a later attempt number;
    -   the job completes and the client receives the result.

The supplied evidence includes a failover run in which the job was first
assigned to worker `10.0.0.11`, was requeued after that worker was lost,
and then completed on worker `10.0.0.12` with `attempts = 2`.

This demonstrates retry-based recovery. It does not prove exactly-once
execution for arbitrary jobs.

## 10. Performance benchmark

The benchmark program can be used to submit a batch of jobs with a
selected concurrency level and job type. Example commands used during
testing include:

``` text
c1 python3 /home/YOUR_USERNAME/project3_distributed_job_queue/benchmark.py --host 10.0.0.10 --jobs 20 --concurrency 4 --type sleep --seconds 0.2
```

A second run can use a different concurrency level or job duration to
compare the results. Check the options supported by the checked-out
version with:

``` bash
python3 benchmark.py --help
```

The benchmark output records the run's job counts and timing/performance
statistics (as implemented by the script), such as throughput and
latency. Use the actual output from your run in the project report;
performance values depend on the VM, workload, worker count, and
concurrency. No benchmark figures are asserted in this README.

## 11. Evidence and test summary

The accompanying `CN_Jackfruit.docx` contains the terminal screenshots
used as implementation evidence. The page references below refer to that
submitted evidence document.

  -----------------------------------------------------------------------
  Evidence document pages             Demonstration shown
  ----------------------------------- -----------------------------------
  1                                   Ryu controller running in Terminal
                                      A; controller application and
                                      OpenFlow event output.

  2                                   Mininet topology startup in
                                      Terminal B, host/switch setup, and
                                      connectivity checks.

  2--5                                Client job requests and responses,
                                      including arithmetic/text jobs,
                                      status output, and benchmark runs.

  6--16                               Broker/worker activity in Terminal
                                      C, worker registration and
                                      reconnection messages, job
                                      scheduling/completion logs, and Ryu
                                      REST state output.

  17--19                              Link/controller operations and
                                      broker/worker log evidence across
                                      the additional terminal.

  20                                  Worker failure test: broker detects
                                      the lost worker, requeues the job,
                                      assigns it to the other worker, and
                                      the client receives a completed
                                      result on attempt 2.

  21                                  Additional benchmark output for
                                      different workload/concurrency
                                      settings.
  -----------------------------------------------------------------------

### Tests evidenced

-   Controller process starts and emits Ryu/OpenFlow events.
-   Mininet topology starts and exposes the configured hosts and
    switches.
-   Host connectivity is checked with Mininet.
-   Workers register with the broker.
-   Clients submit different job types and receive results.
-   Broker status is queried.
-   Ryu REST state is queried.
-   Link-state operations are issued through the REST interface/Mininet.
-   A worker-loss scenario is exercised and the job is requeued and
    completed by the other worker.
-   Benchmark runs are executed and output is captured.

The screenshots are evidence of the runs captured in the document. For a
fresh evaluation, repeat the commands in this README and capture new
output from the current environment.

## 12. Troubleshooting

### Ryu does not start

-   Confirm that the Ryu virtual environment is active:

    ``` bash
    source ~/ryu310-venv/bin/activate
    python --version
    ```

-   Confirm that `ryu-manager` is available:

    ``` bash
    ryu-manager --version
    ```

-   Run the controller from the project directory so that
    `sdn_controller.py` is found.

### Mininet switches do not connect to Ryu

-   Start Ryu before starting the topology.

-   Confirm that the controller is listening on port `6653`.

-   Check that the controller address configured in `topology.py` is
    reachable from the Mininet VM.

-   Clean up stale Mininet state and restart:

    ``` bash
    sudo mn -c
    ```

### Workers repeatedly disconnect

-   Confirm that the broker is running and listening on
    `10.0.0.10:5000`.
-   Confirm that the broker IP passed to each worker is correct.
-   Inspect `/tmp/p3-broker.log`, `/tmp/p3-w1.log`, and
    `/tmp/p3-w2.log`.
-   Ensure that the broker is not being stopped while workers are
    running.

### A client cannot connect

-   Confirm that the broker is running on the broker Mininet host.
-   Confirm that the client uses broker address `10.0.0.10`.
-   Check that Mininet host connectivity works with `pingall`.

### Mininet cleanup

If a previous run left stale Mininet processes or namespaces, run:

``` bash
sudo mn -c
```

Then restart the controller/topology in the documented order.

## 13. Repository contents

  -----------------------------------------------------------------------
  File                                Description
  ----------------------------------- -----------------------------------
  `broker.py`                         TCP broker, job queue, worker
                                      tracking, scheduling, and recovery.

  `worker.py`                         Worker registration, heartbeat, job
                                      execution, and result reporting.

  `client.py`                         Client interface for submitting
                                      jobs and status requests.

  `sdn_controller.py`                 Ryu/OpenFlow SDN controller and
                                      REST interface.

  `topology.py`                       Mininet network topology.

  `benchmark.py`                      Job-load/performance benchmark.

  `requirements-controller.txt`       Controller Python dependency list.

  `README.md`                         Project documentation.

  `REPORT_TEMPLATE.md`                Report-writing template.

  `TEST_PLAN.md`                      Test plan.

  `VIVA_QUESTIONS.md`                 Project viva preparation material.
  -----------------------------------------------------------------------

## 14. Limitations and future improvements

-   The current demonstration uses a small Mininet topology and two
    workers.
-   The worker-recovery mechanism is retry/requeue based; jobs with
    external side effects should be designed to tolerate retries.
-   Benchmark results are dependent on the host machine and VM resource
    allocation.
-   Further work could include persistent job storage, authentication
    between clients/workers and broker, stronger duplicate-job handling,
    richer worker-load scheduling, automated link-failure tests, and
    more extensive performance evaluation.

## 15. Team and project information

**Project:** Distributed Job Queue and Worker Management using TCP
Socket Programming and SDN\
**GitHub:** https://github.com/shivaraj3359/Distributed-Job-Queue-SDN

**Course:** UE25CS243A – Computer Networks  
**Project level:** Level 3 – Jackfruit Mini Project  
**Deliverable:** D2 Final Submission

### Team members

| Team member | USN |
| --- | --- |
| Shivaraj | PES1UG25AM372 |
| Sakshin Prashanthakumar Shetty | PES1UG25AM335 |
| Saaket Dayal | PES1UG25AM326 |

### Final report

The submission report is available in `reports/`:

- `Project3_Distributed_Job_Queue_Final_Report.pdf` — final report for reading and submission.
- `Project3_Distributed_Job_Queue_Final_Report.docx` — editable Word version.

The report includes the system architecture, implementation details, test evidence, observed worker failover, and limitations. Screenshots used as evidence are embedded in the report.

