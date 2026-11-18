#!/usr/bin/env python3
"""Threaded TCP job broker for Project 3: Distributed Job Queue and Worker Management."""
import argparse
import collections
import json
import socket
import threading
import time
import uuid
from contextlib import nullcontext

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 5000
HEARTBEAT_TIMEOUT = 15.0
CLIENT_WAIT_TIMEOUT = 90.0


def recv_json_line(stream):
    line = stream.readline()
    if not line:
        return None
    try:
        return json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {"type": "invalid", "error": "Expected one JSON object per line."}


def send_json(sock, message, send_lock=None):
    data = (json.dumps(message, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")
    with send_lock if send_lock is not None else nullcontext():
        sock.sendall(data)


class JobBroker:
    def __init__(self, host, port, heartbeat_timeout=HEARTBEAT_TIMEOUT, client_timeout=CLIENT_WAIT_TIMEOUT):
        self.host = host
        self.port = port
        self.heartbeat_timeout = heartbeat_timeout
        self.client_timeout = client_timeout
        self.lock = threading.RLock()
        self.workers = {}
        self.jobs = {}
        self.pending = collections.deque()
        self.total_submitted = 0
        self.total_completed = 0
        self.total_failed = 0
        self.stop_event = threading.Event()
        self.server_socket = None

    def log(self, message):
        print(time.strftime("%H:%M:%S"), message, flush=True)

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(128)
        self.server_socket = server
        threading.Thread(target=self.dispatch_loop, name="job-dispatcher", daemon=True).start()
        threading.Thread(target=self.heartbeat_watchdog, name="heartbeat-watchdog", daemon=True).start()
        self.log(f"[BROKER] Listening on {self.host}:{self.port}")
        self.log("[BROKER] Protocol: one newline-delimited JSON request per client TCP connection")
        try:
            while not self.stop_event.is_set():
                try:
                    conn, address = server.accept()
                except OSError:
                    if self.stop_event.is_set():
                        break
                    raise
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                threading.Thread(
                    target=self.handle_connection,
                    args=(conn, address),
                    name=f"client-{address[0]}:{address[1]}",
                    daemon=True,
                ).start()
        except KeyboardInterrupt:
            self.log("[BROKER] Shutdown requested")
        finally:
            self.stop_event.set()
            try:
                server.close()
            except OSError:
                pass
            with self.lock:
                active = list(self.workers.values())
            for worker in active:
                try:
                    worker["sock"].shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    worker["sock"].close()
                except OSError:
                    pass
            self.log("[BROKER] Stopped")

    def handle_connection(self, conn, address):
        worker_id = None
        stream = None
        try:
            stream = conn.makefile("rb")
            first = recv_json_line(stream)
            if not isinstance(first, dict):
                return
            message_type = first.get("type")
            if message_type == "register":
                worker_id = str(first.get("worker_id", "")).strip()
                if not worker_id or len(worker_id) > 64:
                    send_json(conn, {"type": "error", "error": "worker_id must contain 1-64 characters"})
                    return
                if not self.register_worker(worker_id, conn, first):
                    send_json(conn, {"type": "error", "error": f"worker_id '{worker_id}' is already connected"})
                    return
                while not self.stop_event.is_set():
                    message = recv_json_line(stream)
                    if message is None:
                        break
                    if not isinstance(message, dict):
                        continue
                    self.handle_worker_message(worker_id, conn, message)
            elif message_type == "submit":
                self.handle_job_submission(conn, first)
            elif message_type == "status":
                send_json(conn, self.status_snapshot())
            else:
                send_json(conn, {"type": "error", "error": "First message type must be submit, status, or register"})
        except (ConnectionResetError, BrokenPipeError, OSError) as exc:
            self.log(f"[CONNECTION] {address[0]}:{address[1]} closed: {exc}")
        finally:
            if worker_id is not None:
                self.drop_worker(worker_id, conn, "TCP connection closed")
            if stream is not None:
                try:
                    stream.close()
                except OSError:
                    pass
            try:
                conn.close()
            except OSError:
                pass

    def register_worker(self, worker_id, conn, first_message):
        with self.lock:
            current = self.workers.get(worker_id)
            if current is not None:
                return False
            self.workers[worker_id] = {
                "worker_id": worker_id,
                "sock": conn,
                "send_lock": threading.Lock(),
                "last_seen": time.monotonic(),
                "busy_job": None,
                "load": 0.0,
                "jobs_completed": 0,
                "registered_at": time.time(),
                "remote": first_message.get("hostname", "unknown"),
            }
        try:
            send_json(conn, {"type": "registered", "worker_id": worker_id}, self.workers[worker_id]["send_lock"])
        except OSError:
            self.drop_worker(worker_id, conn, "registration acknowledgement failed")
            return False
        self.log(f"[WORKER] Registered {worker_id} from {self.workers[worker_id]['remote']}")
        return True

    def handle_worker_message(self, worker_id, conn, message):
        message_type = message.get("type")
        if message_type == "heartbeat":
            with self.lock:
                worker = self.workers.get(worker_id)
                if worker is not None and worker["sock"] is conn:
                    worker["last_seen"] = time.monotonic()
                    try:
                        worker["load"] = min(1.0, max(0.0, float(message.get("load", 0.0))))
                    except (TypeError, ValueError):
                        worker["load"] = 0.0
            return
        if message_type == "result":
            self.finish_job(worker_id, conn, message)
            return
        if message_type == "worker_error":
            self.log(f"[WORKER] {worker_id} reported: {message.get('error', 'unknown error')}")
            return
        self.log(f"[WORKER] Ignoring unknown message from {worker_id}: {message_type}")

    def handle_job_submission(self, conn, request):
        job_type = str(request.get("job_type", "echo"))
        payload = request.get("payload", {})
        if not isinstance(payload, (dict, list, str, int, float, bool, type(None))):
            send_json(conn, {"type": "error", "error": "payload must be valid JSON data"})
            return
        job_id = uuid.uuid4().hex[:12]
        job = {
            "job_id": job_id,
            "job_type": job_type,
            "payload": payload,
            "state": "queued",
            "worker_id": None,
            "attempt": 0,
            "submitted_at": time.monotonic(),
            "assigned_at": None,
            "finished_at": None,
            "result": None,
            "error": None,
            "event": threading.Event(),
            "client_timed_out": False,
        }
        with self.lock:
            self.jobs[job_id] = job
            self.pending.append(job_id)
            self.total_submitted += 1
            queue_position = len(self.pending)
        self.log(f"[JOB] {job_id} submitted type={job_type} queue_position={queue_position}")
        try:
            send_json(conn, {"type": "ack", "job_id": job_id, "state": "queued", "queue_position": queue_position})
        except OSError:
            with self.lock:
                job["client_timed_out"] = True
            return
        completed = job["event"].wait(self.client_timeout)
        if not completed:
            with self.lock:
                job["client_timed_out"] = True
                state = job["state"]
            try:
                send_json(conn, {"type": "timeout", "job_id": job_id, "state": state, "message": "Client wait timed out; broker continues tracking the job."})
            except OSError:
                pass
            return
        with self.lock:
            response = {
                "type": "result",
                "job_id": job_id,
                "state": job["state"],
                "worker_id": job["worker_id"],
                "attempts": job["attempt"],
                "queue_wait_seconds": round((job["assigned_at"] or job["finished_at"] or time.monotonic()) - job["submitted_at"], 4),
                "result": job["result"],
                "error": job["error"],
            }
        try:
            send_json(conn, response)
        except OSError:
            self.log(f"[CLIENT] Result for job {job_id} could not be returned (client disconnected)")
        finally:
            with self.lock:
                self.jobs.pop(job_id, None)

    def finish_job(self, worker_id, conn, message):
        job_id = str(message.get("job_id", ""))
        try:
            attempt = int(message.get("attempt", -1))
        except (TypeError, ValueError):
            attempt = -1
        with self.lock:
            worker = self.workers.get(worker_id)
            if worker is None or worker["sock"] is not conn:
                return
            worker["last_seen"] = time.monotonic()
            worker["load"] = 0.0
            job = self.jobs.get(job_id)
            if job is None or job["state"] != "running" or job["worker_id"] != worker_id or job["attempt"] != attempt:
                self.log(f"[JOB] Ignored stale result job={job_id} worker={worker_id} attempt={attempt}")
                if worker["busy_job"] == job_id:
                    worker["busy_job"] = None
                return
            success = bool(message.get("ok", True))
            job["state"] = "completed" if success else "failed"
            job["result"] = message.get("result") if success else None
            job["error"] = None if success else str(message.get("error", "worker execution failed"))
            job["finished_at"] = time.monotonic()
            worker["busy_job"] = None
            worker["jobs_completed"] += 1
            if success:
                self.total_completed += 1
            else:
                self.total_failed += 1
            job["event"].set()
            elapsed = round(job["finished_at"] - (job["assigned_at"] or job["submitted_at"]), 4)
            self.log(f"[JOB] {job_id} {job['state']} by {worker_id} attempt={attempt} execution_window={elapsed}s")
            if job.get("client_timed_out"):
                self.jobs.pop(job_id, None)

    def drop_worker(self, worker_id, conn, reason):
        socket_to_close = None
        with self.lock:
            worker = self.workers.get(worker_id)
            if worker is None or worker["sock"] is not conn:
                return
            self.workers.pop(worker_id, None)
            socket_to_close = conn
            job_id = worker.get("busy_job")
            if job_id:
                job = self.jobs.get(job_id)
                if job is not None and job["state"] == "running" and job["worker_id"] == worker_id:
                    job["state"] = "queued"
                    job["worker_id"] = None
                    job["assigned_at"] = None
                    job["error"] = None
                    self.pending.appendleft(job_id)
                    self.log(f"[FAILOVER] Worker {worker_id} lost during job {job_id}; job requeued")
            self.log(f"[WORKER] Disconnected {worker_id}: {reason}")
        try:
            socket_to_close.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            socket_to_close.close()
        except OSError:
            pass

    def dispatch_loop(self):
        while not self.stop_event.is_set():
            assignment = None
            now = time.monotonic()
            with self.lock:
                while self.pending:
                    job_id = self.pending[0]
                    job = self.jobs.get(job_id)
                    if job is None or job["state"] != "queued":
                        self.pending.popleft()
                        continue
                    eligible = [
                        worker for worker in self.workers.values()
                        if worker["busy_job"] is None and now - worker["last_seen"] <= self.heartbeat_timeout
                    ]
                    if not eligible:
                        break
                    worker = min(eligible, key=lambda item: (item["load"], item["jobs_completed"], item["worker_id"]))
                    self.pending.popleft()
                    job["attempt"] += 1
                    job["state"] = "running"
                    job["worker_id"] = worker["worker_id"]
                    job["assigned_at"] = now
                    worker["busy_job"] = job_id
                    worker["load"] = 1.0
                    assignment = (worker, job, {
                        "type": "job",
                        "job_id": job_id,
                        "attempt": job["attempt"],
                        "job_type": job["job_type"],
                        "payload": job["payload"],
                    })
                    break
            if assignment:
                worker, job, message = assignment
                self.log(f"[SCHEDULE] Job {job['job_id']} -> {worker['worker_id']} attempt={job['attempt']}")
                try:
                    send_json(worker["sock"], message, worker["send_lock"])
                except OSError as exc:
                    self.drop_worker(worker["worker_id"], worker["sock"], f"send failed: {exc}")
            else:
                time.sleep(0.1)

    def heartbeat_watchdog(self):
        while not self.stop_event.wait(2.0):
            now = time.monotonic()
            with self.lock:
                stale = [
                    (worker["worker_id"], worker["sock"])
                    for worker in self.workers.values()
                    if now - worker["last_seen"] > self.heartbeat_timeout
                ]
            for worker_id, conn in stale:
                self.drop_worker(worker_id, conn, "heartbeat timeout")

    def status_snapshot(self):
        now = time.monotonic()
        with self.lock:
            workers = []
            for worker in self.workers.values():
                workers.append({
                    "worker_id": worker["worker_id"],
                    "status": "busy" if worker["busy_job"] else "idle",
                    "busy_job": worker["busy_job"],
                    "load": round(worker["load"], 2),
                    "jobs_completed": worker["jobs_completed"],
                    "heartbeat_age_seconds": round(now - worker["last_seen"], 2),
                })
            return {
                "type": "status",
                "workers": sorted(workers, key=lambda item: item["worker_id"]),
                "queued_jobs": sum(1 for job_id in self.pending if job_id in self.jobs and self.jobs[job_id]["state"] == "queued"),
                "active_jobs": sum(1 for job in self.jobs.values() if job["state"] == "running"),
                "total_submitted": self.total_submitted,
                "total_completed": self.total_completed,
                "total_failed": self.total_failed,
            }


def main():
    parser = argparse.ArgumentParser(description="Concurrent TCP job queue broker")
    parser.add_argument("--host", default=DEFAULT_HOST, help="bind address (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="TCP port (default: 5000)")
    parser.add_argument("--heartbeat-timeout", type=float, default=HEARTBEAT_TIMEOUT)
    parser.add_argument("--client-timeout", type=float, default=CLIENT_WAIT_TIMEOUT)
    args = parser.parse_args()
    JobBroker(args.host, args.port, args.heartbeat_timeout, args.client_timeout).start()


if __name__ == "__main__":
    main()
