#!/usr/bin/env python3
"""TCP worker agent. Reconnects automatically if the broker or network disappears."""
import argparse
import json
import math
import socket
import threading
import time


def send_json(sock, message, send_lock):
    data = (json.dumps(message, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")
    with send_lock:
        sock.sendall(data)


def receive_json(stream):
    line = stream.readline()
    if not line:
        return None
    try:
        return json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {"type": "invalid", "error": "Malformed JSON from broker"}


def execute_job(job_type, payload):
    if job_type == "sum":
        values = payload.get("values", []) if isinstance(payload, dict) else []
        if not isinstance(values, list) or not values:
            raise ValueError("sum job requires payload.values as a non-empty list")
        if any(not isinstance(value, (int, float)) or isinstance(value, bool) for value in values):
            raise ValueError("all sum values must be numbers")
        return {"sum": sum(values), "count": len(values)}
    if job_type == "factorial":
        n = payload.get("n") if isinstance(payload, dict) else None
        if not isinstance(n, int) or isinstance(n, bool) or n < 0 or n > 20:
            raise ValueError("factorial requires integer n between 0 and 20")
        return {"n": n, "factorial": math.factorial(n)}
    if job_type == "uppercase":
        text = payload.get("text", "") if isinstance(payload, dict) else str(payload)
        return {"text": str(text).upper()}
    if job_type == "sleep":
        seconds = payload.get("seconds", 2) if isinstance(payload, dict) else 2
        if not isinstance(seconds, (int, float)) or isinstance(seconds, bool):
            raise ValueError("sleep requires numeric seconds")
        seconds = min(10.0, max(0.0, float(seconds)))
        time.sleep(seconds)
        return {"slept_seconds": seconds}
    if job_type == "echo":
        return {"echo": payload}
    raise ValueError(f"unknown job_type '{job_type}'")


def run_worker(worker_id, broker_host, broker_port, heartbeat_interval):
    stop_heartbeat = threading.Event()
    state_lock = threading.Lock()
    state = {"load": 0.0}
    print(f"[WORKER {worker_id}] PID={__import__('os').getpid()} broker={broker_host}:{broker_port}", flush=True)

    while True:
        sock = None
        stream = None
        heartbeat_thread = None
        try:
            sock = socket.create_connection((broker_host, broker_port), timeout=5)
            sock.settimeout(None)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            send_lock = threading.Lock()
            send_json(sock, {"type": "register", "worker_id": worker_id, "hostname": socket.gethostname()}, send_lock)
            stream = sock.makefile("rb")
            stop_heartbeat = threading.Event()

            def heartbeat_loop(connection=sock, lock=send_lock, stop=stop_heartbeat):
                while not stop.wait(heartbeat_interval):
                    with state_lock:
                        load = state["load"]
                    try:
                        send_json(connection, {"type": "heartbeat", "load": load, "time": time.time()}, lock)
                    except OSError:
                        return

            heartbeat_thread = threading.Thread(target=heartbeat_loop, name=f"heartbeat-{worker_id}", daemon=True)
            heartbeat_thread.start()
            print(f"[WORKER {worker_id}] Connected; waiting for jobs", flush=True)
            while True:
                message = receive_json(stream)
                if message is None:
                    raise ConnectionError("broker closed the TCP connection")
                if message.get("type") == "registered":
                    print(f"[WORKER {worker_id}] Registration confirmed", flush=True)
                    continue
                if message.get("type") == "error":
                    raise ConnectionError(message.get("error", "broker rejected worker"))
                if message.get("type") != "job":
                    continue
                job_id = str(message.get("job_id", ""))
                attempt = int(message.get("attempt", 1))
                job_type = str(message.get("job_type", "echo"))
                started = time.monotonic()
                with state_lock:
                    state["load"] = 1.0
                print(f"[WORKER {worker_id}] Running job={job_id} attempt={attempt} type={job_type}", flush=True)
                try:
                    result = execute_job(job_type, message.get("payload", {}))
                    response = {
                        "type": "result", "job_id": job_id, "attempt": attempt,
                        "ok": True, "result": result,
                        "execution_seconds": round(time.monotonic() - started, 4),
                    }
                    print(f"[WORKER {worker_id}] Completed job={job_id}", flush=True)
                except Exception as exc:
                    response = {
                        "type": "result", "job_id": job_id, "attempt": attempt,
                        "ok": False, "error": str(exc),
                        "execution_seconds": round(time.monotonic() - started, 4),
                    }
                    print(f"[WORKER {worker_id}] Failed job={job_id}: {exc}", flush=True)
                finally:
                    with state_lock:
                        state["load"] = 0.0
                send_json(sock, response, send_lock)
        except (OSError, ConnectionError, ValueError) as exc:
            print(f"[WORKER {worker_id}] Connection problem: {exc}; retrying in 2 seconds", flush=True)
        finally:
            stop_heartbeat.set()
            if stream is not None:
                try:
                    stream.close()
                except OSError:
                    pass
            if sock is not None:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    sock.close()
                except OSError:
                    pass
        time.sleep(2)


def main():
    parser = argparse.ArgumentParser(description="Distributed job worker")
    parser.add_argument("--id", required=True, help="unique worker id, e.g. w1")
    parser.add_argument("--broker", default="10.0.0.10", help="broker host/IP")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--heartbeat-interval", type=float, default=3.0)
    args = parser.parse_args()
    if args.heartbeat_interval <= 0:
        parser.error("--heartbeat-interval must be greater than zero")
    run_worker(args.id, args.broker, args.port, args.heartbeat_interval)


if __name__ == "__main__":
    main()
