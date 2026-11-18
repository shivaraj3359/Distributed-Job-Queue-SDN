#!/usr/bin/env python3
"""Concurrent client benchmark for the project performance-evaluation rubric."""
import argparse
import json
import socket
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def do_one(host, port, job_type, seconds, timeout, index):
    payload = {"seconds": seconds} if job_type == "sleep" else {"values": list(range(1, 101))}
    request = {"type": "submit", "job_type": job_type, "payload": payload, "benchmark_index": index}
    started = time.perf_counter()
    try:
        with socket.create_connection((host, port), timeout=timeout) as sock:
            sock.settimeout(timeout)
            sock.sendall((json.dumps(request, separators=(",", ":")) + "\n").encode("utf-8"))
            stream = sock.makefile("rb")
            ack_line = stream.readline()
            if not ack_line:
                return {"ok": False, "latency": time.perf_counter() - started, "error": "no acknowledgement"}
            ack = json.loads(ack_line.decode("utf-8"))
            if ack.get("type") != "ack":
                return {"ok": False, "latency": time.perf_counter() - started, "error": str(ack)}
            result_line = stream.readline()
            if not result_line:
                return {"ok": False, "latency": time.perf_counter() - started, "error": "no result"}
            result = json.loads(result_line.decode("utf-8"))
            latency = time.perf_counter() - started
            return {
                "ok": result.get("state") == "completed",
                "latency": latency,
                "worker_id": result.get("worker_id"),
                "attempts": result.get("attempts"),
                "error": result.get("error"),
            }
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return {"ok": False, "latency": time.perf_counter() - started, "error": str(exc)}


def percentile(values, pct):
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(round((pct / 100.0) * (len(ordered) - 1)))))
    return ordered[index]


def main():
    parser = argparse.ArgumentParser(description="Run concurrent TCP job-queue tests")
    parser.add_argument("--host", default="10.0.0.10")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--jobs", type=int, default=20)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--type", choices=["sum", "sleep"], default="sum")
    parser.add_argument("--seconds", type=float, default=0.2, help="duration for sleep jobs")
    parser.add_argument("--timeout", type=float, default=120.0)
    args = parser.parse_args()
    if args.jobs < 1 or args.concurrency < 1:
        parser.error("--jobs and --concurrency must be positive integers")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")

    started = time.perf_counter()
    results = []
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [pool.submit(do_one, args.host, args.port, args.type, args.seconds, args.timeout, i + 1)
                   for i in range(args.jobs)]
        for future in as_completed(futures):
            results.append(future.result())
    wall = time.perf_counter() - started
    latencies = [result["latency"] for result in results if result["ok"]]
    completed = sum(1 for result in results if result["ok"])
    failed = len(results) - completed
    report = {
        "jobs_requested": args.jobs,
        "concurrency": args.concurrency,
        "job_type": args.type,
        "completed": completed,
        "failed": failed,
        "wall_seconds": round(wall, 4),
        "throughput_jobs_per_second": round(completed / wall, 4) if wall else 0.0,
        "average_latency_seconds": round(statistics.mean(latencies), 4) if latencies else 0.0,
        "p95_latency_seconds": round(percentile(latencies, 95), 4) if latencies else 0.0,
        "workers_used": sorted(set(result.get("worker_id") for result in results if result.get("worker_id"))),
    }
    print(json.dumps(report, indent=2))
    if failed:
        print("Failures:")
        for result in results:
            if not result["ok"]:
                print(" -", result.get("error", "unknown error"))


if __name__ == "__main__":
    main()
