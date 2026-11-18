#!/usr/bin/env python3
"""TCP client for submitting jobs to the distributed job queue broker."""
import argparse
import json
import socket
import sys
import time


def request(host, port, message, timeout):
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.sendall((json.dumps(message, separators=(",", ":")) + "\n").encode("utf-8"))
        stream = sock.makefile("rb")
        first_line = stream.readline()
        if not first_line:
            raise ConnectionError("broker closed connection without a response")
        first = json.loads(first_line.decode("utf-8"))
        if message.get("type") == "status" or first.get("type") == "error":
            return [first]
        responses = [first]
        if first.get("type") == "ack":
            second_line = stream.readline()
            if second_line:
                responses.append(json.loads(second_line.decode("utf-8")))
        return responses


def main():
    parser = argparse.ArgumentParser(description="Submit a TCP job or query broker status")
    parser.add_argument("--host", default="10.0.0.10", help="broker IP (default: 10.0.0.10 in Mininet topology)")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--type", dest="job_type", choices=["sum", "factorial", "uppercase", "sleep", "echo", "status"], default="sum")
    parser.add_argument("--values", nargs="*", type=float, default=[], help="numbers for a sum job")
    parser.add_argument("--n", type=int, help="n for a factorial job")
    parser.add_argument("--text", default="Hello from Mininet", help="text for uppercase job")
    parser.add_argument("--seconds", type=float, default=2.0, help="duration for a sleep job, capped at 10 seconds")
    parser.add_argument("--payload", help="raw JSON payload; overrides the type-specific payload")
    parser.add_argument("--timeout", type=float, default=100.0, help="TCP response timeout in seconds")
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be greater than zero")
    if args.job_type == "status":
        message = {"type": "status"}
    else:
        if args.payload is not None:
            try:
                payload = json.loads(args.payload)
            except json.JSONDecodeError as exc:
                parser.error(f"--payload is not valid JSON: {exc}")
        elif args.job_type == "sum":
            payload = {"values": args.values}
        elif args.job_type == "factorial":
            payload = {"n": args.n}
        elif args.job_type == "uppercase":
            payload = {"text": args.text}
        elif args.job_type == "sleep":
            payload = {"seconds": args.seconds}
        else:
            payload = {"message": args.text}
        message = {"type": "submit", "job_type": args.job_type, "payload": payload, "client_time": time.time()}
    try:
        started = time.perf_counter()
        responses = request(args.host, args.port, message, args.timeout)
        elapsed = round(time.perf_counter() - started, 4)
        if args.job_type != "status" and responses:
            responses[-1]["client_elapsed_seconds"] = elapsed
        for response in responses:
            print(json.dumps(response, indent=2, ensure_ascii=False))
    except (OSError, ConnectionError, json.JSONDecodeError) as exc:
        print(f"[CLIENT] Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
