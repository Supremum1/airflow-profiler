#!/usr/bin/env python3
"""Tiny external HTTP service with a predictable response delay."""

import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        url = urlsplit(self.path)
        params = parse_qs(url.query)
        if url.path not in {"/stream", "/page"}:
            self.handle_request()
            return

        try:
            delay = float(params.get("delay", ["0"])[0])
            size = int(params.get("size", ["0"])[0])
            page = int(params.get("page", ["0"])[0])
            rows = int(params.get("rows", ["0"])[0])
            if not (0 <= delay <= 30 and 0 <= size <= 64 * 1024**2 and 0 <= rows <= 10000 and page >= 0):
                raise ValueError
        except ValueError:
            self.send_error(400, "Invalid workload parameters")
            return

        time.sleep(delay)
        if url.path == "/page":
            start = page * rows
            body = json.dumps([
                {"id": i, "customer": {"region": i % 8}, "amount": i % 10000}
                for i in range(start, start + rows)
            ]).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        self.send_response(200)
        self.send_header("Content-Length", str(size))
        self.end_headers()
        chunk = b"x" * 65536
        while size:
            sent = min(size, len(chunk))
            self.wfile.write(chunk[:sent])
            size -= sent

    def handle_request(self) -> None:
        # Health checks must not consume a deliberately slow benchmark request.
        if self.path != "/health":
            time.sleep(self.server.delay_seconds)
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"ok")

    do_POST = handle_request

    def log_message(self, _format: str, *_args) -> None:
        pass


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--delay-seconds", type=float, default=2.0)
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    server.delay_seconds = args.delay_seconds
    print(f"Тестовый HTTP-сервис: http://127.0.0.1:{args.port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
