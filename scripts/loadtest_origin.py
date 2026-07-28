#!/usr/bin/env python3
"""Minimal blob origin for the data-plane load test (#291).

The load sweep measures the *serve* path: after the first fetch every request
is a resident cache hit, so the origin is touched once per run. It still has to
exist, because a worker resolves an object's version with a HEAD before serving
and fails the read if that HEAD fails.

Implements only what `AzureBackend` requires:
  - HEAD  -> Content-Length + ETag
  - GET   -> 200 whole blob, or 206 with Content-Range for a ranged read

Deterministic ramp bytes, so a caller can verify content if it wants to.
"""

import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SIZE = 64 << 20  # 64 MiB synthetic blob
ETAG = '"0x8LOADTEST"'


def ramp(start: int, length: int) -> bytes:
    return bytes((i % 251) for i in range(start, start + length))


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):  # quiet
        pass

    def _common(self):
        self.send_header("ETag", ETAG)
        self.send_header("Last-Modified", "Mon, 01 Jan 2035 00:00:00 GMT")
        self.send_header("x-ms-blob-type", "BlockBlob")

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Length", str(SIZE))
        self._common()
        self.end_headers()

    def do_GET(self):
        # Azure List Blobs: restype=container&comp=list
        if "comp=list" in (self.path or ""):
            import urllib.parse as _u
            q = _u.parse_qs(_u.urlparse(self.path).query)
            want = q.get("prefix", [""])[0]
            blobs = [("bench", SIZE), ("nested/other.bin", 1024)]
            blobs = [b for b in blobs if b[0].startswith(want)]
            entries = "".join(
                f"<Blob><Name>{n}</Name><Properties>"
                f"<Content-Length>{sz}</Content-Length></Properties></Blob>"
                for n, sz in blobs
            )
            body = (
                '<?xml version="1.0" encoding="utf-8"?>'
                f"<EnumerationResults><Blobs>{entries}</Blobs>"
                "<NextMarker /></EnumerationResults>"
            ).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Type", "application/xml")
            self.end_headers()
            self.wfile.write(body)
            return

        rng = self.headers.get("x-ms-range") or self.headers.get("Range")
        if rng and rng.startswith("bytes="):
            first, _, last = rng[len("bytes="):].partition("-")
            start = int(first)
            end = int(last) if last else SIZE - 1
            end = min(end, SIZE - 1)
            length = max(0, end - start + 1)
            body = ramp(start, length)
            self.send_response(206)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Range", f"bytes {start}-{end}/{SIZE}")
        else:
            body = ramp(0, SIZE)
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
        self._common()
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 17700
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
