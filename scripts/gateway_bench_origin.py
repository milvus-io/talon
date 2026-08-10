#!/usr/bin/env python3
"""Dual-protocol origin stub for the gateway proxy benchmark.

The gateway speaks S3 on one deployment and Azure Blob on another, but each
deployment still talks to a *real* origin for metadata (HEAD, list) and for
every request routed with `TALON_GATEWAY_ROUTE=origin`. Comparing the two
adapters is only meaningful if the thing behind them is identical, so this
serves both protocols from one process over one blob:

  S3     HEAD/GET /<bucket>/<key>            (path style; auth ignored)
         GET /<bucket>?list-type=2           ListObjectsV2
  Azure  HEAD/GET /<account>/<container>/<key>
         GET /<account>/<container>?restype=container&comp=list

Auth is deliberately not verified. The gateway re-signs with its own origin
identity, and a stub that validated signatures would measure Python's crypto
rather than the gateway. Bytes are a deterministic ramp so a caller can verify
content independently of this process.
"""

import re
import sys
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SIZE = int(sys.argv[2]) if len(sys.argv) > 2 else (64 << 20)
ETAG = '"0x8GATEWAYBENCH"'
LAST_MODIFIED = "Mon, 01 Jan 2035 00:00:00 GMT"
KEY = "bench"

_RAMP = bytes((i % 251) for i in range(251 * 64))


def ramp(start: int, length: int) -> bytes:
    """Deterministic bytes for [start, start+length) without materializing SIZE."""
    period = len(_RAMP)
    off = start % period
    need = length + off
    reps = (need + period - 1) // period
    return (_RAMP * reps)[off:off + length]


def parse_range(value, size):
    """Return (start, length) for a single byte range, or None."""
    if not value:
        return None
    m = re.match(r"bytes=(\d*)-(\d*)$", value.strip())
    if not m:
        return None
    lo, hi = m.group(1), m.group(2)
    if lo == "" and hi == "":
        return None
    if lo == "":                      # suffix: last N bytes
        n = min(int(hi), size)
        return (size - n, n)
    start = int(lo)
    if start >= size:
        return None
    end = size - 1 if hi == "" else min(int(hi), size - 1)
    if end < start:
        return None
    return (start, end - start + 1)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    # A benchmark origin that logs every request measures the logger.
    def log_message(self, *_):
        pass

    # -- helpers ---------------------------------------------------------
    def _blob_headers(self):
        self.send_header("ETag", ETAG)
        self.send_header("Last-Modified", LAST_MODIFIED)
        self.send_header("x-ms-blob-type", "BlockBlob")
        self.send_header("x-ms-request-id", "bench")
        self.send_header("Accept-Ranges", "bytes")

    def _xml(self, body: bytes):
        self.send_response(200)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("ETag", ETAG)
        self.end_headers()
        self.wfile.write(body)

    def _query(self):
        return urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)

    def _is_list(self):
        q = self._query()
        return "list-type" in q or q.get("comp", [""])[0] == "list"

    # -- listings --------------------------------------------------------
    def _list_azure(self):
        body = (
            '<?xml version="1.0" encoding="utf-8"?>'
            "<EnumerationResults><Blobs>"
            f"<Blob><Name>{KEY}</Name><Properties>"
            f"<Content-Length>{SIZE}</Content-Length>"
            f"<Etag>{ETAG.strip(chr(34))}</Etag>"
            "<ResourceType>file</ResourceType>"
            "</Properties></Blob>"
            "</Blobs><NextMarker /></EnumerationResults>"
        ).encode()
        self._xml(body)

    def _list_s3(self):
        body = (
            '<?xml version="1.0" encoding="utf-8"?>'
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            "<IsTruncated>false</IsTruncated>"
            f"<KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys>"
            f"<Contents><Key>{KEY}</Key><Size>{SIZE}</Size>"
            f"<ETag>&quot;{ETAG.strip(chr(34))}&quot;</ETag></Contents>"
            "</ListBucketResult>"
        ).encode()
        self._xml(body)

    # -- verbs -----------------------------------------------------------
    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Length", str(SIZE))
        self._blob_headers()
        self.end_headers()

    def do_GET(self):
        if self._is_list():
            if "list-type" in self._query():
                self._list_s3()
            else:
                self._list_azure()
            return

        rng = parse_range(
            self.headers.get("Range") or self.headers.get("x-ms-range"), SIZE
        )
        if rng is None:
            start, length, status = 0, SIZE, 200
        else:
            start, length = rng
            status = 206

        self.send_response(status)
        self.send_header("Content-Length", str(length))
        if status == 206:
            self.send_header(
                "Content-Range", f"bytes {start}-{start + length - 1}/{SIZE}"
            )
        self._blob_headers()
        self.end_headers()

        # Stream in chunks; a multi-GB whole-object GET must not be buffered.
        CHUNK = 1 << 20
        sent = 0
        try:
            while sent < length:
                n = min(CHUNK, length - sent)
                self.wfile.write(ramp(start + sent, n))
                sent += n
        except (BrokenPipeError, ConnectionResetError):
            pass


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 18080
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    srv.daemon_threads = True
    print(f"origin on 127.0.0.1:{port}, blob {KEY} = {SIZE} bytes", flush=True)
    srv.serve_forever()


main()
