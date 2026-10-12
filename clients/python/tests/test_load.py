"""Offline binding tests using real TCP frames; also embedded in Rust tests."""
from concurrent.futures import ThreadPoolExecutor
import io
import socketserver
import struct
import threading
import unittest

import talon


def string(value):
    value = value.encode()
    return struct.pack("<Q", len(value)) + value


class Peer(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), Handler)
        self.frames = []
        self.tenant_frames = []
        self.errors = []
        self.reject = False
        self.drop_reply = False
        self.thread = threading.Thread(target=self.serve_forever, daemon=True)
        self.thread.start()

    @property
    def address(self):
        return "%s:%s" % self.server_address

    def __exit__(self, *args):
        self.shutdown()
        self.server_close()
        self.thread.join(timeout=5)


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        self.request.settimeout(5)
        try:
            while True:
                header = self.rfile.read(16)
                if not header:
                    return
                magic, version, kind, flags, reserved, request_id, length = struct.unpack(">HBBHHII", header)
                assert magic == 0x544C and version in (1, 2, 3)
                body = io.BytesIO(self.rfile.read(length))
                tenant = "default"
                if version >= 2:
                    metadata = io.BytesIO(body.read(struct.unpack(">H", body.read(2))[0]))
                    while metadata.tell() < len(metadata.getvalue()):
                        key, size = struct.unpack(">BH", metadata.read(3))
                        value = metadata.read(size)
                        if key == 5:
                            tenant = value.decode()
                def read_string():
                    size = struct.unpack("<Q", body.read(8))[0]
                    return body.read(size).decode()
                if kind in (10, 11):
                    if kind == 11:
                        assert struct.unpack("<I", body.read(4))[0] == 0
                        assert read_string() == tenant
                    body.read(4)
                    read_string(), read_string()
                    offset, size = struct.unpack("<QQ", body.read(16))
                    assert read_string() == tenant
                    body.read(8)
                    reply = tenant[0].encode() * size
                    self.wfile.write(struct.pack(">HBBHHII", 0x544C, version, 2, 0, 0, request_id, len(reply)) + reply)
                    self.wfile.flush()
                    continue
                assert kind == 0
                schema, tag = struct.unpack("<HI", body.read(6))
                assert schema == 6
                if tag == 4:
                    reply = struct.pack("<HIQQQQ", 6, 5, 1, 1, 500, 1)
                    reply += string("worker") + b"\0\0" + struct.pack("<I", 2)
                    reply += string("instance") + string(self.server.address)
                elif tag == 8:
                    reply = struct.pack("<HIQ", 6, 9, 17) + string(tenant)
                elif tag == 10:
                    reply = struct.pack("<HIQ", 6, 11, 1) + string(tenant) + struct.pack("<Q", 17)
                else:
                    assert tag in (19, 20), "unexpected metadata or non-load request"
                    count = struct.unpack("<Q", body.read(8))[0] if tag == 20 else 1
                    entries = []
                    def read_string():
                        size = struct.unpack("<Q", body.read(8))[0]
                        return body.read(size).decode()
                    for _ in range(count):
                        backend = struct.unpack("<I", body.read(4))[0]
                        bucket, key = read_string(), read_string()
                        offset, block_size = struct.unpack("<QI", body.read(12))
                        source_version = read_string()
                        size = struct.unpack("<Q", body.read(8))[0]
                        entries.append((backend, bucket, key, offset, block_size, source_version, size))
                    assert not body.read()
                    self.server.frames.append((tag, entries))
                    self.server.tenant_frames.append((tenant, entries))
                    if self.server.drop_reply:
                        return
                    if tag == 20 and not self.server.reject:
                        failures = [(i, e) for i, e in enumerate(entries) if e[2] == "bad"]
                        reply = struct.pack("<HIQ", 6, 21, len(failures))
                        for i, _ in failures:
                            reply += struct.pack("<I", i) + string("origin failure")
                    else:
                        reply = struct.pack("<HI", 6, 6)
                        reply += b"\0\1" + string("origin unavailable") if self.server.reject else b"\1\0"
                self.wfile.write(struct.pack(">HBBHHII", 0x544C, 1, 0, 0, 0, request_id, len(reply)) + reply)
                self.wfile.flush()
        except Exception as error:
            self.server.errors.append(error)


class LoadTests(unittest.TestCase):
    def test_one_client_concurrent_request_tenants_and_geometry(self):
        with Peer() as peer:
            client = talon.Client(peer.address, block_size=16)
            def exercise(tenant, block_size):
                self.assertEqual(client.read("s3://bucket/key", length=1, tenant=tenant), tenant[0].encode())
                self.assertEqual(client.stat("s3://bucket/key", tenant=tenant).version, tenant)
                self.assertEqual(client.list("s3/bucket", tenant=tenant)[0].path, tenant)
                result = client.load("s3://bucket/file", version="v1", size=9, tenant=tenant, block_size=block_size)
                self.assertEqual(result.blocks, (9 + block_size - 1) // block_size)
                client.batch_load([talon.LoadRequest("s3://bucket/file", "v1", 9)], tenant=tenant, block_size=block_size)
            with ThreadPoolExecutor(2) as calls:
                jobs = [calls.submit(exercise, "a", 4), calls.submit(exercise, "b", 8)]
                for job in jobs:
                    job.result(timeout=10)
            self.assertEqual(client.read("s3://bucket/key", length=1), b"d")
            self.assertEqual(client.load("s3://bucket/file", version="v1", size=9).blocks, 1)
            for tenant, entries in peer.tenant_frames:
                expected = {"a": 4, "b": 8, "default": 16}[tenant]
                self.assertTrue(all(entry[4] == expected for entry in entries))
            self.assertFalse(peer.errors)
            del client

    def test_single_and_batch_use_the_expected_frames(self):
        with Peer() as peer:
            client = talon.Client(peer.address, block_size=8)
            result = client.load("s3://bucket/file", version="v1", size=9)
            self.assertEqual((result.size, result.blocks), (9, 2))
            requests = [talon.LoadRequest("s3://bucket/file-%d" % i, "v2", 3) for i in range(1025)]
            requests.append(talon.LoadRequest("s3://bucket/empty", "v2", 0))
            results = client.batch_load(requests)
            self.assertEqual([(r.size, r.blocks) for r in results], [(3, 1)] * 1025 + [(0, 0)])
            singles = sorted(entry for tag, entries in peer.frames if tag == 19 for entry in entries)
            self.assertEqual(singles, [(0, "bucket", "file", 0, 8, "v1", 8), (0, "bucket", "file", 8, 8, "v1", 1)])
            self.assertEqual(sorted(len(entries) for tag, entries in peer.frames if tag == 20), [1, 1024])
            self.assertTrue(all(e[5:] == ("v2", 3) for tag, entries in peer.frames if tag == 20 for e in entries))
            self.assertFalse(peer.errors)
            del client

    def test_partial_failure_list_and_lost_reply(self):
        with Peer() as peer:
            client = talon.Client(peer.address, block_size=8)
            with self.assertRaises(talon.BatchLoadError) as caught:
                client.batch_load([
                    talon.LoadRequest("s3://bucket/good", "v1", 8),
                    talon.LoadRequest("s3://bucket/bad", "v1", 1025 * 8),
                    talon.LoadRequest("s3://bucket/bad", "v1", 0),
                    talon.LoadRequest("s3://bucket/bad", "v1", 8),
                ])
            self.assertEqual([(f.index, f.uncertain, f.error) for f in caught.exception.failed_files],
                             [(1, False, "origin failure"), (3, False, "origin failure")])
            self.assertEqual(sum(len(entries) for _, entries in peer.frames), 1027)
            del client
        with Peer() as peer:
            peer.drop_reply = True
            client = talon.Client(peer.address, block_size=8)
            with self.assertRaises(talon.BatchLoadError) as caught:
                client.batch_load([talon.LoadRequest("s3://bucket/file", "v1", 8)])
            self.assertEqual([(f.index, f.uncertain) for f in caught.exception.failed_files], [(0, True)])
            del client

    def test_rejection_and_invalid_inputs(self):
        with Peer() as peer:
            peer.reject = True
            client = talon.Client(peer.address, block_size=8)
            with self.assertRaises(OSError):
                client.batch_load([talon.LoadRequest("s3://bucket/file", "v1", 3)])
            self.assertEqual(len(peer.frames), 1, "worker rejection must not be replayed")
            self.assertFalse(peer.errors)
            del client
        client = talon.Client("127.0.0.1:1", block_size=8)
        self.assertEqual(client.batch_load([]), [])
        self.assertEqual(client.load("s3://bucket/empty", version="v1", size=0).blocks, 0)
        with self.assertRaises(TypeError):
            client.load("s3://bucket/file", version="v1")
        with self.assertRaises(ValueError):
            talon.LoadRequest("s3://bucket/file", "", 1)
        with self.assertRaises(talon.UnavailableError):
            client.load("s3://bucket/file", version="v1", size=1)


def run_tests():
    return unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromTestCase(LoadTests)).wasSuccessful()


if __name__ == "__main__":
    unittest.main()
