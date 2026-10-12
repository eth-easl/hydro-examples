#!/usr/bin/env python3
"""Exercise the running Hydro HTTP service using only Python's standard library."""
import concurrent.futures
import struct
import sys
import urllib.error
import urllib.request
import zlib

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8080"


def chunk(kind, data):
    return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data))


def png(width, height):
    rows = (b"\0" + bytes([23, 80, 160]) * width) * height
    return (b"\x89PNG\r\n\x1a\n"
            + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(rows)) + chunk(b"IEND", b""))


def request(width, height, body, expected_status=200):
    req = urllib.request.Request(
        f"{BASE}/thumbnail?width={width}&height={height}", data=body,
        headers={"Content-Type": "application/octet-stream"}, method="POST")
    try:
        response = urllib.request.urlopen(req, timeout=35)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        assert response.status == expected_status, (response.status, response.read())
        result = response.read()
        if expected_status == 200:
            assert response.headers["Content-Type"] == "image/png"
            assert result[:8] == b"\x89PNG\r\n\x1a\n"
            return struct.unpack(">II", result[16:24])


with urllib.request.urlopen(f"{BASE}/health", timeout=5) as response:
    assert response.read() == b"ok"

image = png(80, 40)
assert request(20, 20, image) == (20, 10)
assert request(200, 200, image) == (80, 40)
request(0, 20, image, 400)
request(2049, 20, image, 400)
request(20, 20, b"invalid image", 422)
request(20, 20, b"x" * (8 * 1024 * 1024 + 1), 413)
# Different dimensions exercise response correlation across concurrent workers.
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
    sizes = list(range(2, 34, 2))
    results = list(pool.map(lambda size: request(size, size, image), sizes))
    assert results == [(size, size // 2) for size in sizes], results
print("PASS: resize, no upscaling, validation, body limit, and 16 concurrent requests")
