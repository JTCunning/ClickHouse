#!/usr/bin/env python3
"""Minimal S3-compatible object server for a ClickHouse `s3` disk, with injected GET latency.

Path-style only: http://host:port/<bucket>/<key>. Implements what a MergeTree disk needs:
GET (with Range), HEAD, PUT (plain, aws-chunked, copy), multipart upload, DELETE, POST ?delete, ListObjectsV2.
No auth checks. Every GET is appended to the request log (one JSON object per line) so reads can be
attributed to part files afterwards.

  s3mock.py --root DIR --port 9100 --get-latency-ms 30 --log get.jsonl
Latency can be changed while running: write a number (ms) into DIR/.latency_ms.
"""
import argparse, hashlib, json, os, re, socket, threading, time, uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, unquote
from xml.sax.saxutils import escape

ARGS = None
LOG = None
LOG_LOCK = threading.Lock()
UPLOADS = {}
UPLOADS_LOCK = threading.Lock()


_LAT = [0.0, -1.0]  # value, time read


def latency_s():
    now = time.time()
    if now - _LAT[1] > 0.5:
        try:
            with open(os.path.join(ARGS.root, '.latency_ms')) as f:
                _LAT[0] = float(f.read().strip()) / 1000.0
        except (OSError, ValueError):
            _LAT[0] = ARGS.get_latency_ms / 1000.0
        _LAT[1] = now
    return _LAT[0]


def obj_path(bucket, key):
    p = os.path.normpath(os.path.join(ARGS.root, bucket, key))
    if not p.startswith(os.path.normpath(ARGS.root) + os.sep):
        raise ValueError('bad key')
    return p


def read_body(h):
    te = h.headers.get('Transfer-Encoding', '')
    if 'chunked' in te.lower():
        data = bytearray()
        while True:
            line = h.rfile.readline().strip()
            size = int(line.split(b';')[0], 16)
            if size == 0:
                h.rfile.readline()
                break
            data += h.rfile.read(size)
            h.rfile.readline()
        body = bytes(data)
    else:
        n = int(h.headers.get('Content-Length', '0') or 0)
        body = h.rfile.read(n) if n else b''
    enc = h.headers.get('Content-Encoding', '') + h.headers.get('x-amz-content-sha256', '')
    if 'aws-chunked' in enc or 'STREAMING' in enc:
        out, i = bytearray(), 0
        while i < len(body):
            j = body.index(b'\r\n', i)
            size = int(body[i:j].split(b';')[0], 16)
            if size == 0:
                break
            out += body[j + 2:j + 2 + size]
            i = j + 2 + size + 2
        body = bytes(out)
    return body


class H(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def setup(self):
        super().setup()
        # Headers and body are written separately: without TCP_NODELAY, Nagle + delayed ACK add ~40 ms per response.
        self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def log_message(self, *a):
        pass

    def split(self):
        u = urlparse(self.path)
        parts = u.path.lstrip('/').split('/', 1)
        bucket = unquote(parts[0])
        key = unquote(parts[1]) if len(parts) > 1 else ''
        return bucket, key, parse_qs(u.query, keep_blank_values=True)

    def reply(self, code, body=b'', headers=None):
        self.send_response(code)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        if body and self.command != 'HEAD':
            self.wfile.write(body)

    def xml(self, code, s):
        self.reply(code, ('<?xml version="1.0" encoding="UTF-8"?>' + s).encode(), {'Content-Type': 'application/xml'})

    def not_found(self, key):
        self.xml(404, f'<Error><Code>NoSuchKey</Code><Message>not found</Message><Key>{escape(key)}</Key></Error>')

    def do_HEAD(self):
        bucket, key, q = self.split()
        if not key:
            return self.reply(200)
        p = obj_path(bucket, key)
        if not os.path.isfile(p):
            return self.reply(404)
        st = os.stat(p)
        self.send_response(200)
        self.send_header('Content-Length', str(st.st_size))
        self.send_header('ETag', '"%x"' % int(st.st_mtime_ns))
        self.send_header('Last-Modified', time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime(st.st_mtime)))
        self.end_headers()

    def do_GET(self):
        bucket, key, q = self.split()
        if not key or 'list-type' in q:
            return self.list(bucket, q)
        p = obj_path(bucket, key)
        t0 = time.time()
        time.sleep(latency_s())
        if not os.path.isfile(p):
            return self.not_found(key)
        size = os.path.getsize(p)
        start, end = 0, size - 1
        rng = self.headers.get('Range')
        code = 200
        if rng:
            m = re.match(r'bytes=(\d*)-(\d*)', rng)
            if m.group(1):
                start = int(m.group(1))
                if m.group(2):
                    end = min(int(m.group(2)), size - 1)
            else:
                start = max(0, size - int(m.group(2)))
            code = 206
        if start > end:
            return self.xml(416, '<Error><Code>InvalidRange</Code></Error>')
        with open(p, 'rb') as f:
            f.seek(start)
            data = f.read(end - start + 1)
        headers = {'ETag': '"x"', 'Content-Type': 'application/octet-stream', 'Accept-Ranges': 'bytes'}
        if code == 206:
            headers['Content-Range'] = f'bytes {start}-{end}/{size}'
        self.reply(code, data, headers)
        if LOG:
            with LOG_LOCK:
                LOG.write(json.dumps({'t': round(t0, 4), 'key': key, 'start': start, 'end': end, 'bytes': len(data),
                                    'size': size, 'thread': threading.get_ident()}) + '\n')
                LOG.flush()

    def list(self, bucket, q):
        prefix = q.get('prefix', [''])[0]
        maxk = int(q.get('max-keys', ['1000'])[0])
        after = q.get('continuation-token', q.get('start-after', ['']))[0]
        base = os.path.join(ARGS.root, bucket)
        keys = []
        for d, _, files in os.walk(base):
            for fn in files:
                k = os.path.relpath(os.path.join(d, fn), base)
                if k.startswith(prefix) and k > after and not fn.startswith('.'):
                    keys.append(k)
        keys.sort()
        trunc = len(keys) > maxk
        keys = keys[:maxk]
        items = ''.join(f'<Contents><Key>{escape(k)}</Key><Size>{os.path.getsize(os.path.join(base, k))}</Size>'
                        f'<ETag>"x"</ETag><LastModified>2026-01-01T00:00:00.000Z</LastModified></Contents>' for k in keys)
        nxt = f'<NextContinuationToken>{escape(keys[-1])}</NextContinuationToken>' if trunc else ''
        self.xml(200, f'<ListBucketResult><Name>{escape(bucket)}</Name><Prefix>{escape(prefix)}</Prefix><KeyCount>{len(keys)}</KeyCount>'
                      f'<MaxKeys>{maxk}</MaxKeys><IsTruncated>{"true" if trunc else "false"}</IsTruncated>{nxt}{items}</ListBucketResult>')

    def do_PUT(self):
        bucket, key, q = self.split()
        if not key:
            read_body(self)
            return self.reply(200)
        body = read_body(self)
        src = self.headers.get('x-amz-copy-source')
        if 'partNumber' in q:
            uid = q['uploadId'][0]
            with UPLOADS_LOCK:
                UPLOADS.setdefault(uid, {})[int(q['partNumber'][0])] = body
            return self.reply(200, b'', {'ETag': '"%s"' % hashlib.md5(body).hexdigest()})
        p = obj_path(bucket, key)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        if src:
            s = unquote(src.lstrip('/')).split('/', 1)
            with open(obj_path(s[0], s[1]), 'rb') as f:
                body = f.read()
        tmp = p + '.tmp' + uuid.uuid4().hex
        with open(tmp, 'wb') as f:
            f.write(body)
        os.replace(tmp, p)
        if src:
            return self.xml(200, '<CopyObjectResult><ETag>"x"</ETag><LastModified>2026-01-01T00:00:00.000Z</LastModified></CopyObjectResult>')
        self.reply(200, b'', {'ETag': '"%s"' % hashlib.md5(body).hexdigest()})

    def do_POST(self):
        bucket, key, q = self.split()
        body = read_body(self)
        if 'delete' in q:
            keys = re.findall(rb'<Key>(.*?)</Key>', body)
            out = ''
            for k in keys:
                k = k.decode()
                try:
                    os.remove(obj_path(bucket, k))
                except FileNotFoundError:
                    pass
                out += f'<Deleted><Key>{escape(k)}</Key></Deleted>'
            return self.xml(200, f'<DeleteResult>{out}</DeleteResult>')
        if 'uploads' in q:
            uid = uuid.uuid4().hex
            with UPLOADS_LOCK:
                UPLOADS[uid] = {}
            return self.xml(200, f'<InitiateMultipartUploadResult><Bucket>{escape(bucket)}</Bucket><Key>{escape(key)}</Key>'
                                 f'<UploadId>{uid}</UploadId></InitiateMultipartUploadResult>')
        if 'uploadId' in q:
            uid = q['uploadId'][0]
            with UPLOADS_LOCK:
                parts = UPLOADS.pop(uid, {})
            p = obj_path(bucket, key)
            os.makedirs(os.path.dirname(p), exist_ok=True)
            with open(p, 'wb') as f:
                for n in sorted(parts):
                    f.write(parts[n])
            return self.xml(200, f'<CompleteMultipartUploadResult><Bucket>{escape(bucket)}</Bucket><Key>{escape(key)}</Key>'
                                 f'<ETag>"x"</ETag></CompleteMultipartUploadResult>')
        self.reply(400)

    def do_DELETE(self):
        bucket, key, q = self.split()
        if 'uploadId' in q:
            with UPLOADS_LOCK:
                UPLOADS.pop(q['uploadId'][0], None)
            return self.reply(204)
        try:
            os.remove(obj_path(bucket, key))
        except FileNotFoundError:
            pass
        self.reply(204)


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--root', required=True)
    ap.add_argument('--port', type=int, default=9100)
    ap.add_argument('--get-latency-ms', type=float, default=0.0)
    ap.add_argument('--log', default='')
    ARGS = ap.parse_args()
    LOG = open(ARGS.log, 'a') if ARGS.log else None
    os.makedirs(ARGS.root, exist_ok=True)
    class Server(ThreadingHTTPServer):
        request_queue_size = 1024  # read by listen() in the constructor; the default 5 drops SYNs under a burst (1 s retransmits)
        daemon_threads = True

    srv = Server(('127.0.0.1', ARGS.port), H)
    srv.serve_forever()
