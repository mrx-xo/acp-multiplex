#!/usr/bin/env python3
"""Attach to an acp-multiplex socket as a secondary frontend and log every
session/update text chunk for a while. Listen only, never prompts."""
import json
import socket
import sys
import time

sock_path, wait = sys.argv[1], float(sys.argv[2])
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.connect(sock_path)
s.settimeout(0.5)
buf = b""
deadline = time.time() + wait
started = time.time()
while time.time() < deadline:
    try:
        chunk = s.recv(65536)
        if not chunk:
            break
        buf += chunk
    except socket.timeout:
        continue
    while b"\n" in buf:
        line, buf = buf.split(b"\n", 1)
        try:
            msg = json.loads(line)
        except Exception:
            continue
        upd = ((msg.get("params") or {}).get("update") or {})
        kind = upd.get("sessionUpdate")
        if not kind:
            continue
        txt = (upd.get("content") or {}).get("text", "")
        if kind in ("user_message_chunk", "turn_complete") or txt:
            print(f"{time.time() - started:5.1f}s {kind}: {txt[:80]!r}", flush=True)
