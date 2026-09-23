#!/usr/bin/env python3
"""Attach to an acp-multiplex socket as a secondary frontend (a fake phone),
send one session/prompt, and print what comes back for a while."""
import json
import socket
import sys
import time

sock_path, text, wait = sys.argv[1], sys.argv[2], float(sys.argv[3])
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.connect(sock_path)
s.settimeout(0.5)
buf = b""
session_id = None
kinds = []
deadline = time.time() + 5


def lines():
    global buf
    try:
        chunk = s.recv(65536)
        if not chunk:
            return []
        buf += chunk
    except socket.timeout:
        return []
    out = []
    while b"\n" in buf:
        line, buf = buf.split(b"\n", 1)
        if line.strip():
            out.append(line)
    return out


# Phase 1: drain the replay snapshot, find the session id.
while time.time() < deadline:
    for line in lines():
        try:
            msg = json.loads(line)
        except Exception:
            continue
        result = msg.get("result") or {}
        if isinstance(result, dict) and result.get("sessionId"):
            session_id = result["sessionId"]
        params = msg.get("params") or {}
        if params.get("sessionId"):
            session_id = session_id or params["sessionId"]
        upd = (params.get("update") or {}).get("sessionUpdate")
        if upd:
            kinds.append(upd)
print("snapshot kinds:", kinds[-8:])
print("session:", session_id)
if not session_id:
    sys.exit("no session id found in snapshot")

req = {"jsonrpc": "2.0", "id": 4242, "method": "session/prompt",
       "params": {"sessionId": session_id,
                  "prompt": [{"type": "text", "text": text}]}}
s.sendall((json.dumps(req) + "\n").encode())
print("sent prompt")

# Phase 2: print what streams back.
deadline = time.time() + wait
seen = []
while time.time() < deadline:
    for line in lines():
        try:
            msg = json.loads(line)
        except Exception:
            continue
        if msg.get("id") == 4242:
            print("RESPONSE:", json.dumps(msg)[:300])
            deadline = min(deadline, time.time() + 3)
            continue
        params = msg.get("params") or {}
        upd = params.get("update") or {}
        kind = upd.get("sessionUpdate")
        if kind:
            txt = (upd.get("content") or {}).get("text", "")
            seen.append(kind)
            if txt:
                print(f"  {kind}: {txt[:100]!r}")
print("update kinds seen:", seen)
