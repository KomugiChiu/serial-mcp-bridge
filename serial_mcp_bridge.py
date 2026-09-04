#!/usr/bin/env python3
"""
Serial Broadcast Bridge MCP Server
==================================
Let an AI agent and a user share the same serial port: each side sees what the other does, and device responses are broadcast to everyone.

Transport:
  - MCP (stdio) :  AI agent (e.g. opencode) spawns this program as a local MCP and talks over stdio
  - TCP text    :  User connects via PuTTY / MobaXterm over telnet/raw to watch output and type

Usage:
  python serial_mcp_bridge.py --port /dev/ttyUSB0 --baud 115200
  python serial_mcp_bridge.py --port COM3 --baud 9600 --tcp 7001
  python serial_mcp_bridge.py --port /dev/ttyUSB0 --baud 115200 --auto-connect

  opencode local MCP config spawns this program (stdio) and opens the TCP bridge for users in the background.

Dependencies:
  pip install "mcp>=1.0,<2" pyserial
"""
import argparse
import os
import socket
import threading
import time
import sys
import unicodedata
from collections import deque
from datetime import datetime
from typing import Optional

try:
    import serial
except ImportError:
    print("[ERROR] pyserial required: pip install pyserial")
    sys.exit(1)

try:
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    from mcp.types import TextContent, Tool
except ImportError:
    print("[ERROR] mcp (1.x) required: pip install \"mcp>=1.0,<2\"")
    sys.exit(1)

# ============================================================
# Global state (one shared serial port)
# ============================================================
serial_conn: Optional[serial.Serial] = None
serial_lock = threading.Lock()
history = deque(maxlen=1000)          # History (disable recording with --no-history; disable replay for new TCP clients with --no-tcp-history)
history_lock = threading.Lock()     # Guards concurrent history access
enable_history = True               # Whether to record history, controlled by --no-history
enable_tcp_history = True           # Whether new TCP clients get a history snapshot, controlled by --no-tcp-history (AI get/search unaffected)
# Unified receive buffer: only the serial_reader thread reads the hardware; MCP read/readline/write_read all consume from here
rx_buffer: deque = deque()          # Each item is a bytes chunk (device data after echo removal)
rx_lock = threading.Lock()
rx_cond = threading.Condition(rx_lock)
tcp_clients = []                      # TCP connections of users
tcp_clients_lock = threading.Lock()
pending_chunks: deque = deque()  # [(bytes, monotonic_ts)] just sent, awaiting device echo match
echo_lock = threading.Lock()
ECHO_EXPIRE_S = 1.0             # An echo not seen within this many seconds counts as lost (keeps stale data from poisoning later dedup)
ECHO_MAX_BYTES = 4096         # Pending cap, drops oldest when exceeded (512 bytes fill in ~40ms at 115200, so keep it roomy to avoid echo leaks under load)
log_fp = None                       # File handle opened by --log-file / !start_log (persistent)
log_fp_lock = threading.Lock()
log_owner: Optional[str] = None     # Who opened the current log file: None / "startup(--log-file)" / "User(addr)" / "AI"
log_name: Optional[str] = None      # Current log file name (for display)
log_dir = "logs"                    # Overridden by --log-dir; files from !start_log / serial_log_start go here
LOG_HOLD_MAX = 4096                 # Max bytes to hold a ! line (beyond this, give up holding and forward everything to the device)
# Newline appended on send: some devices (e.g. Android shell, which maps CR to LF via ICRNL) treat CRLF
# as two newlines = one extra empty command = doubled prompt. Use --line-ending to switch to lf.
LINE_ENDINGS = {"crlf": b"\r\n", "lf": b"\n", "cr": b"\r"}
default_line_ending = "lf"          # Overridden by --line-ending
cooked_mode = False               # Raw passthrough by default; --cooked enables bridge-side line editing (local echo + Up/Down history)
running = True


def _emit(line: str):
    """Write to stderr and tee into --log-file (if any)."""
    print(line, file=sys.stderr, flush=True)
    with log_fp_lock:
        fp = log_fp
        if fp is not None:
            try:
                fp.write(line + "\n")
                fp.flush()
            except Exception:
                pass


def log(msg: str, to_history: bool = True):
    # Short source tags: [SYS]/[ERR]/[WARN] -> [sys]/[err]/[warn].
    for _a, _b in (("[SYS]", "[sys]"), ("[ERR]", "[err]"), ("[WARN]", "[warn]")):
        if msg.startswith(_a):
            msg = _b + msg[len(_a):]
            break
    timestamp = datetime.now().strftime("%H:%M:%S")
    line = _clean_log_text(f"[{timestamp}] {msg}")
    _emit(line)
    if to_history and enable_history:
        with history_lock:
            history.append(line)


# Keep emoji/special symbols out of logs: strip So (symbols, incl. emoji) / Sk (modifier symbols) /
# Me/Mn (enclosing/combining marks, incl. emoji VS16) / Cf (format chars). Keeps CJK (Lo),
# common punctuation (Po/Ps/Pe), currency $ (Sc, used in shell prompts), and spacing.
_LOG_STRIP_CATS = frozenset({"So", "Sk", "Me", "Mn", "Cf"})


def _clean_log_text(text: str) -> str:
    """Strip emoji/special symbols before logging (pure function, easy to test).

    Only non-ASCII is touched: ASCII ^, backtick etc. stay intact (no mangling of ^C or shell backticks).
    """
    return "".join(
        c for c in text
        if ord(c) < 128 or unicodedata.category(c) not in _LOG_STRIP_CATS
    ).strip()


# Short log source tags (log/history/TCP relay all use these).
_SRC_TAG = {"AI": "[_ai]", "User": "[usr]", "Device": "[dev]", "SYS": "[sys]"}


def _short_source(show_as: str) -> str:
    """Map 'AI -> Device'/'User(..) -> Device'/'Device -> ALL' to [_ai]/[usr]/[dev] (pure function)."""
    if show_as.startswith("Device"):
        return "[dev]"
    if show_as.startswith("AI"):
        return "[_ai]"
    if show_as.startswith("User"):
        return "[usr]"
    return f"[{show_as}]"


def _history_snapshot() -> list:
    """Thread-safe copy of the history."""
    with history_lock:
        return list(history)


def _should_send_tcp_history(snapshot: list) -> bool:
    """Whether a new TCP connection gets a history snapshot (pure function, easy to test).

    --no-tcp-history only disables TCP replay: history is still recorded, and AI
    serial_get_history / serial_search_history plus live broadcast are unaffected.
    --no-history is the master switch; nothing is replayed once it is off.
    """
    return bool(enable_history and enable_tcp_history and snapshot)


LOG_HELP = ("bridge commands (handled by the bridge, never sent to the device):\n"
            "  !start_log [name]  start writing all input/output to a log file (auto-named if omitted)\n"
            "  !stop_log          stop recording\n"
            "  !log_status        show recording status\n"
            "  !help              show this help\n"
            "  !!prefix           escape hatch, forwarded to the device literally (e.g. !!ls sends !ls)")


def _sanitize_log_name(name: str) -> str:
    """Whitelist filename filter: basename only, illegal chars become _ (pure function, easy to test)."""
    base = os.path.basename(name.strip())
    if base in ("", ".", ".."):
        return ""
    clean = "".join(c if (c.isascii() and (c.isalnum() or c in "._-")) else "_" for c in base)
    if clean in ("", ".", ".."):
        return ""
    return clean


def _logfile_start(filename: str, owner: str) -> str:
    """Open the global log file (shared by --log-file / !start_log / serial_log_start). Returns a message."""
    global log_fp, log_owner, log_name
    with log_fp_lock:
        if log_fp is not None:
            return f"Already recording: {log_name} ({log_owner}); stop it first"
        name = _sanitize_log_name(filename) if filename else ""
        if not name:
            name = "serial-" + datetime.now().strftime("%Y%m%d-%H%M%S") + ".log"
        try:
            os.makedirs(log_dir, exist_ok=True)
            path = os.path.join(log_dir, name)
            log_fp = open(path, "a", encoding="utf-8")
            log_owner, log_name = owner, name
        except Exception as e:
            return f"Cannot open log file: {e}"
    # Release the lock before log(): log() -> _emit() takes the same lock
    log(f"[SYS] Recording started: {name} ({owner})")
    return f"Recording started: {name}"


def _logfile_stop(by: str) -> str:
    """Close the global log file. Returns a message (who stopped it goes to history for audit)."""
    global log_fp, log_owner, log_name
    with log_fp_lock:
        if log_fp is None:
            return "Not recording"
        name, owner = log_name, log_owner
        try:
            log_fp.close()
        except Exception:
            pass
        log_fp, log_owner, log_name = None, None, None
    log(f"[SYS] Recording stopped: {name} (started by {owner}, stopped by {by})")
    return f"Recording stopped: {name}"


def _logfile_status() -> str:
    with log_fp_lock:
        if log_fp is None:
            return "Not recording"
        return f"Recording: {log_name} ({log_owner})"


def _tcp_announce(text: str, skip=()):
    """System broadcast to all TCP users (so the other side knows when recording starts/stops)."""
    data = (text + "\n").encode("utf-8", errors="replace")
    skipset = set(skip) if skip else set()
    with tcp_clients_lock:
        targets = [c for c in tcp_clients if c not in skipset]
    dead = []
    for c in targets:
        try:
            c.sendall(data)
        except Exception:
            dead.append(c)
    if dead:
        with tcp_clients_lock:
            for d in dead:
                try:
                    tcp_clients.remove(d)
                except ValueError:
                    pass
                log("[SYS] TCP client disconnected (during broadcast)")


def _parse_log_command(line: str) -> tuple:
    """Parse a ! command line (stripped, no newline). Returns (cmd, arg).

    cmd is one of start/stop/status/help/escape/unknown.
    """
    if line.startswith("!!"):
        return ("escape", line[1:])
    if not line.startswith("!"):
        return ("unknown", line)
    parts = line[1:].split(None, 1)
    cmd = parts[0].lower() if parts else ""
    arg = parts[1] if len(parts) > 1 else ""
    if cmd in ("start_log", "startlog", "start"):
        return ("start", arg.strip())
    if cmd in ("stop_log", "stoplog", "stop"):
        return ("stop", "")
    if cmd in ("log_status", "status"):
        return ("status", "")
    if cmd in ("help", "?"):
        return ("help", "")
    return ("unknown", line)


def _make_command_processor(conn, addr):
    """Per-TCP-connection ! line processor: process(line_incl_newline) -> bytes to forward to the device."""
    owner = f"User({addr})"

    def reply(text: str):
        try:
            conn.sendall((text.rstrip("\n") + "\n").encode("utf-8", errors="replace"))
        except Exception:
            pass

    def process(line: bytes) -> bytes:
        try:
            text = line.decode("utf-8", errors="replace").strip()
        except Exception:
            return line
        cmd, arg = _parse_log_command(text)
        if cmd == "start":
            msg = _logfile_start(arg, owner)
            reply(msg)
            _tcp_announce(f"[sys] {owner}: {msg}", skip=(conn,))
            return b""
        if cmd == "stop":
            msg = _logfile_stop(owner)
            reply(msg)
            _tcp_announce(f"[sys] {owner}: {msg}", skip=(conn,))
            return b""
        if cmd == "status":
            reply(_logfile_status())
            return b""
        if cmd == "help":
            reply(LOG_HELP)
            return b""
        if cmd == "escape":
            return (arg + "\n").encode("utf-8", errors="replace")
        # Unknown !xxx: forward to the device as-is + a private hint (never swallow user input)
        reply("Unknown bridge command (forwarded to the device as-is). Type !help for help.")
        return line

    return process


class TcpCommandFilter:
    """! command interceptor for raw TCP connections (works with char-at-a-time telnet).

    Rules:
    - Hold a line only when ! arrives at a line start (fresh connection or right after a newline);
      everything else passes through with zero delay, so interactivity is unaffected.
    - Multi-line packets are scanned line by line, so a pasted ! command starting a later
      line in the same packet is still intercepted.
    - While holding, buffer printable chars with live echo; give up on backspace/ESC/control chars/overflow,
      forwarding the whole chunk to the device (it handles editing), so the bridge never swallows input.
    - Once the newline arrives, judge the whole line: bridge commands run locally, the rest is forwarded as-is.
    """

    def __init__(self):
        self.buf = bytearray()
        self.holding = False
        self.at_line_start = True

    def _track_line(self, data: bytes):
        if not data:
            return
        last_nl = max(data.rfind(b"\n"), data.rfind(b"\r"))
        if last_nl >= 0:
            self.at_line_start = (last_nl == len(data) - 1)
        else:
            self.at_line_start = False

    def feed(self, data: bytes, process_line) -> tuple:
        """Returns (to_device, echo_to_sender, consumed_command).

        Echo contract: bytes the sender should see are returned here exactly once; the caller just
        conn.sendall(echo), and to_device never goes through pushback again.

        Iterative (no recursion): a single packet full of short ! lines must not grow the call stack.
        """
        to_dev = bytearray()
        echo = bytearray()
        consumed = False
        pending = bytes(data)
        while pending:
            if not self.holding:
                if self.at_line_start and pending[:1] == b"!":
                    self.holding = True
                else:
                    # Passthrough one line at a time, so a ! command starting a later
                    # line in the same packet is still intercepted (pasted input).
                    nl = len(pending)
                    for k, byte in enumerate(pending):
                        if byte in (0x0A, 0x0D):
                            nl = k + 1
                            break
                    seg, pending = pending[:nl], pending[nl:]
                    if _wants_pushback(seg):
                        echo += seg
                    to_dev += seg
                    self._track_line(seg)
                    continue
            # Holding: scan byte-by-byte until newline / abandon / end of packet.
            i, n = 0, len(pending)
            while i < n:
                b = pending[i]
                if b in (0x0A, 0x0D):
                    self.buf.append(b)
                    line = bytes(self.buf)
                    self.buf.clear()
                    self.holding = False
                    self.at_line_start = True
                    fwd = process_line(line)
                    if fwd == b"":
                        consumed = True
                    else:
                        to_dev += fwd  # Already echoed char-by-char while holding; no extra echo
                    pending = pending[i + 1:]
                    break
                elif 0x20 <= b <= 0x7E or b >= 0x80:
                    if len(self.buf) >= LOG_HOLD_MAX:
                        rest = bytes(self.buf) + pending[i:]
                        to_dev += rest
                        if _wants_pushback(rest):
                            echo += rest
                        self._track_line(rest)
                        self.buf.clear()
                        self.holding = False
                        pending = b""
                        break
                    self.buf.append(b)
                    echo += bytes((b,))
                    i += 1
                else:
                    # Backspace/ESC/other control chars: give up holding, forward everything to the device
                    rest = bytes(self.buf) + pending[i:]
                    to_dev += rest
                    tail = pending[i:]
                    if _wants_pushback(tail):
                        echo += tail
                    self._track_line(rest)
                    self.buf.clear()
                    self.holding = False
                    pending = b""
                    break
            else:
                # Whole packet buffered while holding; wait for the newline in a later packet.
                pending = b""
        return bytes(to_dev), bytes(echo), consumed


def _filter_cooked_commands(dev: bytes, process_line) -> tuple:
    """Cooked mode: every submit is a complete line; judge line by line. Returns (to_device, consumed)."""
    out = bytearray()
    consumed = False
    for line in dev.splitlines(keepends=True):
        if line[:1] == b"!" and line.endswith((b"\n", b"\r")):
            fwd = process_line(line)
            if fwd == b"":
                consumed = True
            else:
                out += fwd
        else:
            out += line
    return bytes(out), consumed


def _rx_push(data: bytes):
    """Push device data into the unified receive buffer (serial_reader path only)."""
    if not data:
        return
    with rx_cond:
        rx_buffer.append(bytes(data))
        rx_cond.notify_all()


def _rx_drain() -> bytes:
    """Take all currently buffered data (hardware buffer untouched; the reader keeps draining it)."""
    with rx_cond:
        if not rx_buffer:
            return b""
        chunks = list(rx_buffer)
        rx_buffer.clear()
        return b"".join(chunks)


def _rx_wait_and_drain(timeout_s: float, predicate=None) -> bytes:
    """Wait until the predicate holds or timeout, then atomically take everything under the same lock.

    Avoids the two-step wait-then-drain race: data arriving in between
    is not swept along; exactly the judged content is taken (on timeout, whatever is there,
    b"" if empty).
    """
    deadline = time.monotonic() + timeout_s
    with rx_cond:
        while True:
            blob = b"".join(rx_buffer)
            hit = bool(blob) if predicate is None else predicate(blob)
            if hit:
                chunks = list(rx_buffer)
                rx_buffer.clear()
                return b"".join(chunks)
            if time.monotonic() >= deadline:
                chunks = list(rx_buffer)
                rx_buffer.clear()
                return b"".join(chunks)
            rx_cond.wait(timeout=max(0.0, deadline - time.monotonic()))


def _rx_take_line(timeout_s: float) -> tuple[bytes, bool]:
    """Wait for one line and take it atomically (judge + split + push back the rest under one lock).

    Returns (payload, has_newline):
    - Complete line: payload is the first line incl. `\\n`, remainder pushed back;
    - Timeout with fragments: payload is everything buffered (consumed, so nothing wedges), has_newline=False;
    - Timeout with nothing: (b"", False).
    """
    deadline = time.monotonic() + timeout_s
    with rx_cond:
        while True:
            blob = b"".join(rx_buffer)
            if b"\n" in blob:
                rx_buffer.clear()
                line, rest = blob.split(b"\n", 1)
                line += b"\n"
                if rest:
                    rx_buffer.appendleft(rest)
                    rx_cond.notify_all()
                return line, True
            if time.monotonic() >= deadline:
                if blob:
                    rx_buffer.clear()
                    return blob, False
                return b"", False
            rx_cond.wait(timeout=max(0.0, deadline - time.monotonic()))


def _pending_push(data: bytes):
    """Record just-sent bytes for echo dedup (timestamped, expires automatically)."""
    if not data:
        return
    now = time.monotonic()
    with echo_lock:
        pending_chunks.append((bytes(data), now))
        total = sum(len(c) for c, _ in pending_chunks)
        while total > ECHO_MAX_BYTES and pending_chunks:
            old, _ = pending_chunks.popleft()
            total -= len(old)


def _pending_clear():
    """Clear the pending-echo queue and the unified receive buffer (avoid stale data polluting reconnects)."""
    with echo_lock:
        pending_chunks.clear()
    _rx_drain()


def _pending_consume(data: bytes) -> bytes:
    """Strip echo from the front of device data (cross-chunk match; lost/expired/mangled echo is skipped).

    Only the matching prefix is stripped, real output behind it is untouched; the serial
    stream is ordered, so a non-matching pending head must be a lost echo - drop and retry
    (drop pending only, never data).
    """
    if not data:
        return data
    now = time.monotonic()
    buf = bytes(data)
    with echo_lock:
        while buf and pending_chunks:
            chunk, ts = pending_chunks[0]
            if now - ts > ECHO_EXPIRE_S:
                pending_chunks.popleft()
                continue
            n = len(chunk) if len(chunk) < len(buf) else len(buf)
            i = 0
            while i < n and chunk[i] == buf[i]:
                i += 1
            if i == 0:
                pending_chunks.popleft()  # This echo chunk was lost or rewritten by the device; drop and retry
                continue
            buf = buf[i:]
            if i >= len(chunk):
                pending_chunks.popleft()
            else:
                pending_chunks[0] = (chunk[i:], ts)
                break
        return buf


# ============================================================
# Low-level serial operations
# ============================================================
def open_serial(port: str, baudrate: int, timeout: float = 0.1,
                bytesize: int = 8, parity: str = 'N', stopbits: float = 1) -> bool:
    global serial_conn
    try:
        log(f"[SYS] Opening {port} @ {baudrate} ...")
        with serial_lock:
            if serial_conn and serial_conn.is_open:
                serial_conn.close()
            serial_conn = serial.Serial(
                port=port, baudrate=baudrate, timeout=timeout,
                write_timeout=1, bytesize=bytesize, parity=parity, stopbits=stopbits,
            )
        log(f"[SYS] Opened {port} @ {baudrate}")
        _pending_clear()  # On reconnect, drop leftover echo expectations and buffered reads from last time
        return True
    except Exception as e:
        log(f"[ERR] Failed to open {port}: {e}")
        # The old connection was closed above; no stale echo/buffered reads may leak into the next one
        _pending_clear()
        return False


def close_serial():
    global serial_conn
    closed = False
    with serial_lock:
        if serial_conn and serial_conn.is_open:
            serial_conn.close()
            closed = True
    if closed:
        log("[SYS] Serial closed")
    _pending_clear()


def write_serial(data: bytes, source: str = "AI", to_tcp: bool = True, skip_conns=()) -> bool:
    """Send data to the device. to_tcp=False skips TCP; skip_conns skips given connections.

    Sender echo is the caller's job (raw TCP via TcpCommandFilter, cooked via local echo,
    MCP AI needs none); this only forwards and records history.
    Input is kept even when disconnected: recorded as (not sent) for monitoring/debugging.
    """
    with serial_lock:
        conn = serial_conn
        is_open = conn is not None and conn.is_open
        if is_open:
            try:
                assert conn is not None
                conn.write(data)
                conn.flush()
                wrote = True
            except Exception as e:
                log(f"[ERR] Write error: {e}")
                return False
        else:
            wrote = False
    if wrote:
        # Echo expectation first (reader may already be delivering the echo),
        # then history/TCP fan-out without holding serial_lock (blocking I/O).
        _pending_push(data)
        broadcast_text(data, show_as=f"{source} -> Device", to_tcp=to_tcp,
                       skip_conns=skip_conns)
        return True
    else:
        broadcast_text(data, show_as=f"{source} -> Device (not sent)", to_tcp=False)
        log("[WARN] Serial not connected, command not sent")
        return False


# ============================================================
# Broadcast: serial reads -> TCP (users) + history
# ============================================================
def broadcast_text(data: bytes, show_as: str, to_tcp: bool = True, skip_conns=()):
    """Broadcast bytes to the users TCP clients and record history (MCP AI reads it via serial_get_history)"""
    if not data:
        return

    is_device = show_as.startswith("Device")
    # Device echo dedup: when the device echoes just-sent input, strip the matching prefix (cross-packet, expiry-aware)
    if is_device:
        data = _pending_consume(data)

    if not data:
        return

    if is_device:
        # Unified receive path: device data enters rx_buffer first; MCP read/readline/write_read consume from there
        _rx_push(data)

    tag = _short_source(show_as)
    suffix = " (not sent)" if "(not sent)" in show_as else ""
    try:
        text = data.decode('utf-8', errors='replace').strip()
        if text:
            for line in text.splitlines():
                log(f"{tag} {line}{suffix}")
    except Exception:
        log(f"{tag} {data.hex()}{suffix}")

    if not to_tcp:
        return

    # Push to the users TCP clients (copy-then-send: never hold the lock across blocking I/O)
    skip = set(skip_conns) if skip_conns else set()
    with tcp_clients_lock:
        targets = [c for c in tcp_clients if c not in skip]
    dead = []
    for conn in targets:
        try:
            conn.sendall(data)
        except Exception:
            dead.append(conn)
    if dead:
        with tcp_clients_lock:
            for d in dead:
                try:
                    tcp_clients.remove(d)
                except ValueError:
                    pass
                log("[SYS] TCP client disconnected")


def serial_reader():
    """Reader thread: serial -> broadcast.

    Reading never takes `serial_lock`: `read()` with a timeout (default 0.1s) blocks,
    and holding the lock would stall `write_serial` for up to 100ms. Roles are fixed
    (only this thread reads, `write_serial` writes), so they run concurrently over
    pyserial full-duplex; `close`/`open` races are absorbed by try/except
    (PortNotOpenError / OSError) and `serial_conn` is re-fetched next round.
    """
    log("[SYS] Serial reader thread started")
    while running:
        try:
            with serial_lock:
                conn = serial_conn
                is_open = conn is not None and conn.is_open
            if not is_open or conn is None:
                time.sleep(0.1)
                continue

            try:
                waiting = conn.in_waiting
            except Exception:
                time.sleep(0.1)
                continue
            if waiting:
                try:
                    # No lock: blocking read must not stall the writer
                    data = conn.read(waiting or 1)
                except Exception:
                    data = b""
                if data:
                    broadcast_text(data, "Device -> ALL")
            else:
                try:
                    try:
                        # No lock: the writer stays unblocked during the 0.1s timeout
                        data = conn.read(1)
                    except Exception:
                        data = b""
                    if data:
                        try:
                            extra = conn.in_waiting
                        except Exception:
                            extra = 0
                        if extra:
                            try:
                                data += conn.read(extra)
                            except Exception:
                                pass
                        broadcast_text(data, "Device -> ALL")
                        continue
                except Exception:
                    pass
                time.sleep(0.02)
        except Exception as e:
            log(f"[ERR] Serial read error: {e}")
            time.sleep(1)


# ============================================================
# TCP bridge: for (human) users via PuTTY/telnet
# ============================================================
def tcp_bridge(listen_host: str, listen_port: int):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        srv.bind((listen_host, listen_port))
    except Exception as e:
        log(f"[ERR] Cannot bind TCP {listen_host}:{listen_port} - {e}")
        return
    srv.listen(10)
    srv.settimeout(0.5)  # So accept() unblocks when running=False for a clean shutdown
    log(f"[SYS] TCP bridge listening for users on {listen_host}:{listen_port} ...")

    try:
        while running:
            try:
                conn, addr = srv.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            except Exception:
                break
            with tcp_clients_lock:
                tcp_clients.append(conn)
            threading.Thread(target=handle_tcp_client, args=(conn, addr), daemon=True).start()
    finally:
        try:
            srv.close()
        except Exception:
            pass


def handle_tcp_client(conn, addr):
    with tcp_clients_lock:
        n_now = len(tcp_clients)
    log(f"[SYS] User connected {addr} ({n_now} total)")
    try:
        snapshot = _history_snapshot()
        if _should_send_tcp_history(snapshot):
            conn.sendall(("\n".join(snapshot) + "\n").encode('utf-8', errors='replace'))
        conn.sendall("--- Connected. Type commands; AI/human actions are broadcast to each other ---\n".encode('utf-8'))
        conn.sendall("--- bridge commands: !start_log [name] / !stop_log / !log_status / !help (!! escapes) ---\n".encode('utf-8'))
        if cooked_mode:
            conn.sendall("--- Line editing on: Up/Down recalls history ---\n".encode('utf-8'))
    except Exception:
        pass

    editor = CookedLine() if cooked_mode else None
    cmdfilter = TcpCommandFilter()
    process_cmd = _make_command_processor(conn, addr)
    try:
        while running:
            data = conn.recv(4096)
            if not data:
                break
            data = _normalize_user_input(data)  # Normalize newlines per --line-ending (lf mode turns PuTTY CRLF into LF)
            if not data:
                continue
            consumed = False
            if editor is not None:
                conn_out, dev_out = editor.feed(data)
                if conn_out:
                    try:
                        conn.sendall(conn_out)
                    except Exception:
                        break
                data = dev_out
                if not data:
                    continue
                data, consumed = _filter_cooked_commands(data, process_cmd)
                if not data:
                    continue
            else:
                # Raw: ! line interception (incl. char-at-a-time telnet); the filter guarantees exactly-once echo,
                # so to_device never goes through pushback (to_tcp=False).
                data, echo, consumed = cmdfilter.feed(data, process_cmd)
                if echo:
                    try:
                        conn.sendall(echo)
                    except Exception:
                        break
                if not data:
                    continue
            if consumed:
                # Consumed bridge commands skip the [tag] relay (others are told via _tcp_announce instead)
                write_serial(data, source=f"User({addr})", to_tcp=False)
                continue
            with tcp_clients_lock:
                others = [c for c in tcp_clients if c != conn]
            dead = []
            for c in others:
                try:
                    c.sendall(b"[usr] " + data)
                except Exception:
                    dead.append(c)
            if dead:
                with tcp_clients_lock:
                    for d in dead:
                        try:
                            tcp_clients.remove(d)
                        except ValueError:
                            pass
                        log("[SYS] TCP client disconnected (during relay)")
            # History is recorded exactly once, by write_serial -> broadcast_text as [usr].
            # Sender echo: cooked via editor-local echo; raw exactly once via TcpCommandFilter,
            # so to_tcp=False here always (others see live input via the [tag] relay above).
            write_serial(data, source=f"User({addr})", to_tcp=False)
    except Exception as e:
        log(f"[SYS] User {addr} error: {e}")
    finally:
        with tcp_clients_lock:
            if conn in tcp_clients:
                tcp_clients.remove(conn)
        conn.close()
        log(f"[SYS] User disconnected {addr} ({len(tcp_clients)} left)")


# ============================================================
# MCP Server (v1.x classic Server + stdio transport)
# ============================================================
server = Server("serial-bridge-mcp")


def _text(msg: str) -> list[TextContent]:
    return [TextContent(type="text", text=msg)]


def _encode_write_data(data_str: str, encoding: str, append_crlf: bool, line_ending=None):
    """Convert write/write_read string args to bytes; returns (data, err_msg).

    append_crlf=False adds no newline; otherwise line_ending is appended (server --line-ending when unset).
    append_crlf applies in hex mode too (it used to be silently ignored).
    """
    if encoding == "hex":
        try:
            data = bytes.fromhex(data_str)
        except ValueError:
            return b"", "Bad hex format"
    else:
        try:
            data = data_str.encode(encoding)
        except (LookupError, ValueError):
            return b"", f"Unsupported encoding: {encoding}"
    if append_crlf:
        ending = line_ending or default_line_ending
        suffix = LINE_ENDINGS.get(ending)
        if suffix is None:
            return b"", f"Unsupported line ending: {ending} (crlf/lf/cr)"
        data += suffix
    return data, ""


def _normalize_user_input(data: bytes) -> bytes:
    """Normalize TCP user input newlines per server --line-ending (PuTTY Enter usually sends CRLF).

    lf mode (default) maps CRLF/CR to LF; cr mode maps CRLF/LF to CR; crlf passes through.
    """
    if default_line_ending == "lf":
        return data.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    if default_line_ending == "cr":
        return data.replace(b"\r\n", b"\r").replace(b"\n", b"\r")
    return data


def _is_serial_open() -> bool:
    with serial_lock:
        conn = serial_conn
        return conn is not None and conn.is_open


class CookedLine:
    """Line editor for one TCP connection (--cooked mode, for dumb shells without line editing).

    The bridge takes over local echo: printable chars echo immediately, the whole line
    goes to the device only on Enter; Up/Down recalls commands sent on this connection.
    Raw mode (default) passes through, unaffected.
    Byte-oriented: backspace over a UTF-8 multibyte sequence deletes the whole chunk; cursor math is in bytes.
    """
    HIST_MAX = 100
    ESC_TIMEOUT_S = 0.5

    def __init__(self):
        self.buf = bytearray()
        self.cursor = 0
        self.hist = []        # Submitted full lines (newline excluded)
        self.hidx = 0         # Browse position (== len(hist) means editing current input)
        self.saved = b""      # Current input stashed while browsing history
        self.esc = b""        # Incomplete ESC sequence (wait for next packet)
        self.esc_ts = 0.0

    # ---------- display ----------
    def _redraw(self, out: bytearray):
        # \r + blanks to cover the old line + \r + reprint + cursor home (CR/blanks/BS only, no ANSI)
        width = len(self.buf) + 32
        if width < 64:
            width = 64
        out += b"\r" + b" " * width + b"\r" + bytes(self.buf)
        back = len(self.buf) - self.cursor
        if back > 0:
            out += b"\x08" * back

    def _bell(self, out: bytearray):
        out += b"\x07"

    # ---------- history ----------
    def _hist_prev(self, out):
        if not self.hist:
            self._bell(out)
            return
        if self.hidx == len(self.hist):
            self.saved = bytes(self.buf)
        if self.hidx == 0:
            self._bell(out)
            return
        self.hidx -= 1
        self.buf = bytearray(self.hist[self.hidx])
        self.cursor = len(self.buf)
        self._redraw(out)

    def _hist_next(self, out):
        if self.hidx >= len(self.hist):
            self._bell(out)
            return
        self.hidx += 1
        if self.hidx == len(self.hist):
            self.buf = bytearray(self.saved)
        else:
            self.buf = bytearray(self.hist[self.hidx])
        self.cursor = len(self.buf)
        self._redraw(out)

    def _leave_browse(self):
        if self.hidx != len(self.hist):
            self.hidx = len(self.hist)

    # ---------- editing ----------
    def _insert(self, out, ch: bytes):
        self._leave_browse()
        if self.cursor == len(self.buf):
            self.buf += ch
            self.cursor += len(ch)
            out += ch
        else:
            self.buf[self.cursor:self.cursor] = ch
            self.cursor += len(ch)
            self._redraw(out)

    def _backspace(self, out):
        self._leave_browse()
        if self.cursor == 0:
            self._bell(out)
            return
        cut = 1
        if self.cursor == len(self.buf):
            while cut < self.cursor and self.buf[self.cursor - cut] & 0xC0 == 0x80:
                cut += 1
            if cut > 1 and not 0xC2 <= self.buf[self.cursor - cut] <= 0xF4:
                cut = 1
        del self.buf[self.cursor - cut:self.cursor]
        self.cursor -= cut
        if cut == 1 and self.cursor == len(self.buf):
            out += b"\x08 \x08"
        else:
            self._redraw(out)

    def _delete_at(self, out):
        self._leave_browse()
        if self.cursor >= len(self.buf):
            self._bell(out)
            return
        # Byte-oriented cursor may sit inside multibyte text: delete the whole
        # codepoint starting at the cursor (mirrors _backspace; stray bytes delete one).
        first = self.buf[self.cursor]
        if 0xC2 <= first <= 0xF4:
            cut = 1
            while self.cursor + cut < len(self.buf) and self.buf[self.cursor + cut] & 0xC0 == 0x80:
                cut += 1
        else:
            cut = 1
        del self.buf[self.cursor:self.cursor + cut]
        self._redraw(out)

    def _del_word(self, out):
        self._leave_browse()
        end = self.cursor
        start = end
        while start > 0 and self.buf[start - 1:start] == b" ":
            start -= 1
        while start > 0 and self.buf[start - 1:start] != b" ":
            start -= 1
        if start == end:
            self._bell(out)
            return
        del self.buf[start:end]
        self.cursor = start
        self._redraw(out)

    def _move(self, out, delta):
        self._leave_browse()
        pos = self.cursor + delta
        if pos < 0:
            pos = 0
        if pos > len(self.buf):
            pos = len(self.buf)
        if pos == self.cursor:
            self._bell(out)
            return
        self.cursor = pos
        self._redraw(out)

    def _home(self, out):
        self._leave_browse()
        if self.cursor == 0:
            self._bell(out)
            return
        self.cursor = 0
        self._redraw(out)

    def _end(self, out):
        self._leave_browse()
        if self.cursor == len(self.buf):
            self._bell(out)
            return
        self.cursor = len(self.buf)
        self._redraw(out)

    def _reset_line(self):
        self.buf.clear()
        self.cursor = 0
        self.hidx = len(self.hist)
        self.saved = b""

    def _submit(self, out, dev):
        line = bytes(self.buf)
        out += b"\r\n"
        if line.strip(b" \t"):
            self.hist.append(line)
            if len(self.hist) > self.HIST_MAX:
                del self.hist[0]
        self.hidx = len(self.hist)
        self.saved = b""
        self.buf.clear()
        self.cursor = 0
        dev += line + LINE_ENDINGS.get(default_line_ending, b"\n")

    def _csi(self, out, dev, data: bytes, i: int, n: int, now: float):
        """Parse the sequence at data[i] == ESC; return the new i (None means wait for next packet)."""
        if n - i >= 3 and data[i + 1] == 0x5B:
            c = data[i + 2]
            two = data[i + 2:i + 4]
            if c == 0x41:
                self._hist_prev(out)
                return i + 3
            if c == 0x42:
                self._hist_next(out)
                return i + 3
            if c == 0x43:
                self._move(out, 1)
                return i + 3
            if c == 0x44:
                self._move(out, -1)
                return i + 3
            if c == 0x48:
                self._home(out)
                return i + 3
            if c == 0x46:
                self._end(out)
                return i + 3
            if two == b"3~":
                self._delete_at(out)
                return i + 4
            if two == b"1~":
                self._home(out)
                return i + 4
            if two == b"4~":
                self._end(out)
                return i + 4
            # Unknown CSI: look for the terminator (0x40-0x7E; if found it is complete, pass through as-is;
            # not found means truncated, wait for next packet)
            j = i + 2
            while j < n and 0x20 <= data[j] <= 0x3F:
                j += 1
            if j < n and 0x40 <= data[j] <= 0x7E:
                dev += data[i:j + 1]
                return j + 1
            if j == i + 2:
                # Not CSI chars at all after ESC[: the earlier stash was a false alarm, drop ESC[ and resume here
                return i + 2
            self.esc = data[i:]
            self.esc_ts = now
            return None
        if n - i == 1 or data[i + 1] == 0x5B:
            self.esc = data[i:]  # Truncated, wait for next packet
            self.esc_ts = now
            return None
        return i + 1  # Bare ESC (e.g. Alt combo): drop ESC, the main key is treated as a normal char next round

    def feed(self, data: bytes):
        """Consume one TCP input packet; returns (to_conn, to_device).
        to_conn goes straight back to the sender (local echo/redraw), to_device goes to the device whole."""
        now = time.monotonic()
        if self.esc:
            if now - self.esc_ts > self.ESC_TIMEOUT_S:
                self.esc = b""  # Drop stale incomplete sequences
            else:
                data = self.esc + data
                self.esc = b""
        out = bytearray()
        dev = bytearray()
        i, n = 0, len(data)
        while i < n:
            b = data[i]
            if b == 0x1B:
                ni = self._csi(out, dev, data, i, n, now)
                if ni is None:
                    break
                i = ni
                continue
            if b in (0x0D, 0x0A):  # Enter variants: CR / LF / CRLF / CR NUL all count once
                if b == 0x0D and i + 1 < n and data[i + 1] in (0x0A, 0x00):
                    i += 1
                self._submit(out, dev)
                i += 1
                continue
            if b == 0x00:
                i += 1  # Drop NUL
                continue
            if b == 0x03:  # Ctrl+C: interrupt the device + clear the line
                dev += b"\x03"
                self._reset_line()
                out += b"^C\r\n"
                i += 1
                continue
            if b == 0x1A:  # Ctrl+Z: pass through + clear the line
                dev += b"\x1a"
                self._reset_line()
                out += b"^Z\r\n"
                i += 1
                continue
            if b == 0x15:  # Ctrl+U: delete the whole line
                self._leave_browse()
                self.buf.clear()
                self.cursor = 0
                self._redraw(out)
                i += 1
                continue
            if b == 0x17:  # Ctrl+W: delete one word
                self._del_word(out)
                i += 1
                continue
            if b == 0x04:  # Ctrl+D: send EOT on an empty line, else delete the char at cursor
                if not self.buf:
                    dev += b"\x04"
                else:
                    self._delete_at(out)
                i += 1
                continue
            if b in (0x08, 0x7F):  # Backspace / DEL
                self._backspace(out)
                i += 1
                continue
            if b == 0x09:  # Tab: flush the current line along with the Tab (the device needs content to complete)
                self._leave_browse()
                dev += bytes(self.buf) + b"\x09"
                self.buf.clear()
                self.cursor = 0
                i += 1
                continue
            if b < 0x20:  # Other control chars: pass through
                dev += bytes((b,))
                i += 1
                continue
            self._insert(out, bytes((b,)))  # Printable (multibyte kept as-is)
            i += 1
        return bytes(out), bytes(dev)


def _wants_pushback(data: bytes) -> bool:
    """Whether user input should be pushed back to the sender for live display.

    Cooked never pushes (local echo already shows it); raw input with ESC never pushes (arrow-key
    sequences pushed back would be read as cursor moves, wrecking the device redraw; let the device echo/redraw show it).
    Other printable raw input is pushed back to the sender (typing stays visible).
    """
    return not cooked_mode and b"\x1b" not in data


@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(name="serial_connect", description="Connect to the serial port (shared by all AIs/users)",
             inputSchema={"type": "object",
                          "properties": {
                              "port": {"type": "string", "description": "Serial port, e.g. /dev/ttyUSB0, COM3"},
                              "baudrate": {"type": "integer", "description": "Baud rate", "default": 115200},
                              "bytesize": {"type": "integer", "description": "Data bits", "default": 8, "enum": [5, 6, 7, 8]},
                              "parity": {"type": "string", "description": "Parity", "default": "N", "enum": ["N", "E", "O"]},
                              "stopbits": {"type": "number", "description": "Stop bits", "default": 1, "enum": [1, 1.5, 2]},
                          },
                          "required": ["port"]}),
        Tool(name="serial_disconnect", description="Disconnect the serial port",
             inputSchema={"type": "object", "properties": {}}),
        Tool(name="serial_write", description="Write data to the serial port (visible to users and other AIs; broadcast to everyone)",
             inputSchema={"type": "object",
                          "properties": {
                              "data": {"type": "string", "description": "Data to write"},
                              "encoding": {"type": "string", "description": "Encoding", "default": "utf-8", "enum": ["utf-8", "ascii", "hex"]},
                              "append_crlf": {"type": "boolean", "description": "Append a newline automatically", "default": True},
                              "line_ending": {"type": "string", "description": "Newline style (defaults to server --line-ending, lf; use crlf for devices needing CRLF)", "enum": ["crlf", "lf", "cr"]},
                          },
                          "required": ["data"]}),
        Tool(name="serial_read", description="Read device responses from the unified receive buffer (same source as TCP broadcast/history; never races the reader thread)",
             inputSchema={"type": "object",
                          "properties": {"timeout_ms": {"type": "integer", "description": "Read timeout (ms)", "default": 200}}}),
        Tool(name="serial_readline", description="Read one line from the unified receive buffer (up to newline)",
             inputSchema={"type": "object",
                          "properties": {"timeout_ms": {"type": "integer", "description": "Read timeout (ms)", "default": 1500}}}),
        Tool(name="serial_write_read", description="Send a command and wait for the response (drains stale input first; for Q&A-style devices; consumes the buffer)",
             inputSchema={"type": "object",
                          "properties": {
                              "data": {"type": "string", "description": "Data to write"},
                              "encoding": {"type": "string", "description": "Encoding", "default": "utf-8", "enum": ["utf-8", "ascii", "hex"]},
                              "append_crlf": {"type": "boolean", "description": "Append a newline automatically", "default": True},
                              "line_ending": {"type": "string", "description": "Newline style (defaults to server --line-ending, lf; use crlf for devices needing CRLF)", "enum": ["crlf", "lf", "cr"]},
                              "wait_ms": {"type": "integer", "description": "Time to wait for a response after writing (ms)", "default": 500},
                          },
                          "required": ["data"]}),
        Tool(name="serial_status", description="Show serial port connection status",
             inputSchema={"type": "object", "properties": {}}),
        Tool(name="serial_list_ports", description="List all available serial ports",
             inputSchema={"type": "object", "properties": {}}),
        Tool(name="serial_get_history", description="Show recent serial history (user / AI / device activity)",
             inputSchema={"type": "object",
                          "properties": {"lines": {"type": "integer", "description": "Number of lines", "default": 50}}}),
        Tool(name="serial_search_history", description="Search history (filter by keyword/source)",
             inputSchema={"type": "object",
                          "properties": {
                              "keyword": {"type": "string", "description": "Keyword (substring match, empty means no filter)", "default": ""},
                              "source": {"type": "string", "description": "Source filter: AI / User / Device / SYS / all", "default": "all", "enum": ["all", "AI", "User", "Device", "SYS"]},
                              "lines": {"type": "integer", "description": "How many recent lines to search", "default": 200},
                              "limit": {"type": "integer", "description": "Max lines to return", "default": 50},
                          }}),
         Tool(name="serial_flush", description="Flush serial port buffers (input/output/both)",
              inputSchema={"type": "object",
                           "properties": {"which": {"type": "string", "default": "both", "enum": ["input", "output", "both"]}}}),
         Tool(name="serial_log_start", description="Start writing all input/output to a log file (shared switch with TCP !start_log)",
              inputSchema={"type": "object",
                           "properties": {"filename": {"type": "string", "description": "File name (auto-generated if empty); basename only", "default": ""}}}),
         Tool(name="serial_log_stop", description="Stop recording the log file",
              inputSchema={"type": "object", "properties": {}}),
     ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    arguments = arguments or {}
    if name == "serial_connect":
        ok = open_serial(arguments.get("port"), arguments.get("baudrate", 115200),
                         bytesize=arguments.get("bytesize", 8),
                         parity=arguments.get("parity", "N"),
                         stopbits=arguments.get("stopbits", 1))
        if ok:
            return _text(f"Connected to {arguments['port']} @ {arguments.get('baudrate', 115200)} (shared by all AIs/users)")
        return _text(f"Cannot connect to {arguments.get('port')}; check that it exists, permissions, and availability")

    elif name == "serial_disconnect":
        close_serial()
        return _text("Serial port disconnected")

    elif name == "serial_write":
        data_str = arguments.get("data", "")
        enc = arguments.get("encoding", "utf-8")
        data, err = _encode_write_data(data_str, enc, arguments.get("append_crlf", True),
                                       arguments.get("line_ending"))
        if err:
            return _text(err)
        ok = write_serial(data, source="AI")
        return _text(f"Written: {data_str}" if ok else "Write failed (serial not connected)")

    elif name == "serial_write_read":
        data_str = arguments.get("data", "")
        enc = arguments.get("encoding", "utf-8")
        wait_ms = arguments.get("wait_ms", 500)
        data, err = _encode_write_data(data_str, enc, arguments.get("append_crlf", True),
                                       arguments.get("line_ending"))
        if err:
            return _text(err)
        if not _is_serial_open():
            return _text("Serial not connected")
        # Q&A semantics: drop stale buffered reads from before the write (spontaneous output
        # in between goes too; for streaming use serial_write + serial_read).
        _rx_drain()
        ok = write_serial(data, source="AI")
        if not ok:
            return _text("Write failed (serial not connected)")
        # Atomic wait-and-take: judge and clear under one lock, no wait-then-drain race.
        blob = _rx_wait_and_drain(wait_ms / 1000.0)
        if blob:
            return _text(f"Response ({len(blob)} bytes):\n{blob.decode('utf-8', errors='replace')}")
        return _text("Written, but no response within the timeout")

    elif name == "serial_read":
        timeout_ms = arguments.get("timeout_ms", 200)
        if not _is_serial_open():
            return _text("Serial not connected")
        # Atomic wait-and-take (consuming): same source as TCP broadcast/history, never races the reader.
        data = _rx_wait_and_drain(timeout_ms / 1000.0)
        if data:
            return _text(f"Received ({len(data)} bytes):\n{data.decode('utf-8', errors='replace')}")
        return _text("No data (timeout)")

    elif name == "serial_readline":
        timeout_ms = arguments.get("timeout_ms", 1500)
        if not _is_serial_open():
            return _text("Serial not connected")
        # Atomic line take: judge + split + push back the rest under one lock; fragments on timeout are consumed, not wedged.
        line, has_newline = _rx_take_line(timeout_ms / 1000.0)
        if has_newline:
            return _text(f"Got one line: {line.decode('utf-8', errors='replace').strip()}")
        if line:
            return _text(f"Received (no newline, {len(line)} bytes): {line.decode('utf-8', errors='replace').strip()}")
        return _text("No complete line (timeout)")

    elif name == "serial_status":
        with serial_lock:
            conn = serial_conn
            if conn is not None and conn.is_open:
                try:
                    info = (conn.port, conn.baudrate, conn.bytesize,
                            conn.parity, conn.stopbits, conn.in_waiting)
                except Exception:
                    info = None
            else:
                info = None
        with tcp_clients_lock:
            n_tcp = len(tcp_clients)
        with rx_cond:
            n_rx_chunks = len(rx_buffer)
            n_rx_bytes = sum(len(c) for c in rx_buffer)
        hist_state = "disabled (--no-history)" if not enable_history else f"enabled ({len(_history_snapshot())} line(s) buffered)"
        tcp_hist_state = "off (--no-tcp-history)" if not enable_tcp_history else "on"
        edit_state = "cooked" if cooked_mode else "raw"
        with log_fp_lock:
            log_state = f"{log_name} ({log_owner})" if log_fp is not None else "not recording"
        if info is not None:
            port, baud, bytesize, parity, stopbits, in_waiting = info
            return _text(f"Serial status:\n  Port: {port}\n  Baud: {baud}\n"
                         f"  Bytesize: {bytesize} Parity: {parity} Stopbits: {stopbits}\n"
                         f"  In Waiting (hw): {in_waiting}\n"
                         f"  RX buffer: {n_rx_bytes} bytes / {n_rx_chunks} chunks\n"
                         f"  History: {hist_state}\n"
                         f"  TCP history replay: {tcp_hist_state}\n"
                         f"  Log: {log_state}\n"
                         f"  Line editing: {edit_state}\n"
                         f"  TCP users: {n_tcp}")
        return _text(f"Serial not connected\n  RX buffer: {n_rx_bytes} bytes / {n_rx_chunks} chunks\n"
                     f"  History: {hist_state}\n  TCP history replay: {tcp_hist_state}\n  Log: {log_state}\n"
                     f"  Line editing: {edit_state}\n  TCP users: {n_tcp}")

    elif name == "serial_list_ports":
        from serial.tools.list_ports import comports
        ports = list(comports())
        if ports:
            return _text("Available ports:\n" + "\n".join(f"  - {p.device}: {p.description}" for p in ports))
        return _text("No serial ports found")

    elif name == "serial_get_history":
        if not enable_history:
            return _text("History is disabled (started with --no-history)")
        lines = arguments.get("lines", 50)
        recent = _history_snapshot()[-lines:]
        if recent:
            return _text(f"Last {len(recent)} lines:\n" + "\n".join(recent))
        return _text("No history")

    elif name == "serial_search_history":
        if not enable_history:
            return _text("History is disabled (started with --no-history)")
        keyword = arguments.get("keyword", "") or ""
        source = arguments.get("source", "all")
        lines = arguments.get("lines", 200)
        limit = arguments.get("limit", 50)
        pool = _history_snapshot()[-lines:]
        out = []
        want = _SRC_TAG.get(source, source)
        for entry in pool:
            if source != "all" and want not in entry:
                continue
            if keyword and keyword not in entry:
                continue
            out.append(entry)
        out = out[-limit:]
        if out:
            return _text(f"{len(out)} hits (keyword={keyword!r}, source={source}):\n" + "\n".join(out))
        return _text(f"No hits (keyword={keyword!r}, source={source})")

    elif name == "serial_flush":
        which = arguments.get("which", "both")
        _rx_drain()  # Drain the unified receive buffer first
        with serial_lock:
            conn = serial_conn
            if not conn or not conn.is_open:
                return _text("RX buffer cleared (serial not connected, nothing to flush on hardware)")
            try:
                if which in ("input", "both"):
                    conn.reset_input_buffer()
                if which in ("output", "both"):
                    conn.reset_output_buffer()
            except Exception as e:
                return _text(f"Failed to flush hardware buffers: {e} (RX buffer already cleared)")
            return _text(f"Flushed {which} buffer(s)")

    elif name == "serial_log_start":
        msg = _logfile_start(arguments.get("filename", ""), "AI")
        _tcp_announce(f"[sys] AI: {msg}")
        return _text(msg)

    elif name == "serial_log_stop":
        msg = _logfile_stop("AI")
        _tcp_announce(f"[sys] AI: {msg}")
        return _text(msg)

    return _text(f"Unknown tool: {name}")


def run_stdio():
    """Serve MCP over the stdio transport (opencode spawns this program as a local MCP)."""
    import anyio

    async def _serve():
        async with stdio_server() as (read_stream, write_stream):
            await server.run(read_stream, write_stream, server.create_initialization_options())

    anyio.run(_serve)


def main():
    parser = argparse.ArgumentParser(description="Serial Broadcast Bridge MCP Server - human/AI shared COM port")
    parser.add_argument("--port", "-p", help="Serial port, e.g. COM3 or /dev/ttyUSB0")
    parser.add_argument("--baud", "-b", type=int, default=115200, help="Baud rate")
    parser.add_argument("--tcp", "-t", type=int, default=7001, help="User TCP/telnet port (default 7001)")
    parser.add_argument("--tcphost", default="127.0.0.1",
                        help="TCP bind IP (for users; default 127.0.0.1, localhost only. 0.0.0.0 exposes it to the LAN where anyone could inject serial commands)")
    parser.add_argument("--auto-connect", action="store_true", help="Auto-connect the serial port on startup")
    parser.add_argument("--no-history", action="store_true", help="Disable history: no recording, no replay for new TCP clients, serial_get_history reports disabled (live broadcast unaffected)")
    parser.add_argument("--no-tcp-history", action="store_true",
                        help="Skip history replay for new TCP clients; history is still recorded and AI serial_get_history/search keep working (live broadcast unaffected)")
    parser.add_argument("--log-file", default=None, help="Persist logs to a file (append), e.g. serial.log; stderr still gets a copy")
    parser.add_argument("--log-dir", default="logs",
                        help="Directory for files started via !start_log / serial_log_start (auto-created; names are basenamed, default logs)")
    parser.add_argument("--line-ending", default="lf", choices=["crlf", "lf", "cr"],
                        help="Newline character(s) to append (default lf; use crlf for devices that need CRLF)")
    parser.add_argument("--cooked", action="store_true",
                        help="Bridge-side line editing: local echo, Up/Down history recall (for dumb shells without line editing; default is raw passthrough)")
    args = parser.parse_args()

    global enable_history, enable_tcp_history, log_fp, log_owner, log_name, log_dir
    global default_line_ending, cooked_mode
    enable_history = not args.no_history
    enable_tcp_history = not args.no_tcp_history
    default_line_ending = args.line_ending
    cooked_mode = args.cooked
    log_dir = args.log_dir
    if args.log_file:
        try:
            log_fp = open(args.log_file, "a", encoding="utf-8")
            log_owner = "startup(--log-file)"
            log_name = args.log_file
        except Exception as e:
            print(f"[ERR] Cannot open log file {args.log_file}: {e}", file=sys.stderr)
            log_fp = None

    _emit("=" * 62)
    _emit(" Serial Broadcast Bridge MCP Server (stdio)")
    _emit(f"  Serial : {args.port or '(unset; use serial_connect)'} @ {args.baud}")
    _emit(f"  User   : telnet/raw {args.tcphost}:{args.tcp}")
    _emit("  Both sides share the connection; actions and device responses are broadcast to everyone")
    _emit(f"  History: {'disabled (--no-history, live broadcast unaffected)' if args.no_history else 'enabled'}")
    _emit(f"  TCP history replay: {'off (--no-tcp-history, new connections get no backlog; AI can still query)' if args.no_tcp_history else 'on'}")
    _emit(f"  Line ending: {args.line_ending} {LINE_ENDINGS[args.line_ending]!r}")
    _emit(f"  Line editing: {'cooked (local echo + Up/Down history)' if args.cooked else 'raw (passthrough)'}")
    _emit(f"  Log file : {args.log_file or '(off; use --log-file, or !start_log / serial_log_start after connecting)'}")
    _emit(f"  Log dir  : {args.log_dir} (dynamic recordings go here)")
    _emit("=" * 62)

    global running
    running = True

    if args.auto_connect and args.port:
        open_serial(args.port, args.baud)

    threading.Thread(target=serial_reader, daemon=True).start()
    threading.Thread(target=tcp_bridge, args=(args.tcphost, args.tcp), daemon=True).start()

    try:
        run_stdio()
    except KeyboardInterrupt:
        running = False
        log("[SYS] Got Ctrl+C, shutting down...")
    finally:
        running = False
        close_serial()
        log("[SYS] Closed")
        if log_fp is not None:
            try:
                with log_fp_lock:
                    log_fp.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
