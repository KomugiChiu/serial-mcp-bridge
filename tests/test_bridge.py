"""Pure-logic unit tests for serial_mcp_bridge (no hardware; stub out mcp/pyserial)."""
import importlib.util
import multiprocessing
import os
import sys
import threading
import types
from pathlib import Path

import pytest


def _load_bridge():
    # Stub these first so importing serial / mcp cannot fail
    if "serial" not in sys.modules:
        serial_stub = types.ModuleType("serial")
        serial_stub.Serial = object  # type: ignore
        sys.modules["serial"] = serial_stub
    for name in ("mcp", "mcp.server", "mcp.server.stdio", "mcp.types"):
        if name not in sys.modules:
            sys.modules[name] = types.ModuleType(name)
    sys.modules["mcp.server"].Server = type(
        "Server",
        (),
        {
            "__init__": lambda self, *a, **k: None,
            "list_tools": lambda self, *a, **k: (lambda fn: fn),
            "call_tool": lambda self, *a, **k: (lambda fn: fn),
        },
    )  # type: ignore
    sys.modules["mcp.server.stdio"].stdio_server = object  # type: ignore
    sys.modules["mcp.types"].TextContent = object  # type: ignore
    sys.modules["mcp.types"].Tool = object  # type: ignore

    root = Path(__file__).resolve().parents[1]
    bridge_path = root / "serial_mcp_bridge.py"
    spec = importlib.util.spec_from_file_location("bridge_under_test", bridge_path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["bridge_under_test"] = mod
    spec.loader.exec_module(mod)
    return mod


bridge = _load_bridge()


@pytest.fixture(autouse=True)
def _bridge_globals():
    """Save/restore mutable bridge globals so one failing test cannot pollute the next."""
    saved = (bridge.default_line_ending, bridge.cooked_mode, bridge.enable_history,
             bridge.enable_tcp_history, bridge.log_dir)
    yield
    (bridge.default_line_ending, bridge.cooked_mode, bridge.enable_history,
     bridge.enable_tcp_history, bridge.log_dir) = saved
    bridge._pending_clear()
    _reset_log_state()


def test_encode_utf8_lf():
    bridge.default_line_ending = "lf"
    data, err = bridge._encode_write_data("ls", "utf-8", True, None)
    assert err == "" and data == b"ls\n", data


def test_encode_hex_append_crlf():
    # P0-1 regression: hex + append_crlf=True must append a newline (it used to return early, ignoring it)
    bridge.default_line_ending = "lf"
    data, err = bridge._encode_write_data("41 42", "hex", True, None)
    assert err == "" and data == b"AB\n", data
    data2, err2 = bridge._encode_write_data("41 42", "hex", False, None)
    assert err2 == "" and data2 == b"AB", data2


def test_encode_hex_invalid():
    data, err = bridge._encode_write_data("zz", "hex", True, None)
    assert data == b"" and err


def test_echo_exact_and_cross_chunk():
    bridge._pending_clear()
    bridge._pending_push(b"ls\n")
    # Full echo + real output glued in one packet: only the prefix is stripped
    out = bridge._pending_consume(b"ls\nOK\n")
    assert out == b"OK\n", out


def test_echo_split_packets():
    bridge._pending_clear()
    bridge._pending_push(b"hello\n")
    assert bridge._pending_consume(b"hel") == b""
    assert bridge._pending_consume(b"lo\nworld") == b"world"


def test_echo_mismatch_dropped():
    # Rewritten/lost echo (mismatched head) drops pending, never data
    bridge._pending_clear()
    bridge._pending_push(b"ABC")
    assert bridge._pending_consume(b"XYZ") == b"XYZ"


def test_pending_clear_drains_rx():
    bridge._pending_clear()
    bridge._rx_push(b"stale")
    bridge._pending_push(b"stale-echo")
    bridge._pending_clear()
    assert bridge._rx_drain() == b""
    assert bridge._pending_consume(b"stale-echo") == b"stale-echo"


def test_cooked_typing_enter():
    bridge.default_line_ending = "lf"
    ed = bridge.CookedLine()
    out, dev = ed.feed(b"ls")
    assert out == b"ls" and dev == b""
    out, dev = ed.feed(b"\r")
    assert dev == b"ls\n", dev


def test_cooked_backspace_utf8():
    bridge.default_line_ending = "lf"
    ed = bridge.CookedLine()
    ed.feed("中".encode())
    assert bytes(ed.buf) == "中".encode()
    ed.feed(b"\x7f")
    assert bytes(ed.buf) == b""


def test_cooked_history_recall():
    bridge.default_line_ending = "lf"
    ed = bridge.CookedLine()
    ed.feed(b"first\r")
    ed.feed(b"second\r")
    out, _ = ed.feed(b"\x1b[A")  # Up -> second
    assert bytes(ed.buf) == b"second"
    ed.feed(b"\x1b[A")  # Up -> first
    assert bytes(ed.buf) == b"first"
    ed.feed(b"\x1b[B")  # Down -> second
    assert bytes(ed.buf) == b"second"


def test_cooked_ctrl_c_clears():
    bridge.default_line_ending = "lf"
    ed = bridge.CookedLine()
    ed.feed(b"abc")
    out, dev = ed.feed(b"\x03")
    assert dev == b"\x03" and bytes(ed.buf) == b"" and b"^C" in out


def test_wants_pushback_esc():
    bridge.cooked_mode = False
    assert bridge._wants_pushback(b"ls") is True
    assert bridge._wants_pushback(b"\x1b[A") is False
    assert bridge._wants_pushback(b"\x09") is False  # Tab: device owns completion
    assert bridge._wants_pushback(b"\x03") is False  # Ctrl-C: device owns handling
    assert bridge._wants_pushback(b"ls\n") is True  # newline still echoed
    bridge.cooked_mode = True
    assert bridge._wants_pushback(b"ls") is False
    bridge.cooked_mode = False  # restore default


def test_pushback_echo_filters_tab():
    # Tab completion regression: "ls te" + TAB must not echo the TAB back
    # (else the terminal shows a gap plus the "st" suffix: "ls te    st").
    bridge.cooked_mode = False
    assert bridge._pushback_echo(b"\x09") == b""
    assert bridge._pushback_echo(b"ls te") == b"ls te"
    assert bridge._pushback_echo(b"ab\x09cd\n") == b"abcd\n"  # only TAB suppressed
    assert bridge._pushback_echo(b"\x1b[A") == b""  # ESC sequences: nothing
    bridge.cooked_mode = False


def test_cmd_filter_tab_no_echo():
    # End-to-end raw path: TAB is forwarded to the device but never echoed.
    bridge.cooked_mode = False
    f = bridge.TcpCommandFilter()
    for ch in b"ls te":
        dev, echo, consumed = f.feed(bytes((ch,)), lambda line: line)
        assert (dev, echo) == (bytes((ch,)), bytes((ch,)))
    dev, echo, consumed = f.feed(b"\x09", lambda line: line)
    assert dev == b"\x09" and echo == b"" and consumed is False
    # Pasted input with TAB: device gets everything, sender sees text minus TAB.
    f2 = bridge.TcpCommandFilter()
    dev, echo, consumed = f2.feed(b"ab\x09cd\n", lambda line: line)
    assert dev == b"ab\x09cd\n" and echo == b"abcd\n"
    bridge.cooked_mode = False


def test_normalize_user_input_lf():
    bridge.default_line_ending = "lf"
    assert bridge._normalize_user_input(b"a\r\nb\rc") == b"a\nb\nc"


def test_wait_and_drain_atomic():
    bridge._pending_clear()
    bridge._rx_push(b"a")
    bridge._rx_push(b"b")
    assert bridge._rx_wait_and_drain(0.05) == b"ab"
    assert bridge._rx_drain() == b""
    assert bridge._rx_wait_and_drain(0.01) == b""


def test_take_line_split_and_leftover():
    bridge._pending_clear()
    bridge._rx_push(b"one\ntwo\n")
    line, has_nl = bridge._rx_take_line(0.05)
    assert (line, has_nl) == (b"one\n", True)
    line, has_nl = bridge._rx_take_line(0.05)
    assert (line, has_nl) == (b"two\n", True)
    assert bridge._rx_drain() == b""


def test_take_line_partial_timeout_consumes():
    bridge._pending_clear()
    bridge._rx_push(b"frag")
    line, has_nl = bridge._rx_take_line(0.01)
    assert (line, has_nl) == (b"frag", False)
    assert bridge._rx_drain() == b""


def test_take_line_empty_timeout():
    bridge._pending_clear()
    assert bridge._rx_take_line(0.01) == (b"", False)


def test_tcp_history_gate():
    # Default: replay when a snapshot exists; --no-tcp-history only disables TCP replay,
    # AI queries keep working (the history itself is untouched)
    bridge.enable_history = True
    bridge.enable_tcp_history = True
    assert bridge._should_send_tcp_history(["a"]) is True
    assert bridge._should_send_tcp_history([]) is False
    bridge.enable_tcp_history = False
    assert bridge._should_send_tcp_history(["a"]) is False
    bridge.enable_history = False
    bridge.enable_tcp_history = True
    assert bridge._should_send_tcp_history(["a"]) is False
    bridge.enable_history = True  # restore default


def test_sanitize_log_name():
    assert bridge._sanitize_log_name("a.log") == "a.log"
    assert bridge._sanitize_log_name("../../etc/passwd") == "passwd"  # traversal -> basename only
    assert bridge._sanitize_log_name("a/b") == "b"
    assert bridge._sanitize_log_name("a b?.log") == "a_b_.log"
    for bad in ("", "  ", ".", "..", "../"):
        assert bridge._sanitize_log_name(bad) == "", bad


def test_parse_log_command():
    assert bridge._parse_log_command("!start_log") == ("start", "")
    assert bridge._parse_log_command("!start_log foo.log") == ("start", "foo.log")
    assert bridge._parse_log_command("!START") == ("start", "")
    assert bridge._parse_log_command("!stop_log") == ("stop", "")
    assert bridge._parse_log_command("!log_status") == ("status", "")
    assert bridge._parse_log_command("!help") == ("help", "")
    assert bridge._parse_log_command("!!ls") == ("escape", "!ls")
    assert bridge._parse_log_command("!nope") == ("unknown", "!nope")
    assert bridge._parse_log_command("!") == ("unknown", "!")
    assert bridge._parse_log_command("ls") == ("unknown", "ls")


def _reset_log_state():
    try:
        if bridge.log_fp is not None:
            bridge.log_fp.close()
    except Exception:
        pass
    bridge.log_fp = None
    bridge.log_owner = None
    bridge.log_name = None


def test_logfile_start_stop(tmp_path):
    old_dir = bridge.log_dir
    bridge.log_dir = str(tmp_path)
    try:
        _reset_log_state()
        assert bridge._logfile_start("t.log", "TEST").startswith("Recording started")
        assert bridge._logfile_status().startswith("Recording:")
        assert "Already recording" in bridge._logfile_start("other.log", "TEST")  # second open is refused
        files = list(tmp_path.iterdir())
        assert len(files) == 1 and files[0].name == "t.log"
        assert "Recording started" in files[0].read_text(encoding="utf-8")
        assert bridge._logfile_stop("TEST").startswith("Recording stopped")
        assert bridge._logfile_status() == "Not recording"
        assert bridge._logfile_stop("TEST") == "Not recording"  # stopping while idle is safe
        auto = bridge._logfile_start("", "TEST")  # omitted name is auto-generated
        assert auto.startswith("Recording started") and bridge.log_name.startswith("serial-")
    finally:
        _reset_log_state()
        bridge.log_dir = old_dir


class _FakeConn:
    def __init__(self):
        self.sent = bytearray()

    def sendall(self, data):
        self.sent += data


def _real_processor():
    return bridge._make_command_processor(_FakeConn(), ("127.0.0.1", 1))


def test_command_processor_escape_and_unknown():
    proc = _real_processor()
    assert proc(b"!!ls\n") == b"!ls\n"  # escape goes to the device as-is
    assert proc(b"!nope\n") == b"!nope\n"  # unknown commands are not swallowed; forwarded


def test_command_processor_status(tmp_path):
    old_dir = bridge.log_dir
    bridge.log_dir = str(tmp_path)
    try:
        _reset_log_state()
        conn = _FakeConn()
        proc = bridge._make_command_processor(conn, ("127.0.0.1", 1))
        assert proc(b"!log_status\n") == b""
        assert "Not recording" in bytes(conn.sent).decode()
        assert proc(b"!start_log t.log\n") == b""
        assert proc(b"!log_status\n") == b""
        assert "t.log" in bytes(conn.sent).decode()
        assert proc(b"!stop_log\n") == b""
        assert "Recording stopped" in bytes(conn.sent).decode()
    finally:
        _reset_log_state()
        bridge.log_dir = old_dir


def test_cmd_filter_passthrough():
    bridge.cooked_mode = False
    f = bridge.TcpCommandFilter()
    dev, echo, consumed = f.feed(b"ls\n", lambda line: b"")
    assert (dev, echo, consumed) == (b"ls\n", b"ls\n", False)
    dev, echo, consumed = f.feed(b"ls !foo\n", lambda line: b"")  # mid-line ! never triggers
    assert (dev, echo, consumed) == (b"ls !foo\n", b"ls !foo\n", False)
    bridge.cooked_mode = False  # restore default


def test_cmd_filter_char_at_a_time():
    bridge.cooked_mode = False
    f = bridge.TcpCommandFilter()
    seen = []
    dev_total, echo_total = b"", b""
    for ch in b"!log_status\n":
        dev, echo, consumed = f.feed(bytes((ch,)), lambda line: seen.append(line) or b"")
        dev_total += dev
        echo_total += echo
    assert seen == [b"!log_status\n"]  # char-at-a-time still assembles a full line
    assert echo_total == b"!log_status"  # held input echoes char-by-char; the terminating \n is consumed, reply follows
    assert dev_total == b"" and consumed is True
    bridge.cooked_mode = False


def test_cmd_filter_backspace_abandons():
    bridge.cooked_mode = False
    f = bridge.TcpCommandFilter()
    dev, echo, consumed = f.feed(b"!star", lambda line: b"")
    assert dev == b"" and echo == b"!star"  # held so far
    dev, echo, consumed = f.feed(b"\x7fmore\n", lambda line: b"")
    assert dev == b"!star\x7fmore\n"  # backspace abandons holding; everything incl. the erase goes to the device
    assert consumed is False
    bridge.cooked_mode = False


def test_filter_cooked_commands():
    got = []
    dev, consumed = bridge._filter_cooked_commands(b"!log_status\nls\n", lambda line: got.append(line) or b"")
    assert got == [b"!log_status\n"] and (dev, consumed) == (b"ls\n", True)
    dev, consumed = bridge._filter_cooked_commands(b"!sta", lambda line: b"")  # no newline, no judgement
    assert (dev, consumed) == (b"!sta", False)


def test_write_serial_offline_logged():
    # Offline input must still leave a trace (monitoring without gaps), tagged (not sent)
    bridge.enable_history = True
    assert bridge.write_serial(b"hi\n", source="TEST") is False
    assert any("(not sent)" in e and "hi" in e for e in bridge._history_snapshot()[-5:])


def test_short_source():
    assert bridge._short_source("AI -> Device") == "[_ai]"
    assert bridge._short_source("User(('127.0.0.1', 1)) -> Device") == "[usr]"
    assert bridge._short_source("User(('127.0.0.1', 1)) -> Device (not sent)") == "[usr]"
    assert bridge._short_source("Device -> ALL") == "[dev]"


def test_broadcast_tags_and_sys_normalized():
    bridge.enable_history = True
    bridge._pending_clear()
    bridge.broadcast_text(b"hello\n", show_as="AI -> Device", to_tcp=False)
    assert "[_ai] hello" in bridge._history_snapshot()[-1]
    bridge.broadcast_text(b"oops\n", show_as="User(('127.0.0.1', 5)) -> Device (not sent)", to_tcp=False)
    assert "[usr] oops (not sent)" in bridge._history_snapshot()[-1]
    bridge.log("[SYS] hi")
    assert bridge._history_snapshot()[-1].endswith("[sys] hi")


def test_clean_log_text():
    # Emoji/special symbols are stripped; CJK, punctuation, $, and spacing survive
    assert bridge._clean_log_text("✅ Recording started: a.log") == "Recording started: a.log"
    assert bridge._clean_log_text("📝 Recording: x") == "Recording: x"
    assert bridge._clean_log_text("❌ Write failed") == "Write failed"
    assert bridge._clean_log_text("中文測試：，。「」") == "中文測試：，。「」"  # CJK fixture: device output may contain it
    assert bridge._clean_log_text("console:/ $ ") == "console:/ $"
    assert bridge._clean_log_text("a  b   c") == "a  b   c"
    assert bridge._clean_log_text("^C `ls` ok") == "^C `ls` ok"  # ASCII kept as-is


def test_cmd_filter_many_lines_no_recursion():
    # A single packet full of short ! lines must not grow the call stack (used to recurse per line).
    bridge.cooked_mode = False
    f = bridge.TcpCommandFilter()
    dev, echo, consumed = f.feed(b"!log_status\n" * 2000, lambda line: b"")
    assert dev == b"" and consumed is True


def test_cmd_filter_many_lines_mixed():
    # Consumed commands and forwarded lines can interleave in one packet without recursion.
    bridge.cooked_mode = False
    f = bridge.TcpCommandFilter()
    dev, echo, consumed = f.feed(
        b"!log_status\nls\n!log_status\n", lambda line: b"" if line.startswith(b"!") else line)
    assert dev == b"ls\n" and consumed is True


def test_delete_at_multibyte():
    # Delete-at-cursor removes the whole codepoint, mirroring _backspace.
    bridge.default_line_ending = "lf"
    ed = bridge.CookedLine()
    ed.feed("中".encode())
    ed._home(bytearray())
    ed._delete_at(bytearray())
    assert bytes(ed.buf) == b""
    ed.buf = bytearray("a中b".encode())
    ed.cursor = 1
    ed._delete_at(bytearray())
    assert bytes(ed.buf) == "ab".encode()


def test_emit_concurrent_stop_safe(tmp_path):
    # Stopping the log while another thread emits must not raise (write-to-closed is swallowed).
    bridge.log_dir = str(tmp_path)
    bridge._logfile_start("t.log", "TEST")
    errors = []

    def stopper():
        try:
            bridge._logfile_stop("TEST")
        except Exception as e:  # pragma: no cover - must never happen
            errors.append(e)

    t = threading.Thread(target=stopper)
    t.start()
    for i in range(200):
        bridge._emit(f"line {i}")
    t.join()
    assert not errors
    bridge._emit("after stop")  # still works with no log file open


class _BadConn:
    def sendall(self, data):
        raise OSError("broken pipe")


def test_broadcast_dead_client_removed():
    # Copy-then-send: a dead TCP client is dropped, the live one still gets the data.
    bridge.enable_history = True
    good = _FakeConn()
    bridge.tcp_clients.append(good)
    bridge.tcp_clients.append(_BadConn())
    try:
        bridge._pending_clear()
        bridge.broadcast_text(b"hi\n", show_as="Device -> ALL", to_tcp=True)
        assert b"hi\r\n" in bytes(good.sent)  # telnet needs CRLF (was bare LF before fix)
        assert all(isinstance(c, _FakeConn) for c in bridge.tcp_clients)
    finally:
        bridge.tcp_clients.clear()
        bridge._rx_drain()


def test_to_tcp_lineending():
    # Bare LF -> CRLF for telnet; existing CRLF untouched; lone CR preserved
    assert bridge._to_tcp_lineending(b"a\nb\n") == b"a\r\nb\r\n"
    assert bridge._to_tcp_lineending(b"a\r\nb\r\n") == b"a\r\nb\r\n"
    assert bridge._to_tcp_lineending(b"a\rb") == b"a\rb"  # lone CR: cursor-control / cooked redraw
    assert bridge._to_tcp_lineending(b"\r" + b" " * 4 + b"\r" + b"ls") == b"\r" + b" " * 4 + b"\r" + b"ls"
    assert bridge._to_tcp_lineending(b"mixed\r\nlf\ncr\rmix") == b"mixed\r\nlf\r\ncr\rmix"
    assert bridge._to_tcp_lineending(b"") == b""
    assert bridge._to_tcp_lineending(b"no newline") == b"no newline"


def test_broadcast_to_tcp_uses_crlf():
    # AI write (LF-only) and LF-only device output must reach TCP as CRLF
    bridge.enable_history = True
    good = _FakeConn()
    bridge.tcp_clients.append(good)
    try:
        bridge._pending_clear()
        bridge.broadcast_text(b"ls\n", show_as="AI -> Device", to_tcp=True)
        assert bytes(good.sent).endswith(b"ls\r\n")
        good.sent.clear()
        bridge._pending_clear()
        bridge.broadcast_text(b"out\nline2\n", show_as="Device -> ALL", to_tcp=True)
        assert b"out\r\nline2\r\n" in bytes(good.sent)
        # Existing CRLF device output is not doubled
        good.sent.clear()
        bridge._pending_clear()
        bridge.broadcast_text(b"ok\r\n", show_as="Device -> ALL", to_tcp=True)
        assert b"ok\r\n" in bytes(good.sent) and b"\r\r\n" not in bytes(good.sent)
    finally:
        bridge.tcp_clients.clear()
        bridge._rx_drain()


def test_tcp_send_text_crlf():
    conn = _FakeConn()
    bridge._tcp_send_text(conn, "hello\nworld\n")
    assert bytes(conn.sent) == b"hello\r\nworld\r\n"
    conn2 = _FakeConn()
    bridge._tcp_send(conn2, b"a\n")
    assert bytes(conn2.sent) == b"a\r\n"


# ---------- single-instance guard ----------

def _require_flock():
    if not bridge._HAS_FCNTL:
        pytest.skip("flock not available on this platform")


def test_lock_names_sanitized(tmp_path):
    assert bridge._lock_path("tcp-127.0.0.1-7001", str(tmp_path)).endswith(
        "serial-mcp-bridge-tcp-127.0.0.1-7001.lock")
    assert bridge._lock_path("serial-/dev/ttyUSB0", str(tmp_path)).endswith(
        "serial-mcp-bridge-serial-_dev_ttyUSB0.lock")
    assert bridge._tcp_lock_name("127.0.0.1", 7001) == "tcp-127.0.0.1-7001"
    assert bridge._serial_lock_name("/dev/ttyUSB0") == "serial-/dev/ttyUSB0"


def test_lock_acquire_release(tmp_path):
    _require_flock()
    name = "ut-basic"
    try:
        ok, _ = bridge._acquire_lock(name, str(tmp_path))
        assert ok is True
        ok2, _ = bridge._acquire_lock(name, str(tmp_path))  # same process: re-entrant
        assert ok2 is True
        assert (tmp_path / f"serial-mcp-bridge-{name}.lock").read_text().strip() == str(os.getpid())
    finally:
        bridge._release_lock(name)
    assert name not in bridge._instance_locks
    ok3, _ = bridge._acquire_lock(name, str(tmp_path))  # re-acquirable after release
    assert ok3 is True
    bridge._release_lock(name)


def test_lock_owner_alive_basics():
    assert bridge._lock_owner_alive(os.getpid()) is False
    assert bridge._lock_owner_alive(0) is False
    assert bridge._lock_owner_alive(-5) is False
    assert bridge._lock_owner_alive(2 ** 30) is False  # dead pid
    assert bridge._read_lock_pid("/nonexistent-path-xyz") == -1


def test_lock_conflict_live_holder(tmp_path, monkeypatch):
    # Another live bridge holds the lock: refuse loudly with its pid.
    _require_flock()
    import fcntl as _fcntl
    name = "ut-conflict"
    path = bridge._lock_path(name, str(tmp_path))
    holder = open(path, "w")
    holder.write("123456")
    holder.flush()
    _fcntl.flock(holder.fileno(), _fcntl.LOCK_EX | _fcntl.LOCK_NB)
    monkeypatch.setattr(bridge, "_lock_owner_alive", lambda pid: pid == 123456)
    try:
        ok, msg = bridge._acquire_lock(name, str(tmp_path))
        assert ok is False and "123456" in msg and "already running" in msg
        assert name not in bridge._instance_locks
    finally:
        monkeypatch.undo()
        try:
            _fcntl.flock(holder.fileno(), _fcntl.LOCK_UN)
        except Exception:
            pass
        holder.close()


def test_lock_stale_takeover(tmp_path):
    # Dead pid in the lock file: take over and overwrite with our pid.
    _require_flock()
    name = "ut-stale"
    path = bridge._lock_path(name, str(tmp_path))
    path = Path(path)
    path.write_text("1073741824")  # 2**30: no such process
    try:
        ok, _ = bridge._acquire_lock(name, str(tmp_path))
        assert ok is True
        assert path.read_text().strip() == str(os.getpid())
    finally:
        bridge._release_lock(name)


def _child_hold_lock(lock_dir, name, ready):
    import time as _time
    ok, _ = bridge._acquire_lock(name, lock_dir)
    ready.value = 1 if ok else -1
    _time.sleep(10)


def test_lock_cross_process_conflict(tmp_path):
    # A real second process holding the lock blocks us; after it dies we take over.
    _require_flock()
    from multiprocessing import Value
    name = "ut-xproc"
    ready = Value("i", 0)
    child = multiprocessing.Process(target=_child_hold_lock,
                                    args=(str(tmp_path), name, ready))
    try:
        child.start()
        for _ in range(100):
            if ready.value != 0:
                break
            child.join(timeout=0.1)
        assert ready.value == 1, "child failed to acquire the lock"
        ok, _ = bridge._acquire_lock(name, str(tmp_path))
        assert ok is False  # blocked by the live child
    finally:
        child.terminate()
        child.join(timeout=5)
    ok, _ = bridge._acquire_lock(name, str(tmp_path))  # stale pid file: take over
    assert ok is True
    bridge._release_lock(name)


def test_open_serial_refused_by_other_bridge(tmp_path, monkeypatch):
    # Second bridge on the same tty must refuse before touching pyserial.
    _require_flock()
    import fcntl as _fcntl
    port = "/dev/ttyUT9"
    monkeypatch.setenv("SERIAL_MCP_BRIDGE_LOCK_DIR", str(tmp_path))
    holder = open(bridge._lock_path(bridge._serial_lock_name(port), str(tmp_path)), "w")
    holder.write("123456")
    holder.flush()
    _fcntl.flock(holder.fileno(), _fcntl.LOCK_EX | _fcntl.LOCK_NB)
    monkeypatch.setattr(bridge, "_lock_owner_alive", lambda pid: pid == 123456)
    calls = []
    real_serial = bridge.serial

    class _NoOpen:
        def __init__(self, *a, **k):
            calls.append((a, k))
            raise AssertionError("pyserial must not be touched on lock refusal")

    monkeypatch.setattr(bridge, "serial", types.SimpleNamespace(Serial=_NoOpen))
    old_err = bridge._last_open_error
    try:
        assert bridge.open_serial(port, 115200) is False
        assert calls == []
        assert "already running" in bridge._last_open_error
    finally:
        monkeypatch.setattr(bridge, "serial", real_serial)
        bridge._last_open_error = old_err
        monkeypatch.undo()
        try:
            _fcntl.flock(holder.fileno(), _fcntl.LOCK_UN)
        except Exception:
            pass
        holder.close()


def test_bind_socket_conflict():
    srv = bridge._bind_tcp_socket("127.0.0.1", 0)
    try:
        port = srv.getsockname()[1]
        with pytest.raises(OSError):
            bridge._bind_tcp_socket("127.0.0.1", port)
    finally:
        srv.close()


def test_sigterm_handler_stops():
    old_running = bridge.running
    bridge.running = True
    try:
        with pytest.raises(SystemExit):
            bridge._handle_sigterm(bridge.signal.SIGTERM, None)
        assert bridge.running is False
    finally:
        bridge.running = old_running
