"""serial_mcp_bridge 純邏輯單元測試（免硬體、免 mcp/pyserial 實裝，用 stub 載入）。"""
import importlib.util
import sys
import types
from pathlib import Path


def _load_bridge():
    # 先塞 stub，避免 import serial / mcp 失敗
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


def test_encode_utf8_lf():
    bridge.default_line_ending = "lf"
    data, err = bridge._encode_write_data("ls", "utf-8", True, None)
    assert err == "" and data == b"ls\n", data


def test_encode_hex_append_crlf():
    # P0-1 回歸：hex + append_crlf=True 必須加換行（之前直接 return 忽略）
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
    # 完整 echo + 真輸出黏在同一包，只扣前綴
    out = bridge._pending_consume(b"ls\nOK\n")
    assert out == b"OK\n", out


def test_echo_split_packets():
    bridge._pending_clear()
    bridge._pending_push(b"hello\n")
    assert bridge._pending_consume(b"hel") == b""
    assert bridge._pending_consume(b"lo\nworld") == b"world"


def test_echo_mismatch_dropped():
    # 設備改寫/遺失 echo（開頭對不上）時丟 pending、不丟 data
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
    bridge.cooked_mode = True
    assert bridge._wants_pushback(b"ls") is False
    bridge.cooked_mode = False  # 還原預設


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
