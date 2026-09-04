#!/usr/bin/env python3
"""
Serial Broadcast Bridge MCP Server
==================================
讓 AI Agent 與 User 同時連接同一個 Serial Port，互看對方操作、Device 回應廣播給所有人。

Transport:
  - MCP (stdio) :  AI agent（例如 opencode）以 local MCP 啟動本程式，經 stdio 交談
  - TCP text    :  User 用 PuTTY / MobaXterm 連 telnet/raw port 看待辦輸出並輸入

Usage:
  python serial_mcp_bridge.py --port /dev/ttyUSB0 --baud 115200
  python serial_mcp_bridge.py --port COM3 --baud 9600 --tcp 7001
  python serial_mcp_bridge.py --port /dev/ttyUSB0 --baud 115200 --auto-connect

  opencode local MCP 設定會 spawn 本程式（stdio），並在背景同時開啟 TCP bridge 給 User。

Dependencies:
  pip install "mcp<2" pyserial
"""
import argparse
import socket
import threading
import time
import sys
from collections import deque
from datetime import datetime
from typing import Optional

try:
    import serial
except ImportError:
    print("[ERROR] 需要 pyserial: pip install pyserial")
    sys.exit(1)

try:
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    from mcp.types import TextContent, Tool
except ImportError:
    print("[ERROR] 需要 mcp (1.x): pip install \"mcp<2\"")
    sys.exit(1)

# ============================================================
# 全域狀態（共用同一個 serial port）
# ============================================================
serial_conn: Optional[serial.Serial] = None
serial_lock = threading.Lock()
history = deque(maxlen=1000)          # 歷史紀錄，給新連線者看（可用 --no-history 停用）
history_lock = threading.Lock()     # 保護 history 的併發存取
enable_history = True               # 是否紀錄歷史，由啟動參數 --no-history 控制
# 統一接收緩衝：只有 serial_reader 線程讀硬體，MCP 的 read/readline/write_read 都從這裡取
rx_buffer: deque = deque()          # 每個元素是一段 bytes（Device 去 echo 後的資料）
rx_lock = threading.Lock()
rx_cond = threading.Condition(rx_lock)
tcp_clients = []                      # User 的 TCP 連線清單
tcp_clients_lock = threading.Lock()
pending_chunks: deque = deque()  # [(bytes, monotonic_ts)] 剛送出、等設備 echo 比對用
echo_lock = threading.Lock()
ECHO_EXPIRE_S = 1.0             # 超過此秒數還沒回顯，視為遺失（避免陳舊資料毒化後續去重）
ECHO_MAX_BYTES = 4096         # pending 上限，超過丟最舊（115200 下 512 約 40ms 就滿，放大避免高吞吐 echo 洩漏）
log_fp = None                       # --log-file 開啟的檔案 handle（持久化）
log_fp_lock = threading.Lock()
# 送出的換行字元：有些設備（如 Android shell，走 ICRNL 把 CR 轉 LF）收到 CRLF
# 會變成兩個換行 = 多執行一次空指令 = prompt 成雙。可用 --line-ending 改成 lf。
LINE_ENDINGS = {"crlf": b"\r\n", "lf": b"\n", "cr": b"\r", "none": b""}
default_line_ending = "lf"          # 由啟動參數 --line-ending 覆寫
cooked_mode = False               # 預設 raw 直通；--cooked 啟用 bridge 端行編輯（本地 echo + Up/Down 歷史）
running = True


def _emit(line: str):
    """寫到 stderr，同時 tee 到 --log-file（若有）。"""
    print(line, file=sys.stderr, flush=True)
    if log_fp is not None:
        with log_fp_lock:
            try:
                log_fp.write(line + "\n")
                log_fp.flush()
            except Exception:
                pass


def log(msg: str, to_history: bool = True):
    timestamp = datetime.now().strftime("%H:%M:%S")
    line = f"[{timestamp}] {msg}"
    _emit(line)
    if to_history and enable_history:
        with history_lock:
            history.append(line)


def _history_snapshot() -> list:
    """線程安全地拷貝歷史紀錄。"""
    with history_lock:
        return list(history)


def _rx_push(data: bytes):
    """Device 資料推入統一接收緩衝（僅 serial_reader 路徑呼叫）。"""
    if not data:
        return
    with rx_cond:
        rx_buffer.append(bytes(data))
        rx_cond.notify_all()


def _rx_drain() -> bytes:
    """取走目前緩衝全部資料（不清硬體 buffer，硬體由 reader 持續排空）。"""
    with rx_cond:
        if not rx_buffer:
            return b""
        chunks = list(rx_buffer)
        rx_buffer.clear()
        return b"".join(chunks)


def _rx_wait(timeout_s: float, predicate=None) -> bytes:
    """等到 predicate(buffer) 為真或超時；回傳當下全部緩衝（不清空，由呼叫方決定）。

    注意：回傳後再 `_rx_drain()` 有競爭（中間新資料會被順帶取走）。
    新程式請用 `_rx_wait_and_drain()` / `_rx_take_line()` 原子版本。
    """
    deadline = time.monotonic() + timeout_s
    with rx_cond:
        while True:
            blob = b"".join(rx_buffer)
            if predicate is None:
                if blob:
                    return blob
            elif predicate(blob):
                return blob
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return b"".join(rx_buffer)
            rx_cond.wait(timeout=remaining)


def _rx_wait_and_drain(timeout_s: float, predicate=None) -> bytes:
    """等到 predicate 為真或超時，然後在同一把鎖內原子取走全部緩衝。

    避免 `_rx_wait()` + `_rx_drain()` 兩步之間的競爭：中間到達的新資料
    不會被意外捎帶，取走的恰為判定當下的內容（超時則取走超時當下的全部，
    若為空回傳 b""）。
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
    """等一行並原子取走（同一把鎖內判定+切割+推回餘料）。

    回傳 (payload, has_newline)：
    - 有完整行：payload 為含 `\\n` 的第一行，餘料推回緩衝；
    - 超時但有零散資料：payload 為當下全部資料（已消耗，避免卡死），has_newline=False；
    - 超時且無資料：(b"", False)。
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
    """記下剛送出的字元供 echo 去重（附時間戳，過期自動失效）。"""
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
    """清掉待去重的 echo 佇列與統一接收緩衝（connect/disconnect 時避免陳舊資料污染）。"""
    with echo_lock:
        pending_chunks.clear()
    _rx_drain()


def _pending_consume(data: bytes) -> bytes:
    """從 Device 資料開頭扣除 echo（跨 chunk 比對；遺失/過期/變形的 echo 自動跳過）。

    只扣除相符的前綴、不動後面的真正輸出；serial 保序，所以開頭對不上的
    pending 一定是回顯遺失，直接丟掉重試（只丟 pending、不丟 data）。
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
                pending_chunks.popleft()  # 此段 echo 遺失或被設備改寫，丟掉重試
                continue
            buf = buf[i:]
            if i >= len(chunk):
                pending_chunks.popleft()
            else:
                pending_chunks[0] = (chunk[i:], ts)
                break
        return buf


# ============================================================
# Serial 低層操作
# ============================================================
def open_serial(port: str, baudrate: int, timeout: float = 0.1,
                bytesize: int = 8, parity: str = 'N', stopbits: float = 1) -> bool:
    global serial_conn
    try:
        log(f"[SYS] 嘗試開啟 {port} @ {baudrate} ...")
        with serial_lock:
            if serial_conn and serial_conn.is_open:
                serial_conn.close()
            serial_conn = serial.Serial(
                port=port, baudrate=baudrate, timeout=timeout,
                write_timeout=1, bytesize=bytesize, parity=parity, stopbits=stopbits,
            )
        log(f"[SYS] 成功開啟 {port} @ {baudrate}")
        _pending_clear()  # 重連時丟掉上次殘留的 echo 期待與待讀，避免污染下次 write_read
        return True
    except Exception as e:
        log(f"[ERR] 無法開啟 {port}: {e}")
        # 舊連線已在上面被 close，不能留陳舊 echo/待讀給下次連線
        _pending_clear()
        return False


def close_serial():
    global serial_conn
    with serial_lock:
        if serial_conn and serial_conn.is_open:
            serial_conn.close()
            log("[SYS] Serial 已關閉")
    _pending_clear()


def write_serial(data: bytes, source: str = "AI", to_tcp: bool = True, skip_conns=()) -> bool:
    """送資料給設備。to_tcp=False 不推給 TCP；skip_conns 跳過指定連線。

    raw 模式的 User 輸入用 skip_others：sender 即時看到自己打的字，
    其他人已有 [tag] 前綴轉發，不再收 raw（cooked 模式 sender 有本地 echo，沿用 to_tcp=False）。
    """
    with serial_lock:
        if serial_conn and serial_conn.is_open:
            try:
                serial_conn.write(data)
                serial_conn.flush()
                # 記錄「剛送出的字元」供設備 echo 去重複
                _pending_push(data)
                broadcast_text(data, show_as=f"{source} -> Device", to_tcp=to_tcp,
                               skip_conns=skip_conns)
                return True
            except Exception as e:
                log(f"[ERR] 寫入錯誤: {e}")
                return False
        else:
            log("[WARN] Serial 未連線，指令未送出")
            return False


# ============================================================
# 廣播：Serial 讀取 -> TCP(user) + 歷史
# ============================================================
def broadcast_text(data: bytes, show_as: str, to_tcp: bool = True, skip_conns=()):
    """把 bytes 廣播給 User 的 TCP clients，並記入歷史（供 MCP AI 用 serial_get_history 查）"""
    if not data:
        return

    is_device = show_as.startswith("Device")
    # 設備 echo 去重複：設備回顯剛送出的指令時，扣除相符的前綴（跨包、過期自動跳過）
    if is_device:
        data = _pending_consume(data)

    if not data:
        return

    if is_device:
        # 統一接收路徑：Device 資料先進 rx_buffer，MCP read/readline/write_read 從這裡取
        _rx_push(data)

    try:
        text = data.decode('utf-8', errors='replace').strip()
        if text:
            for line in text.splitlines():
                log(f"[{show_as}] {line}")
    except Exception:
        log(f"[{show_as}] {data.hex()}")

    if not to_tcp:
        return

    # 送給 User 的 TCP clients（近似原本 serial_bridge）
    skip = set(skip_conns) if skip_conns else set()
    with tcp_clients_lock:
        dead = []
        for conn in tcp_clients:
            if conn in skip:
                continue
            try:
                conn.sendall(data)
            except Exception:
                dead.append(conn)
        for d in dead:
            tcp_clients.remove(d)
            log(f"[SYS] TCP 客戶端斷線")


def serial_reader():
    """讀取線程：Serial -> broadcast。

    讀取時不拿 `serial_lock`：`read()` 含 timeout（預設 0.1s）是 blocking，
    拿鎖會把 `write_serial` 擋住最多 100ms。讀寫分工固定（僅本線程讀、
    `write_serial` 寫），pyserial 全雙工下可併發；`close/open` 併發由
    try/except 吸收（PortNotOpenError / OSError），下輪重取 `serial_conn`。
    """
    log("[SYS] Serial 讀取線程已啟動")
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
                    # 不拿鎖：blocking read 不擋 writer
                    data = conn.read(waiting or 1)
                except Exception:
                    data = b""
                if data:
                    broadcast_text(data, "Device -> ALL")
            else:
                try:
                    try:
                        # 不拿鎖：timeout 0.1s 期間 writer 可正常寫入
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
            log(f"[ERR] Serial 讀取錯誤: {e}")
            time.sleep(1)


# ============================================================
# TCP bridge：給 User（人）用 PuTTY/telnet 連
# ============================================================
def tcp_bridge(listen_host: str, listen_port: int):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        srv.bind((listen_host, listen_port))
    except Exception as e:
        log(f"[ERR] 無法綁定 TCP {listen_host}:{listen_port} - {e}")
        return
    srv.listen(10)
    srv.settimeout(0.5)  # 讓 running=False 時能跳出 accept，正常關閉
    log(f"[SYS] TCP bridge 等待 User 連線中 {listen_host}:{listen_port} ...")

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
    log(f"[SYS] User 連線 {addr} (目前 {n_now} 個)")
    try:
        snapshot = _history_snapshot()
        if enable_history and snapshot:
            conn.sendall(("\n".join(snapshot) + "\n").encode('utf-8', errors='replace'))
        conn.sendall("--- 已連線，可輸入指令，AI/人的操作會互相廣播 ---\n".encode('utf-8'))
        if cooked_mode:
            conn.sendall("--- 行編輯已啟用：Up/Down 召回歷史 ---\n".encode('utf-8'))
    except Exception:
        pass

    editor = CookedLine() if cooked_mode else None
    try:
        while running:
            data = conn.recv(4096)
            if not data:
                break
            data = _normalize_user_input(data)  # 按 --line-ending 正規化（lf 模式把 PuTTY 的 CRLF 轉 LF）
            if not data:
                continue
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
            tag = f"{addr} -> Device"
            with tcp_clients_lock:
                dead = []
                for c in tcp_clients:
                    if c != conn:
                        try:
                            c.sendall(f"[{tag}] ".encode() + data)
                        except Exception:
                            dead.append(c)
                for d in dead:
                    try:
                        tcp_clients.remove(d)
                    except ValueError:
                        pass
                    log("[SYS] TCP 客戶端斷線（轉發時）")
            # 歷史只記一次：由 write_serial -> broadcast_text 記錄 [User(addr) -> Device]。
            # cooked：sender 有本地 echo，不推 raw；raw：只推回給 sender（即時可見），
            # 其他人已有上面的 [tag] 轉發，不再收 raw；含 ESC 的輸入不推（終端機會誤解讀）。
            if editor is not None:
                write_serial(data, source=f"User({addr})", to_tcp=False)
            elif _wants_pushback(data):
                with tcp_clients_lock:
                    others = [c for c in tcp_clients if c != conn]
                write_serial(data, source=f"User({addr})", to_tcp=True, skip_conns=others)
            else:
                write_serial(data, source=f"User({addr})", to_tcp=False)
    except Exception as e:
        log(f"[SYS] User {addr} 錯誤: {e}")
    finally:
        with tcp_clients_lock:
            if conn in tcp_clients:
                tcp_clients.remove(conn)
        conn.close()
        log(f"[SYS] User 離線 {addr} (剩餘 {len(tcp_clients)} 個)")


# ============================================================
# MCP Server (v1.x classic Server + stdio transport)
# ============================================================
server = Server("serial-bridge-mcp")


def _text(msg: str) -> list[TextContent]:
    return [TextContent(type="text", text=msg)]


def _encode_write_data(data_str: str, encoding: str, append_crlf: bool, line_ending=None):
    """把 write/write_read 的字串參數轉成 bytes；回傳 (data, err_msg）。

    append_crlf=False 不加換行；否則加 line_ending（未指定用 server --line-ending）。
    hex 模式同樣適用 append_crlf（之前會靜默忽略）。
    """
    if encoding == "hex":
        try:
            data = bytes.fromhex(data_str)
        except ValueError:
            return b"", "❌ Hex 格式錯誤"
    else:
        try:
            data = data_str.encode(encoding)
        except (LookupError, ValueError):
            return b"", f"❌ 不支援的編碼: {encoding}"
    if append_crlf:
        ending = line_ending or default_line_ending
        suffix = LINE_ENDINGS.get(ending)
        if suffix is None:
            return b"", f"❌ 不支援的換行: {ending}（crlf/lf/cr）"
        data += suffix
    return data, ""


def _normalize_user_input(data: bytes) -> bytes:
    """按 server --line-ending 正規化 TCP user 輸入的換行（PuTTY Enter 多半送 CRLF）。

    lf 模式（預設）把 CRLF/CR 轉 LF；cr 模式把 CRLF/LF 轉 CR；crlf 原樣直通。
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
    """單條 TCP 連線的行編輯器（--cooked 模式，給沒有行編輯能力的 dumb shell 用）。

    bridge 接管本地 echo：可列印字元即時回顯，Enter 才整行送給設備；
    Up/Down 召回此連線送過的指令。raw 模式（預設）走直通，不受影響。
    Byte-oriented：退格遇到 UTF-8 multibyte 會整段刪；游標對齊按 byte 計。
    """
    HIST_MAX = 100
    ESC_TIMEOUT_S = 0.5

    def __init__(self):
        self.buf = bytearray()
        self.cursor = 0
        self.hist = []        # 送過的完整行（不含換行）
        self.hidx = 0         # 瀏覽位置（== len(hist) 表示正在打當前輸入）
        self.saved = b""      # 瀏覽歷史時暫存的當前輸入
        self.esc = b""        # 未收完的 ESC 序列（等下一包）
        self.esc_ts = 0.0

    # ---------- 顯示 ----------
    def _redraw(self, out: bytearray):
        # \r + 空白蓋掉舊行 + \r + 重印 + 游標歸位（只用 CR/空白/BS，不依賴 ANSI）
        width = len(self.buf) + 32
        if width < 64:
            width = 64
        out += b"\r" + b" " * width + b"\r" + bytes(self.buf)
        back = len(self.buf) - self.cursor
        if back > 0:
            out += b"\x08" * back

    def _bell(self, out: bytearray):
        out += b"\x07"

    # ---------- 歷史 ----------
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

    # ---------- 編輯 ----------
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
        del self.buf[self.cursor:self.cursor + 1]
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
        """解析 data[i] == ESC 處的序列；回傳新 i（None 表示等下一包）。"""
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
            # 未知 CSI：找終止字元（0x40-0x7E，找到就是完整序列，原樣透傳；
            # 沒找到表示被截斷，等下一包）
            j = i + 2
            while j < n and 0x20 <= data[j] <= 0x3F:
                j += 1
            if j < n and 0x40 <= data[j] <= 0x7E:
                dev += data[i:j + 1]
                return j + 1
            if j == i + 2:
                # ESC[ 後面完全不是 CSI 字元：之前的 stash 是誤判，丟掉 ESC[ 從此處重來
                return i + 2
            self.esc = data[i:]
            self.esc_ts = now
            return None
        if n - i == 1 or data[i + 1] == 0x5B:
            self.esc = data[i:]  # 被截斷，等下一包
            self.esc_ts = now
            return None
        return i + 1  # 裸 ESC（如 Alt 組合）：丟掉 ESC，主鍵下輪當一般字元

    def feed(self, data: bytes):
        """吃一包 TCP 輸入；回傳 (to_conn, to_device)。
        to_conn 直接回給 sender（本地 echo/重繪），to_device 整包送設備。"""
        now = time.monotonic()
        if self.esc:
            if now - self.esc_ts > self.ESC_TIMEOUT_S:
                self.esc = b""  # 陳舊未完成序列丟掉
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
            if b in (0x0D, 0x0A):  # Enter 變體：CR / LF / CRLF / CR NUL 都算一次
                if b == 0x0D and i + 1 < n and data[i + 1] in (0x0A, 0x00):
                    i += 1
                self._submit(out, dev)
                i += 1
                continue
            if b == 0x00:
                i += 1  # NUL 丟掉
                continue
            if b == 0x03:  # Ctrl+C：中斷設備 + 清行
                dev += b"\x03"
                self._reset_line()
                out += b"^C\r\n"
                i += 1
                continue
            if b == 0x1A:  # Ctrl+Z：透傳 + 清行
                dev += b"\x1a"
                self._reset_line()
                out += b"^Z\r\n"
                i += 1
                continue
            if b == 0x15:  # Ctrl+U：整行刪
                self._leave_browse()
                self.buf.clear()
                self.cursor = 0
                self._redraw(out)
                i += 1
                continue
            if b == 0x17:  # Ctrl+W：刪一個詞
                self._del_word(out)
                i += 1
                continue
            if b == 0x04:  # Ctrl+D：空行送 EOT，否則刪游標字元
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
            if b == 0x09:  # Tab：把當前行一起送出再補 Tab（device 要有內容才能補全）
                self._leave_browse()
                dev += bytes(self.buf) + b"\x09"
                self.buf.clear()
                self.cursor = 0
                i += 1
                continue
            if b < 0x20:  # 其他控制字元：透傳
                dev += bytes((b,))
                i += 1
                continue
            self._insert(out, bytes((b,)))  # 可列印（含 multibyte 原樣）
            i += 1
        return bytes(out), bytes(dev)


def _wants_pushback(data: bytes) -> bool:
    """User 輸入是否要推回給 sender 即時顯示。

    cooked 不推（本地 echo 已顯示）；raw 含 ESC 不推（方向鍵等序列推回去會被
    終端機解讀成游標動作，反而把設備重繪打到錯的行；交給設備回顯/重繪顯示）。
    其餘 raw 可列印輸入推回給 sender（打字即時可見）。
    """
    return not cooked_mode and b"\x1b" not in data


@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(name="serial_connect", description="連接到 Serial Port（全部 AI/User 共用同一個 port）",
             inputSchema={"type": "object",
                          "properties": {
                              "port": {"type": "string", "description": "Serial port，例: /dev/ttyUSB0, COM3"},
                              "baudrate": {"type": "integer", "description": "Baud rate", "default": 115200},
                              "bytesize": {"type": "integer", "description": "Data bits", "default": 8, "enum": [5, 6, 7, 8]},
                              "parity": {"type": "string", "description": "Parity", "default": "N", "enum": ["N", "E", "O"]},
                              "stopbits": {"type": "number", "description": "Stop bits", "default": 1, "enum": [1, 1.5, 2]},
                          },
                          "required": ["port"]}),
        Tool(name="serial_disconnect", description="斷開 Serial Port 連接",
             inputSchema={"type": "object", "properties": {}}),
        Tool(name="serial_write", description="寫入資料到 Serial Port（User 與其他 AI 可見，會互相廣播）",
             inputSchema={"type": "object",
                          "properties": {
                              "data": {"type": "string", "description": "要寫入的資料"},
                              "encoding": {"type": "string", "description": "編碼", "default": "utf-8", "enum": ["utf-8", "ascii", "hex"]},
                              "append_crlf": {"type": "boolean", "description": "是否自動加換行", "default": True},
                              "line_ending": {"type": "string", "description": "換行字元（預設跟 server --line-ending，預設 lf；需 CRLF 的設備用 crlf）", "enum": ["crlf", "lf", "cr"]},
                          },
                          "required": ["data"]}),
        Tool(name="serial_read", description="從統一接收緩衝讀取 Device 回應（與 TCP 廣播/歷史同源，不會跟讀取線程搶 port）",
             inputSchema={"type": "object",
                          "properties": {"timeout_ms": {"type": "integer", "description": "讀取超時（毫秒）", "default": 200}}}),
        Tool(name="serial_readline", description="從統一接收緩衝讀取一行（直到換行）",
             inputSchema={"type": "object",
                          "properties": {"timeout_ms": {"type": "integer", "description": "讀取超時（毫秒）", "default": 1500}}}),
        Tool(name="serial_write_read", description="送指令後等待回應（先清待讀緩衝再寫，適合問答式設備；會消耗緩衝）",
             inputSchema={"type": "object",
                          "properties": {
                              "data": {"type": "string", "description": "要寫入的資料"},
                              "encoding": {"type": "string", "description": "編碼", "default": "utf-8", "enum": ["utf-8", "ascii", "hex"]},
                              "append_crlf": {"type": "boolean", "description": "是否自動加換行", "default": True},
                              "line_ending": {"type": "string", "description": "換行字元（預設跟 server --line-ending，預設 lf；需 CRLF 的設備用 crlf）", "enum": ["crlf", "lf", "cr"]},
                              "wait_ms": {"type": "integer", "description": "寫入後等待回應的時間（毫秒）", "default": 500},
                          },
                          "required": ["data"]}),
        Tool(name="serial_status", description="取得 Serial Port 連線狀態",
             inputSchema={"type": "object", "properties": {}}),
        Tool(name="serial_list_ports", description="列出所有可用 Serial Ports",
             inputSchema={"type": "object", "properties": {}}),
        Tool(name="serial_get_history", description="取得最近的 Serial 輸出歷史（含 User / AI / Device 操作）",
             inputSchema={"type": "object",
                          "properties": {"lines": {"type": "integer", "description": "行數", "default": 50}}}),
        Tool(name="serial_search_history", description="搜尋歷史紀錄（按關鍵字/來源過濾）",
             inputSchema={"type": "object",
                          "properties": {
                              "keyword": {"type": "string", "description": "關鍵字（子字串比對，空字串=不過濾）", "default": ""},
                              "source": {"type": "string", "description": "來源過濾：AI / User / Device / SYS / 全部", "default": "全部", "enum": ["全部", "AI", "User", "Device", "SYS"]},
                              "lines": {"type": "integer", "description": "往回搜尋幾行", "default": 200},
                              "limit": {"type": "integer", "description": "最多回傳幾行", "default": 50},
                          }}),
        Tool(name="serial_flush", description="清除 Serial Port 緩衝區 (input/output/both)",
             inputSchema={"type": "object",
                          "properties": {"which": {"type": "string", "default": "both", "enum": ["input", "output", "both"]}}}),
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
            return _text(f"✅ 已連接到 {arguments['port']} @ {arguments.get('baudrate', 115200)}（可供所有 AI/User 共用）")
        return _text(f"❌ 無法連接到 {arguments.get('port')}，請確認 port 存在、有權限、未被占用")

    elif name == "serial_disconnect":
        close_serial()
        return _text("✅ 已斷開 Serial Port")

    elif name == "serial_write":
        data_str = arguments.get("data", "")
        enc = arguments.get("encoding", "utf-8")
        data, err = _encode_write_data(data_str, enc, arguments.get("append_crlf", True),
                                       arguments.get("line_ending"))
        if err:
            return _text(err)
        ok = write_serial(data, source="AI")
        return _text(f"✅ 已寫入: {data_str}" if ok else "❌ 寫入失敗（Serial 未連線）")

    elif name == "serial_write_read":
        data_str = arguments.get("data", "")
        enc = arguments.get("encoding", "utf-8")
        wait_ms = arguments.get("wait_ms", 500)
        data, err = _encode_write_data(data_str, enc, arguments.get("append_crlf", True),
                                       arguments.get("line_ending"))
        if err:
            return _text(err)
        if not _is_serial_open():
            return _text("❌ Serial 未連線")
        # 問答式語義：先丟棄寫入前的陳舊待讀（期間的 spontaneous 輸出會一併丟棄，
        # 適合問答式設備；要保留串流請改用 serial_write + serial_read）。
        _rx_drain()
        ok = write_serial(data, source="AI")
        if not ok:
            return _text("❌ 寫入失敗（Serial 未連線）")
        # 原子等待+取走：判定與清空同一把鎖，避免 wait-then-drain 競爭。
        blob = _rx_wait_and_drain(wait_ms / 1000.0)
        if blob:
            return _text(f"📥 回應 ({len(blob)} bytes):\n{blob.decode('utf-8', errors='replace')}")
        return _text("📭 寫入成功，但等待時間內沒有回應（超時）")

    elif name == "serial_read":
        timeout_ms = arguments.get("timeout_ms", 200)
        if not _is_serial_open():
            return _text("❌ Serial 未連線")
        # 原子等待+取走（消耗語義）：與 TCP 廣播/歷史同源，不會跟讀取線程搶 port。
        data = _rx_wait_and_drain(timeout_ms / 1000.0)
        if data:
            return _text(f"📥 收到 ({len(data)} bytes):\n{data.decode('utf-8', errors='replace')}")
        return _text("📭 沒有資料（超時）")

    elif name == "serial_readline":
        timeout_ms = arguments.get("timeout_ms", 1500)
        if not _is_serial_open():
            return _text("❌ Serial 未連線")
        # 原子取行：判定+切割+推回餘料同一把鎖；超時有零散資料則消耗回傳（避免卡死）。
        line, has_newline = _rx_take_line(timeout_ms / 1000.0)
        if has_newline:
            return _text(f"📥 收到一行: {line.decode('utf-8', errors='replace').strip()}")
        if line:
            return _text(f"📥 收到 (無換行, {len(line)} bytes): {line.decode('utf-8', errors='replace').strip()}")
        return _text("📭 沒有完整一行（超時）")

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
        hist_state = "停用 (--no-history)" if not enable_history else f"啟用 ({len(_history_snapshot())} 行暫存)"
        edit_state = "cooked" if cooked_mode else "raw"
        if info is not None:
            port, baud, bytesize, parity, stopbits, in_waiting = info
            return _text(f"✅ Serial 狀態:\n  Port: {port}\n  Baud: {baud}\n"
                         f"  Bytesize: {bytesize} Parity: {parity} Stopbits: {stopbits}\n"
                         f"  In Waiting (硬體): {in_waiting}\n"
                         f"  待讀緩衝: {n_rx_bytes} bytes / {n_rx_chunks} 段\n"
                         f"  歷史紀錄: {hist_state}\n"
                         f"  行編輯: {edit_state}\n"
                         f"  TCP User: {n_tcp} 個")
        return _text(f"❌ Serial 未連接\n  待讀緩衝: {n_rx_bytes} bytes / {n_rx_chunks} 段\n"
                     f"  歷史紀錄: {hist_state}\n  行編輯: {edit_state}\n  TCP User: {n_tcp} 個")

    elif name == "serial_list_ports":
        from serial.tools.list_ports import comports
        ports = list(comports())
        if ports:
            return _text("📋 可用 Ports:\n" + "\n".join(f"  - {p.device}: {p.description}" for p in ports))
        return _text("📭 找不到任何 Serial Ports")

    elif name == "serial_get_history":
        if not enable_history:
            return _text("📭 歷史紀錄功能已停用（啟動時加了 --no-history）")
        lines = arguments.get("lines", 50)
        recent = _history_snapshot()[-lines:]
        if recent:
            return _text(f"📜 最近 {len(recent)} 行:\n" + "\n".join(recent))
        return _text("📭 沒有歷史紀錄")

    elif name == "serial_search_history":
        if not enable_history:
            return _text("📭 歷史紀錄功能已停用（啟動時加了 --no-history）")
        keyword = arguments.get("keyword", "") or ""
        source = arguments.get("source", "全部")
        lines = arguments.get("lines", 200)
        limit = arguments.get("limit", 50)
        pool = _history_snapshot()[-lines:]
        out = []
        for entry in pool:
            if source != "全部" and f"[{source}" not in entry and f"({source}" not in entry and f"{source} ->" not in entry:
                continue
            if keyword and keyword not in entry:
                continue
            out.append(entry)
        out = out[-limit:]
        if out:
            return _text(f"🔍 命中 {len(out)} 行 (keyword={keyword!r}, source={source}):\n" + "\n".join(out))
        return _text(f"📭 沒有命中 (keyword={keyword!r}, source={source})")

    elif name == "serial_flush":
        which = arguments.get("which", "both")
        _rx_drain()  # 先清統一接收緩衝的待讀
        with serial_lock:
            conn = serial_conn
            if not conn or not conn.is_open:
                return _text("✅ 已清除待讀緩衝（Serial 未連線，硬體 buffer 無需清除）")
            try:
                if which in ("input", "both"):
                    conn.reset_input_buffer()
                if which in ("output", "both"):
                    conn.reset_output_buffer()
            except Exception as e:
                return _text(f"⚠️ 清除硬體緩衝失敗: {e}（待讀緩衝已清除）")
            return _text(f"✅ 已清除 {which} 緩衝區")

    return _text(f"❌ 未知工具: {name}")


def run_stdio():
    """以 stdio transport 服務 MCP 連線（opencode 以 local MCP spawn 本程式）。"""
    import anyio

    async def _serve():
        async with stdio_server() as (read_stream, write_stream):
            await server.run(read_stream, write_stream, server.create_initialization_options())

    anyio.run(_serve)


def main():
    parser = argparse.ArgumentParser(description="Serial Broadcast Bridge MCP Server - 人/AI 共用 COM port")
    parser.add_argument("--port", "-p", help="Serial port，例: COM3 或 /dev/ttyUSB0")
    parser.add_argument("--baud", "-b", type=int, default=115200, help="Baud rate")
    parser.add_argument("--tcp", "-t", type=int, default=7001, help="User TCP/telnet 埠 (預設 7001)")
    parser.add_argument("--tcphost", default="127.0.0.1",
                        help="TCP 綁定 IP（User 用；預設 127.0.0.1 只聽本機。用 0.0.0.0 會暴露到 LAN，任何人可注入 serial 指令，請確認必要才用）")
    parser.add_argument("--auto-connect", action="store_true", help="啟動時自動連 serial")
    parser.add_argument("--no-history", action="store_true", help="停用歷史紀錄：不記 history、新 TCP 連線不補歷史、serial_get_history 回報停用（即時廣播不受影響）")
    parser.add_argument("--log-file", default=None, help="持久化 log 到檔案（append），例: serial.log；stderr 照常輸出")
    parser.add_argument("--line-ending", default="lf", choices=["crlf", "lf", "cr"],
                        help="送出的換行字元（預設 lf；少數需要 CRLF 的設備請用 crlf）")
    parser.add_argument("--cooked", action="store_true",
                        help="啟用 bridge 端行編輯：本地 echo、Up/Down 召回歷史（給沒有行編輯的 dumb shell 用；預設 raw 直通）")
    args = parser.parse_args()

    global enable_history, log_fp, default_line_ending, cooked_mode
    enable_history = not args.no_history
    default_line_ending = args.line_ending
    cooked_mode = args.cooked
    if args.log_file:
        try:
            log_fp = open(args.log_file, "a", encoding="utf-8")
        except Exception as e:
            print(f"[ERR] 無法開啟 log 檔 {args.log_file}: {e}", file=sys.stderr)
            log_fp = None

    _emit("=" * 62)
    _emit(" Serial Broadcast Bridge MCP Server (stdio)")
    _emit(f"  Serial : {args.port or '(未指定，用 serial_connect)'} @ {args.baud}")
    _emit(f"  User   : telnet/raw {args.tcphost}:{args.tcp}")
    _emit("  兩方可同時連線，操作互相廣播，Device 回應廣播給所有人")
    _emit(f"  歷史紀錄: {'停用 (--no-history，即時廣播不受影響)' if args.no_history else '啟用'}")
    _emit(f"  換行    : {args.line_ending} {LINE_ENDINGS[args.line_ending]!r}")
    _emit(f"  行編輯  : {'cooked（本地 echo + Up/Down 歷史）' if args.cooked else 'raw（直通）'}")
    _emit(f"  Log 檔  : {args.log_file or '(未啟用，用 --log-file 持久化)'}")
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
        log("[SYS] 收到 Ctrl+C，關閉中...")
    finally:
        running = False
        close_serial()
        log("[SYS] 已關閉")
        if log_fp is not None:
            try:
                with log_fp_lock:
                    log_fp.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
