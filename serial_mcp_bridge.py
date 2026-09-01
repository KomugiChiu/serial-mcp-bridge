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
history = deque(maxlen=1000)          # 歷史紀錄，給新連線者看
tcp_clients = []                      # User 的 TCP 連線清單
tcp_clients_lock = threading.Lock()
pending_echo = bytearray()            # 剛送出給設備的字元，供 echo 去重複
echo_lock = threading.Lock()
running = True


def log(msg: str, to_history: bool = True):
    timestamp = datetime.now().strftime("%H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line, file=sys.stderr, flush=True)
    if to_history:
        history.append(line)


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
        return True
    except Exception as e:
        log(f"[ERR] 無法開啟 {port}: {e}")
        return False


def close_serial():
    global serial_conn
    with serial_lock:
        if serial_conn and serial_conn.is_open:
            serial_conn.close()
            log("[SYS] Serial 已關閉")


def write_serial(data: bytes, source: str = "AI") -> bool:
    with serial_lock:
        if serial_conn and serial_conn.is_open:
            try:
                serial_conn.write(data)
                serial_conn.flush()
                # 記錄「剛送出的字元」供設備 echo 去重複
                with echo_lock:
                    pending_echo.extend(data)
                    if len(pending_echo) > 256:
                        del pending_echo[: len(pending_echo) - 256]
                broadcast_text(data, show_as=f"{source} -> Device")
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
def broadcast_text(data: bytes, show_as: str):
    """把 bytes 廣播給 User 的 TCP clients，並記入歷史（供 MCP AI 用 serial_get_history 查）"""
    if not data:
        return

    # 設備 echo 去重複：設備回顯剛送出的指令時，扣除相符的字元，避免「ls」重複出現
    if show_as.startswith("Device"):
        with echo_lock:
            consumed = 0
            for b in data:
                if consumed < len(pending_echo) and b == pending_echo[consumed]:
                    consumed += 1
                else:
                    break
            if consumed:
                data = data[consumed:]
                del pending_echo[:consumed]

    if not data:
        return

    try:
        text = data.decode('utf-8', errors='replace').strip()
        if text:
            for line in text.splitlines():
                log(f"[{show_as}] {line}")
    except Exception:
        log(f"[{show_as}] {data.hex()}")

    # 送給 User 的 TCP clients（近似原本 serial_bridge）
    with tcp_clients_lock:
        dead = []
        for conn in tcp_clients:
            try:
                conn.sendall(data)
            except Exception:
                dead.append(conn)
        for d in dead:
            tcp_clients.remove(d)
            log(f"[SYS] TCP 客戶端斷線")


def serial_reader():
    """讀取線程：Serial -> broadcast"""
    log("[SYS] Serial 讀取線程已啟動")
    while running:
        try:
            with serial_lock:
                conn = serial_conn
                is_open = conn is not None and conn.is_open
            if not is_open:
                time.sleep(0.1)
                continue

            waiting = conn.in_waiting if conn else 0
            if waiting:
                with serial_lock:
                    data = conn.read(waiting or 1)
                if data:
                    broadcast_text(data, "Device -> ALL")
            else:
                try:
                    with serial_lock:
                        data = conn.read(1)
                    if data:
                        with serial_lock:
                            extra = conn.in_waiting
                            if extra:
                                data += conn.read(extra)
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
    log(f"[SYS] TCP bridge 等待 User 連線中 {listen_host}:{listen_port} ...")

    while running:
        try:
            conn, addr = srv.accept()
        except Exception:
            break
        with tcp_clients_lock:
            tcp_clients.append(conn)
        threading.Thread(target=handle_tcp_client, args=(conn, addr), daemon=True).start()


def handle_tcp_client(conn, addr):
    log(f"[SYS] User 連線 {addr} (目前 {len(tcp_clients)+1} 個)")
    try:
        if history:
            conn.sendall(("\n".join(history) + "\n").encode('utf-8', errors='replace'))
        conn.sendall("--- 已連線，可輸入指令，AI/人的操作會互相廣播 ---\n".encode('utf-8'))
    except Exception:
        pass

    try:
        while running:
            data = conn.recv(4096)
            if not data:
                break
            text_preview = data.decode('utf-8', errors='replace').strip()
            tag = f"{addr} -> Device"
            with tcp_clients_lock:
                for c in tcp_clients:
                    if c != conn:
                        try:
                            c.sendall(f"[{tag}] ".encode() + data)
                        except Exception:
                            pass
            if text_preview:
                for line in text_preview.splitlines():
                    log(f"[{tag}] {line}")
            write_serial(data, source=f"User({addr})")
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
                              "append_crlf": {"type": "boolean", "description": "是否自動加 \\r\\n", "default": True},
                          },
                          "required": ["data"]}),
        Tool(name="serial_read", description="從 Serial Port 讀取目前緩衝資料",
             inputSchema={"type": "object",
                          "properties": {"timeout_ms": {"type": "integer", "description": "讀取超時（毫秒）", "default": 200}}}),
        Tool(name="serial_readline", description="從 Serial Port 讀取一行（直到換行）",
             inputSchema={"type": "object",
                          "properties": {"timeout_ms": {"type": "integer", "description": "讀取超時（毫秒）", "default": 1500}}}),
        Tool(name="serial_status", description="取得 Serial Port 連線狀態",
             inputSchema={"type": "object", "properties": {}}),
        Tool(name="serial_list_ports", description="列出所有可用 Serial Ports",
             inputSchema={"type": "object", "properties": {}}),
        Tool(name="serial_get_history", description="取得最近的 Serial 輸出歷史（含 User / AI / Device 操作）",
             inputSchema={"type": "object",
                          "properties": {"lines": {"type": "integer", "description": "行數", "default": 50}}}),
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
        if enc == "hex":
            try:
                data = bytes.fromhex(data_str)
            except ValueError:
                return _text("❌ Hex 格式錯誤")
        else:
            data = data_str.encode(enc)
            if arguments.get("append_crlf", True):
                data += b"\r\n"
        ok = write_serial(data, source="AI")
        return _text(f"✅ 已寫入: {data_str}" if ok else "❌ 寫入失敗（Serial 未連線）")

    elif name == "serial_read":
        timeout_ms = arguments.get("timeout_ms", 200)
        with serial_lock:
            conn = serial_conn
        if not conn or not conn.is_open:
            return _text("❌ Serial 未連線")
        with serial_lock:
            old = conn.timeout
            conn.timeout = timeout_ms / 1000.0
            data = conn.read(1)
            if data:
                extra = conn.in_waiting
                if extra:
                    data += conn.read(extra)
            conn.timeout = old
        if data:
            return _text(f"📥 收到 ({len(data)} bytes):\n{data.decode('utf-8', errors='replace')}")
        return _text("📭 沒有資料（超時）")

    elif name == "serial_readline":
        timeout_ms = arguments.get("timeout_ms", 1500)
        with serial_lock:
            conn = serial_conn
        if not conn or not conn.is_open:
            return _text("❌ Serial 未連線")
        with serial_lock:
            old = conn.timeout
            conn.timeout = timeout_ms / 1000.0
            line = conn.readline()
            conn.timeout = old
        if line:
            return _text(f"📥 收到一行: {line.decode('utf-8', errors='replace').strip()}")
        return _text("📭 沒有完整一行（超時）")

    elif name == "serial_status":
        with serial_lock:
            conn = serial_conn
        if conn and conn.is_open:
            return _text(f"✅ Serial 狀態:\n  Port: {conn.port}\n  Baud: {conn.baudrate}\n"
                         f"  Bytesize: {conn.bytesize} Parity: {conn.parity} Stopbits: {conn.stopbits}\n"
                         f"  In Waiting: {conn.in_waiting}")
        return _text("❌ Serial 未連接")

    elif name == "serial_list_ports":
        from serial.tools.list_ports import comports
        ports = list(comports())
        if ports:
            return _text("📋 可用 Ports:\n" + "\n".join(f"  - {p.device}: {p.description}" for p in ports))
        return _text("📭 找不到任何 Serial Ports")

    elif name == "serial_get_history":
        lines = arguments.get("lines", 50)
        recent = list(history)[-lines:]
        if recent:
            return _text(f"📜 最近 {len(recent)} 行:\n" + "\n".join(recent))
        return _text("📭 沒有歷史紀錄")

    elif name == "serial_flush":
        which = arguments.get("which", "both")
        with serial_lock:
            conn = serial_conn
        if not conn or not conn.is_open:
            return _text("❌ Serial 未連線")
        if which in ("input", "both"):
            conn.reset_input_buffer()
        if which in ("output", "both"):
            conn.reset_output_buffer()
        return _text(f"✅ 已清除 {which} 緩衝區")

    return _text(f"❌ 未知工具: {name}")


def run_stdio():
    """以 stdio transport 服務 MCP 連線（opencode 以 local MCP spawn 本程式）。"""
    import anyio
    from mcp import StdioServerParameters

    async def _serve():
        async with stdio_server() as (read_stream, write_stream):
            await server.run(read_stream, write_stream, server.create_initialization_options())

    anyio.run(_serve)


def main():
    parser = argparse.ArgumentParser(description="Serial Broadcast Bridge MCP Server - 人/AI 共用 COM port")
    parser.add_argument("--port", "-p", help="Serial port，例: COM3 或 /dev/ttyUSB0")
    parser.add_argument("--baud", "-b", type=int, default=115200, help="Baud rate")
    parser.add_argument("--tcp", "-t", type=int, default=7001, help="User TCP/telnet 埠 (預設 7001)")
    parser.add_argument("--tcphost", default="0.0.0.0", help="TCP 綁定 IP（User 用）")
    parser.add_argument("--auto-connect", action="store_true", help="啟動時自動連 serial")
    args = parser.parse_args()

    print("=" * 62, file=sys.stderr)
    print(" Serial Broadcast Bridge MCP Server (stdio)", file=sys.stderr)
    print(f"  Serial : {args.port or '(未指定，用 serial_connect)'} @ {args.baud}", file=sys.stderr)
    print(f"  User   : telnet/raw {args.tcphost}:{args.tcp}", file=sys.stderr)
    print("  兩方可同時連線，操作互相廣播，Device 回應廣播給所有人", file=sys.stderr)
    print("=" * 62, file=sys.stderr)
    sys.stderr.flush()

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


if __name__ == "__main__":
    main()
