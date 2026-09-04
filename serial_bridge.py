#!/usr/bin/env python3
"""
DEPRECATED：舊版純 TCP bridge，已由 `serial_mcp_bridge.py` 取代。

- 新版修正：統一 rx_buffer、history lock、echo 去重、serial_flush/status race、TCP 關閉等。
- 此檔僅保留供臨時對照，請新部署一律使用 `serial_mcp_bridge.py`。
- 預計下個版本刪除。

Serial Broadcast Bridge - 讓 人 與 AI 同時控制同一個 COM port 並互看對方操作
Usage:
  python serial_bridge.py --port COM3 --baud 115200
  python serial_bridge.py --port /dev/ttyUSB0 --baud 115200 --tcp 7001
  python serial_bridge.py --port COM3 --baud 9600 --host 127.0.0.1 --tcp 7001 --log serial.log
"""
import argparse
import socket
import threading
import time
import sys
from collections import deque
from datetime import datetime

try:
    import serial
except ImportError:
    print("[ERROR] 需要 pyserial: pip install pyserial")
    sys.exit(1)

# 全域狀態
clients = []
clients_lock = threading.Lock()
history = deque(maxlen=100)  # 給新連線者看歷史
serial_conn = None
serial_lock = threading.Lock()
running = True


def log(msg, to_history=True):
    timestamp = datetime.now().strftime("%H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line, flush=True)
    if to_history and history is not None:
        history.append(line)


def broadcast_to_clients(data: bytes, source_addr=None, show_as="Device -> ALL"):
    """把 data 廣播給所有 TCP 客戶端"""
    if not data:
        return
    # 1. 印到 server console + history
    try:
        text = data.decode('utf-8', errors='replace').strip()
        if text:
            for line in text.splitlines():
                log(f"[{show_as}] {line}")
    except:
        log(f"[{show_as}] {data.hex()}")

    # 2. 廣播給所有 TCP clients
    with clients_lock:
        dead = []
        for conn, addr in clients:
            try:
                conn.sendall(data)
            except:
                dead.append((conn, addr))
        for d in dead:
            clients.remove(d)
            log(f"[SYS] 客戶端斷線 {d[1]}")


def handle_client(conn, addr):
    log(f"[SYS] 客戶端連線 {addr} (目前 {len(clients)+1} 個客戶端)")
    # 送歷史紀錄給新連線者
    try:
        if history:
            conn.sendall(("\n".join(history) + "\n").encode('utf-8', errors='replace'))
            conn.sendall("--- 已連線，可開始收發，AI/人的操作會互相廣播 ---\n".encode('utf-8'))
    except:
        pass

    try:
        while running:
            data = conn.recv(4096)
            if not data:
                break
            # 廣播給其他 clients，讓人看到 AI 的指令 / AI看到人的指令
            text_preview = data.decode('utf-8', errors='replace').strip()
            tag = f"{addr} -> Device"
            # 先廣播給其他人（讓人看到 AI 打了什麼）
            with clients_lock:
                for c, a in clients:
                    if c != conn:
                        try:
                            # 讓其他客戶端看到是誰送的
                            c.sendall(f"[{tag}] ".encode() + data)
                        except:
                            pass
            
            # 同時 log
            if text_preview:
                for line in text_preview.splitlines():
                    log(f"[{tag}] {line}")

            # 送給硬體
            with serial_lock:
                if serial_conn and serial_conn.is_open:
                    serial_conn.write(data)
                else:
                    log("[WARN] Serial 未連線，指令未送出")
                    try:
                        conn.sendall("[WARN] Serial 未連線\n".encode('utf-8'))
                    except:
                        pass
    except Exception as e:
        log(f"[SYS] 客戶端 {addr} 錯誤: {e}")
    finally:
        with clients_lock:
            if (conn, addr) in clients:
                clients.remove((conn, addr))
        conn.close()
        log(f"[SYS] 客戶端離線 {addr} (剩餘 {len(clients)} 個)")


def serial_reader():
    """持續讀 serial，廣播給所有 TCP 客戶端"""
    log("[SYS] Serial 讀取線程已啟動")
    while running:
        try:
            if serial_conn and serial_conn.is_open and serial_conn.in_waiting:
                with serial_lock:
                    data = serial_conn.read(serial_conn.in_waiting or 1)
                if data:
                    broadcast_to_clients(data, show_as="Device -> ALL")
            else:
                # 有些設備 in_waiting 不準，改用 timeout read 1 byte
                with serial_lock:
                    if serial_conn and serial_conn.is_open:
                        # 避免佔鎖太久
                        pass
                time.sleep(0.01)
                # 嘗試讀 1 byte with timeout
                try:
                    with serial_lock:
                        if serial_conn and serial_conn.is_open:
                            data = serial_conn.read(1)
                            if data:
                                # 有資料就把剩下的也讀出來
                                waiting = serial_conn.in_waiting
                                if waiting:
                                    data += serial_conn.read(waiting)
                                broadcast_to_clients(data, show_as="Device -> ALL")
                                continue
                except Exception as e:
                    # 讀取錯誤可能是斷線
                    pass
                time.sleep(0.02)
        except Exception as e:
            log(f"[ERR] Serial 讀取錯誤: {e}")
            time.sleep(1)


def open_serial_with_retry(port, baud, timeout=0.1):
    global serial_conn
    while running:
        try:
            log(f"[SYS] 嘗試開啟 {port} @ {baud} ...")
            serial_conn = serial.Serial(port, baudrate=baud, timeout=timeout, write_timeout=1)
            log(f"[SYS] 成功開啟 {port} @ {baud}")
            return True
        except Exception as e:
            log(f"[ERR] 無法開啟 {port}: {e}，2秒後重試...")
            time.sleep(2)
    return False


def main():
    parser = argparse.ArgumentParser(description="Serial Broadcast Bridge - 人/AI 共用 COM port")
    parser.add_argument("--port", "-p", required=True, help="COM port, 例: COM3 或 /dev/ttyUSB0")
    parser.add_argument("--baud", "-b", type=int, required=True, help="Baudrate, 例: 115200, 9600")
    parser.add_argument("--tcp", "-t", type=int, default=7001, help="TCP 監聽埠 (預設 7001)")
    parser.add_argument("--host", default="0.0.0.0", help="TCP 綁定 IP (預設 0.0.0.0, 只允許本機用 127.0.0.1)")
    parser.add_argument("--log", default=None, help="同時寫 log 到檔案, 例: serial.log")
    parser.add_argument("--bytesize", type=int, default=8, choices=[5,6,7,8])
    parser.add_argument("--parity", default="N", choices=["N","E","O"])
    parser.add_argument("--stopbits", type=float, default=1, choices=[1,1.5,2])
    args = parser.parse_args()

    # 選用 log 檔案
    if args.log:
        import logging
        # 同時 tee 到檔案
        class Tee:
            def __init__(self, *files): self.files = files
            def write(self, obj):
                for f in self.files: f.write(obj); f.flush()
            def flush(self):
                for f in self.files: f.flush()
        log_file = open(args.log, "a", encoding="utf-8")
        sys.stdout = Tee(sys.stdout, log_file)
        sys.stderr = Tee(sys.stderr, log_file)
        log(f"[SYS] Log 同時寫入 {args.log}")

    print("="*60)
    print(f" Serial Broadcast Bridge")
    print(f"  Serial : {args.port} @ {args.baud}")
    print(f"  TCP    : {args.host}:{args.tcp}")
    print(f"  人用 PuTTY/MobaXterm 連 Telnet/Raw -> {args.host}:{args.tcp}")
    print(f"  AI 用 nc / telnet / python socket 連同一個位址")
    print(f"  雙方操作會互相廣播，Device 回應也會廣播給所有人")
    print("="*60)

    if not open_serial_with_retry(args.port, args.baud):
        sys.exit(1)

    # 覆蓋 serial 參數
    try:
        with serial_lock:
            serial_conn.bytesize = args.bytesize
            serial_conn.parity = args.parity
            serial_conn.stopbits = args.stopbits
    except:
        pass

    # 啟動 serial 讀線程
    threading.Thread(target=serial_reader, daemon=True).start()

    # 啟動 TCP server
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        srv.bind((args.host, args.tcp))
    except Exception as e:
        log(f"[ERR] 無法綁定 {args.host}:{args.tcp} - {e}")
        sys.exit(1)
    srv.listen(10)
    log(f"[SYS] TCP 等待連線中 {args.host}:{args.tcp} ... (人/AI 同時連此位址)")

    try:
        while True:
            conn, addr = srv.accept()
            with clients_lock:
                clients.append((conn, addr))
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
    except KeyboardInterrupt:
        global running
        running = False
        log("[SYS] 收到 Ctrl+C，關閉中...")
    finally:
        srv.close()
        if serial_conn and serial_conn.is_open:
            serial_conn.close()
        log("[SYS] 已關閉")


if __name__ == "__main__":
    main()
