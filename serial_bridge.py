#!/usr/bin/env python3
"""
DEPRECATED: legacy pure-TCP bridge, superseded by `serial_mcp_bridge.py`.

- Fixes in the new version: unified rx_buffer, history lock, echo dedup, serial_flush/status race, TCP teardown, etc.
- Kept for temporary reference only; new deployments must use `serial_mcp_bridge.py`.
- Slated for removal in a future version.

Serial Broadcast Bridge - let humans and AI share one COM port and see each other's actions
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
    print("[ERROR] pyserial required: pip install pyserial")
    sys.exit(1)

# Global state
clients = []
clients_lock = threading.Lock()
history = deque(maxlen=100)  # History replay for newcomers
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
    """Broadcast data to all TCP clients"""
    if not data:
        return
    # 1. Print to server console + history
    try:
        text = data.decode('utf-8', errors='replace').strip()
        if text:
            for line in text.splitlines():
                log(f"[{show_as}] {line}")
    except:
        log(f"[{show_as}] {data.hex()}")

    # 2. Broadcast to all TCP clients
    with clients_lock:
        dead = []
        for conn, addr in clients:
            try:
                conn.sendall(data)
            except:
                dead.append((conn, addr))
        for d in dead:
            clients.remove(d)
            log(f"[SYS] Client disconnected {d[1]}")


def handle_client(conn, addr):
    log(f"[SYS] Client connected {addr} ({len(clients)+1} total)")
    # Send history to the newcomer
    try:
        if history:
            conn.sendall(("\n".join(history) + "\n").encode('utf-8', errors='replace'))
            conn.sendall("--- Connected. Type away; AI/human actions are broadcast to each other ---\n".encode('utf-8'))
    except:
        pass

    try:
        while running:
            data = conn.recv(4096)
            if not data:
                break
            # Broadcast to the other clients, so humans see AI commands and vice versa
            text_preview = data.decode('utf-8', errors='replace').strip()
            tag = f"{addr} -> Device"
            # Relay to the others first (so everyone sees what was typed)
            with clients_lock:
                for c, a in clients:
                    if c != conn:
                        try:
                            # So the other clients see who sent it
                            c.sendall(f"[{tag}] ".encode() + data)
                        except:
                            pass
            
            # Log it too
            if text_preview:
                for line in text_preview.splitlines():
                    log(f"[{tag}] {line}")

            # Send to the hardware
            with serial_lock:
                if serial_conn and serial_conn.is_open:
                    serial_conn.write(data)
                else:
                    log("[WARN] Serial not connected, command not sent")
                    try:
                        conn.sendall("[WARN] Serial not connected\n".encode('utf-8'))
                    except:
                        pass
    except Exception as e:
        log(f"[SYS] Client {addr} error: {e}")
    finally:
        with clients_lock:
            if (conn, addr) in clients:
                clients.remove((conn, addr))
        conn.close()
        log(f"[SYS] Client offline {addr} ({len(clients)} left)")


def serial_reader():
    """Keep reading serial, broadcast to all TCP clients"""
    log("[SYS] Serial reader thread started")
    while running:
        try:
            if serial_conn and serial_conn.is_open and serial_conn.in_waiting:
                with serial_lock:
                    data = serial_conn.read(serial_conn.in_waiting or 1)
                if data:
                    broadcast_to_clients(data, show_as="Device -> ALL")
            else:
                # Some devices report in_waiting unreliably; fall back to a timeout read of 1 byte
                with serial_lock:
                    if serial_conn and serial_conn.is_open:
                        # Avoid holding the lock too long
                        pass
                time.sleep(0.01)
                # Try reading 1 byte with timeout
                try:
                    with serial_lock:
                        if serial_conn and serial_conn.is_open:
                            data = serial_conn.read(1)
                            if data:
                                # If data arrived, read the rest too
                                waiting = serial_conn.in_waiting
                                if waiting:
                                    data += serial_conn.read(waiting)
                                broadcast_to_clients(data, show_as="Device -> ALL")
                                continue
                except Exception as e:
                    # A read error may mean disconnection
                    pass
                time.sleep(0.02)
        except Exception as e:
            log(f"[ERR] Serial read error: {e}")
            time.sleep(1)


def open_serial_with_retry(port, baud, timeout=0.1):
    global serial_conn
    while running:
        try:
            log(f"[SYS] Opening {port} @ {baud} ...")
            serial_conn = serial.Serial(port, baudrate=baud, timeout=timeout, write_timeout=1)
            log(f"[SYS] Opened {port} @ {baud}")
            return True
        except Exception as e:
            log(f"[ERR] Failed to open {port}: {e}; retrying in 2s...")
            time.sleep(2)
    return False


def main():
    parser = argparse.ArgumentParser(description="Serial Broadcast Bridge - human/AI shared COM port")
    parser.add_argument("--port", "-p", required=True, help="COM port, e.g. COM3 or /dev/ttyUSB0")
    parser.add_argument("--baud", "-b", type=int, required=True, help="Baudrate, e.g. 115200, 9600")
    parser.add_argument("--tcp", "-t", type=int, default=7001, help="TCP listen port (default 7001)")
    parser.add_argument("--host", default="0.0.0.0", help="TCP bind IP (default 0.0.0.0; use 127.0.0.1 for localhost only)")
    parser.add_argument("--log", default=None, help="Also write logs to a file, e.g. serial.log")
    parser.add_argument("--bytesize", type=int, default=8, choices=[5,6,7,8])
    parser.add_argument("--parity", default="N", choices=["N","E","O"])
    parser.add_argument("--stopbits", type=float, default=1, choices=[1,1.5,2])
    args = parser.parse_args()

    # Optional log file
    if args.log:
        import logging
        # Tee to the file too
        class Tee:
            def __init__(self, *files): self.files = files
            def write(self, obj):
                for f in self.files: f.write(obj); f.flush()
            def flush(self):
                for f in self.files: f.flush()
        log_file = open(args.log, "a", encoding="utf-8")
        sys.stdout = Tee(sys.stdout, log_file)
        sys.stderr = Tee(sys.stderr, log_file)
        log(f"[SYS] Also logging to {args.log}")

    print("="*60)
    print(f" Serial Broadcast Bridge")
    print(f"  Serial : {args.port} @ {args.baud}")
    print(f"  TCP    : {args.host}:{args.tcp}")
    print(f"  Humans: PuTTY/MobaXterm via Telnet/Raw -> {args.host}:{args.tcp}")
    print(f"  AI: same address via nc / telnet / python socket")
    print(f"  Actions from both sides are broadcast; device responses go to everyone")
    print("="*60)

    if not open_serial_with_retry(args.port, args.baud):
        sys.exit(1)

    # Override serial params
    try:
        with serial_lock:
            serial_conn.bytesize = args.bytesize
            serial_conn.parity = args.parity
            serial_conn.stopbits = args.stopbits
    except:
        pass

    # Start the serial reader thread
    threading.Thread(target=serial_reader, daemon=True).start()

    # Start the TCP server
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        srv.bind((args.host, args.tcp))
    except Exception as e:
        log(f"[ERR] Cannot bind {args.host}:{args.tcp} - {e}")
        sys.exit(1)
    srv.listen(10)
    log(f"[SYS] TCP listening on {args.host}:{args.tcp} ... (humans/AI connect here together)")

    try:
        while True:
            conn, addr = srv.accept()
            with clients_lock:
                clients.append((conn, addr))
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
    except KeyboardInterrupt:
        global running
        running = False
        log("[SYS] Got Ctrl+C, shutting down...")
    finally:
        srv.close()
        if serial_conn and serial_conn.is_open:
            serial_conn.close()
        log("[SYS] Closed")


if __name__ == "__main__":
    main()
