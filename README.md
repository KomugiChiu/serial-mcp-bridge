# Serial Bridge MCP Server

**English** | [繁體中文](README.zh-TW.md)

Let an **AI agent** and a **user (human)** share the same serial port at the same time: each side sees what the other does, and device responses are broadcast to everyone.

This is the MCP-ified version of `serial_bridge.py`: it keeps the multi-user single-COM-port + broadcast design while offering two connection channels.

## Two connection channels

| Channel | Who | How |
|---------|-----|-----|
| **MCP (stdio)** | AI agent (e.g. opencode) | opencode spawns this program as a local MCP and talks over stdio |
| **TCP text bridge** | User (human) | PuTTY / MobaXterm via telnet/raw to `[tcp-host]:7001` |

Both sides share one serial port; whenever either side writes or the device responds:
- it is recorded in the shared history (the AI can query it with `serial_get_history`)
- it is broadcast to the user's TCP connection

> Device echo dedup: when the user/AI sends a command (e.g. `ls`), the bridge records the "sent command" only once; the copy echoed back by the device is filtered out automatically, leaving only the real output. No more duplicated `ls`.

### User in-band commands (TCP)

Lines starting with `!` at the beginning of a line are intercepted by the bridge (never sent to the device), even when typed character-by-character over telnet:

| Input | Effect |
|-------|--------|
| `!start_log [name]` | Start writing all input/output to a log file (`--log-dir`, auto-named if omitted) |
| `!stop_log` | Stop recording |
| `!log_status` | Show recording status |
| `!help` | Show this list |
| `!!...` | Escape: sent to the device literally (`!!ls` sends `!ls`) |

Start/stop events are logged to history and announced to the other side, so no one records secretly. Unknown `!xxx` is forwarded to the device untouched (plus a private hint).

## Install

```bash
pip install -r requirements.txt
# Requires Python 3.10+; do NOT use mcp>=2 (different API, not supported here)
```

## Start

```bash
python serial_mcp_bridge.py --port /dev/ttyUSB0 --baud 115200
# Custom port:
python serial_mcp_bridge.py --port COM3 --baud 9600 --tcp 7001
# Auto-connect serial on startup:
python serial_mcp_bridge.py --port /dev/ttyUSB0 --baud 115200 --auto-connect
```

Options:
- `--port / -p` serial port (optional; the AI can connect later with `serial_connect`)
- `--baud / -b` baud rate (default 115200)
- `--tcp / -t` user TCP/telnet port (default 7001)
- `--tcphost` TCP bind IP (default 127.0.0.1, localhost only; `0.0.0.0` exposes it to the LAN where anyone could inject serial commands)
- `--auto-connect` connect the given serial port on startup
- `--no-history` disable history: no history recording, no history replay for new TCP clients; live broadcast unaffected
- `--no-tcp-history` skip history replay for new TCP clients; history is still recorded and the AI's `serial_get_history`/`search` keep working (live broadcast unaffected)
- `--log-file` persist logs to a file (append), e.g. `--log-file serial.log`
- `--log-dir` directory for files started via `!start_log` / `serial_log_start` (auto-created, default `logs`; names are basenamed)
- `--line-ending {crlf,lf,cr}` line ending appended on send (default `lf`; use `crlf` for devices that need CRLF; TCP user input is normalized the same way)
- `--cooked` bridge-side line editing (local echo, Up/Down history recall; for dumb shells with no line editing; default is raw passthrough)

> Note: MCP runs over **stdio**, so all logs go to stderr. Never print anything else to stdout (it would corrupt the MCP JSON-RPC channel).

## opencode setup (local MCP spawn, stdio)

Install and register locally with `uv tool`:

```bash
cd tool/serial_mcp_bridge_cli
uv tool install .
```

Then add to your opencode config:

```json
{
  "mcp": {
    "serial_bridge": {
      "type": "local",
      "command": ["/home/user/.local/bin/serial-mcp-bridge", "--tcp", "7001"],
      "enabled": true,
      "environment": {
        "SERIAL_MCP_BRIDGE_PATH": "/abs/path/to/serial_mcp_bridge.py"
      }
    }
  }
}
```

`SERIAL_MCP_BRIDGE_PATH` points to `serial_mcp_bridge.py`; the CLI wrapper needs it.

After restarting opencode, the `serial_*` tools are available; the user connects simultaneously via telnet/raw to `127.0.0.1:7001`.

> The legacy `serial_bridge.py` is deprecated and kept for reference only; always use `serial_mcp_bridge.py`.

## AI usage (MCP tools)

Once connected (stdio), the AI can use these tools:

| Tool | Description |
|------|-------------|
| `serial_connect(port, baudrate, ...)` | Connect to the serial port (shared by all AIs/users) |
| `serial_disconnect()` | Disconnect the serial port |
| `serial_write(data, encoding, append_crlf, line_ending)` | Write data (broadcast to all users/AIs; `line_ending` defaults to the server's `--line-ending`) |
| `serial_write_read(data, encoding, append_crlf, line_ending, wait_ms)` | Send a command and wait for the response (drains stale input first; for Q&A-style devices) |
| `serial_read(timeout_ms)` | Read buffered device responses (unified receive buffer; never races the reader thread for the port) |
| `serial_readline(timeout_ms)` | Read one line (up to newline) |
| `serial_status()` | Show connection status |
| `serial_list_ports()` | List available serial ports |
| `serial_get_history(lines)` | Get recent shared history (user / AI / device activity) |
| `serial_search_history(keyword, source, lines, limit)` | Search history (filter by keyword / source AI/User/Device/SYS) |
| `serial_flush(which)` | Flush buffers |
| `serial_log_start(filename)` | Start writing all input/output to a log file (shared switch with `!start_log`) |
| `serial_log_stop()` | Stop recording |

### Suggested AI workflow

1. `serial_list_ports()` to find the port
2. `serial_connect(port=..., baudrate=...)` to connect
3. `serial_write(data="...")` to send a command
4. `serial_read(timeout_ms=...)` or `serial_get_history()` to see the device response
5. To keep watching output, poll `serial_read` / `serial_get_history`

> Note: this server requires MCP SDK **1.x** (`pip install "mcp<2"`).
