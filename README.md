# Serial Bridge MCP Server

讓 **AI Agent** 與 **User（人）** 同時連接同一個 Serial Port，互看對方操作，Device 回應廣播給所有人。

這是 `serial_bridge.py` 的 MCP 化版本：沿用多用戶共用單一 COM port + 廣播模型的設計，
同時提供兩條連線通道。

## 兩條連線通道

| 通道 | 誰用 | 方式 |
|------|------|------|
| **MCP (stdio)** | AI Agent（例如 opencode） | opencode 以 local MCP spawn 本程式，經 stdio 交談 |
| **TCP text bridge** | User（人） | PuTTY / MobaXterm 連 telnet/raw `[tcp-host]:7001` |

雙方共用同一個 serial port；任何一方寫入或收到 Device 回應，都會：
- 記入共享歷史（AI 可用 `serial_get_history` 查）
- 廣播給 User 的 TCP 連線

> 裝置 echo 去重複：User/AI 送出指令（如 `ls`）時，bridge 只記錄一次「送出的指令」，
> 設備端 echo 回來的那份會被自動濾掉，剩餘的才是真正的執行輸出。避免 `ls` 出現兩次。

## 安裝

```bash
pip install -r requirements.txt
# 需要 Python 3.10+；切勿用 mcp>=2（API 不同，此版本未支援）
```

## 啟動

```bash
python serial_mcp_bridge.py --port /dev/ttyUSB0 --baud 115200
# 自訂埠：
python serial_mcp_bridge.py --port COM3 --baud 9600 --tcp 7001
# 啟動即自動連 serial：
python serial_mcp_bridge.py --port /dev/ttyUSB0 --baud 115200 --auto-connect
```

參數：
- `--port / -p` serial port（也可不指定，之後由 AI 用 `serial_connect` 連）
- `--baud / -b` baud rate（預設 115200）
- `--tcp / -t` User TCP/telnet 埠（預設 7001）
- `--tcphost` TCP 綁定 IP（預設 0.0.0.0）
- `--auto-connect` 啟動時自動連指定的 port
- `--no-history` 停用歷史紀錄（不記 history、新 TCP 不補歷史；即時廣播不受影響）
- `--log-file` 持久化 log 到檔案（append），例：`--log-file serial.log`
- `--line-ending {crlf,lf,cr}` 送出的換行字元（預設 `lf`；少數需要 CRLF 的設備請用 `crlf`；TCP user 輸入也會按此正規化）

> 注意：MCP 走 **stdio**，因此輸出 log 一律寫到 stderr，請勿把 stdout 拿去印其他東西（會污染 MCP JSON-RPC 通道）。

## opencode 設定（local MCP spawn，stdio）

在本機用 `uv tool` 安裝並註冊：

```bash
cd tool/serial_mcp_bridge_cli
uv tool install .
```

然後在 opencode 設定加入：

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

`SERIAL_MCP_BRIDGE_PATH` 指向 `serial_mcp_bridge.py`，這支 CLI wrapper 需要。

重啟 opencode 後，即可用 `serial_*` tools；User 同時用 telnet/raw 連 `127.0.0.1:7001`。

## AI 端使用（MCP tools）

AI 連上（stdio）後可使用以下工具：

| Tool | 說明 |
|------|------|
| `serial_connect(port, baudrate, ...)` | 連接到 Serial Port（全部 AI/User 共用） |
| `serial_disconnect()` | 斷開 Serial Port |
| `serial_write(data, encoding, append_crlf, line_ending)` | 寫入資料（broadcast 給所有 User/AI；`line_ending` 未指定跟 server `--line-ending`） |
| `serial_write_read(data, encoding, append_crlf, line_ending, wait_ms)` | 送指令後等回應（先清待讀再寫，問答式設備用） |
| `serial_read(timeout_ms)` | 讀取目前緩衝資料（統一接收緩衝，不跟讀取線程搶 port） |
| `serial_readline(timeout_ms)` | 讀取一行（直到換行） |
| `serial_status()` | 查看連線狀態 |
| `serial_list_ports()` | 列出可用 Serial Ports |
| `serial_get_history(lines)` | 取得共享歷史（看 User/AI/Device 操作） |
| `serial_search_history(keyword, source, lines, limit)` | 搜尋歷史（按關鍵字/來源 AI/User/Device/SYS 過濾） |
| `serial_flush(which)` | 清除緩衝區 |

### 建議的 AI 操作流程

1. `serial_list_ports()` 找 port
2. `serial_connect(port=..., baudrate=...)` 連線
3. `serial_write(data="...")` 送指令
4. `serial_read(timeout_ms=...)` 或 `serial_get_history()` 看 Device 回應
5. 若要「持續看輸出」，可迴圈呼叫 `serial_read` / `serial_get_history`

> 注意：本 server 的 MCP SDK 需使用 **mcp 1.x**（`pip install "mcp<2"`）。