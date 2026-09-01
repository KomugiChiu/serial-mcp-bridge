"""Entry point to launch the Serial Broadcast Bridge MCP Server.

載入並執行專案根的 `serial_mcp_bridge.py`。

路徑來源（依優先序）：
  1. 環境變數 SERIAL_MCP_BRIDGE_PATH（recommended，由 opencode MCP 的 env 提供）
  2. 內建預設路徑 DEFAULT_BRIDGE_PATH（見下方常數，可依安裝位置調整）
"""
from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

# 當 SERIAL_MCP_BRIDGE_PATH 未設定時使用的預設 script 路徑。
# 可依實際安裝位置修改，或改用環境變數覆寫。
DEFAULT_BRIDGE_PATH = Path("~/Downloads/komugi/compart_bridge/serial_mcp_bridge.py").expanduser()


def main() -> None:
    override = os.environ.get("SERIAL_MCP_BRIDGE_PATH")
    bridge = Path(override) if override else DEFAULT_BRIDGE_PATH

    if not bridge.exists():
        print(
            f"[ERROR] 找不到 serial_mcp_bridge.py: {bridge}\n"
            "請設定環境變數 SERIAL_MCP_BRIDGE_PATH 指向正確路徑，"
            "或調整本程式的 DEFAULT_BRIDGE_PATH。",
            file=sys.stderr,
        )
        sys.exit(1)

    spec = importlib.util.spec_from_file_location("serial_mcp_bridge_main", bridge)
    if spec is None or spec.loader is None:
        print(f"[ERROR] 無法載入 {bridge}", file=sys.stderr)
        sys.exit(1)

    module = importlib.util.module_from_spec(spec)
    sys.modules["serial_mcp_bridge_main"] = module
    spec.loader.exec_module(module)  # 定義 main()（不觸發 __main__ guard）

    # 觸發 script 的 main()，argparse 會吃 sys.argv
    module.main()


if __name__ == "__main__":
    main()