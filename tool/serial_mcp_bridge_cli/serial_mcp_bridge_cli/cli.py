"""Entry point to launch the Serial Broadcast Bridge MCP Server.

Load and run the project-root `serial_mcp_bridge.py`.

Path sources (by priority):
  1. SERIAL_MCP_BRIDGE_PATH env var (recommended, provided by the opencode MCP env)
  2. Built-in DEFAULT_BRIDGE_PATH (constant below, adjust to the install location)
"""
from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

# Default script path used when SERIAL_MCP_BRIDGE_PATH is unset.
# Adjust to the real install location, or override via the env var.
DEFAULT_BRIDGE_PATH = Path("~/Downloads/komugi/compart_bridge/serial_mcp_bridge.py").expanduser()


def main() -> None:
    override = os.environ.get("SERIAL_MCP_BRIDGE_PATH")
    bridge = Path(override) if override else DEFAULT_BRIDGE_PATH

    if not bridge.exists():
        print(
            f"[ERROR] serial_mcp_bridge.py not found: {bridge}\n"
            "Set the SERIAL_MCP_BRIDGE_PATH env var to the right path, "
            "or adjust this program's DEFAULT_BRIDGE_PATH.",
            file=sys.stderr,
        )
        sys.exit(1)

    spec = importlib.util.spec_from_file_location("serial_mcp_bridge_main", bridge)
    if spec is None or spec.loader is None:
        print(f"[ERROR] Cannot load {bridge}", file=sys.stderr)
        sys.exit(1)

    module = importlib.util.module_from_spec(spec)
    sys.modules["serial_mcp_bridge_main"] = module
    spec.loader.exec_module(module)  # Defines main() (does not trigger the __main__ guard)

    # Invoke the script main(); argparse consumes sys.argv
    module.main()


if __name__ == "__main__":
    main()