from __future__ import annotations

import sys
from pathlib import Path


def main() -> None:
    _root = Path(__file__).resolve().parent
    sys.path.insert(0, str(_root / "src"))
    sys.path.insert(0, str(_root / "epc-ew-cli" / "src"))
    from epc_ew_cli import run

    run()


if __name__ == "__main__":
    main()
