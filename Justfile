set windows-shell := ["powershell.exe", "-NoLogo", "-Command"]

help:
  uv run epc_ew.py --help

install:
  uv sync

run file output:
  uv run epc_ew.py --file {{file}} --output {{output}}

uprns uprns output:
  uv run epc_ew.py --uprns {{uprns}} --output {{output}}

rerun file output:
  uv run epc_ew.py --file {{file}} --output {{output}} --overwrite

