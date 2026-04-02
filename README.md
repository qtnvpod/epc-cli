# EPC Consumer — England & Wales

Importable Python package (`epc-ew`) to fetch EPC domestic certificates by UPRN from the England & Wales EPC API and save them to `.csv` or `.parquet`. The Typer CLI is a separate installable package (`epc-ew-cli`) so library-only users do not need Typer or python-dotenv.

## Install (dev)

If you use `uv`:

```bash
uv sync
```

This workspace installs `epc-ew` (library) and `epc-ew-cli` (console entry `epc-ew`) by default via the `dev` dependency group.

- **Library only** (no CLI): `pip install epc-ew` — dependencies are `httpx` and `duckdb` (DuckDB is only imported for `.parquet` output).
- **CLI**: `pip install epc-ew-cli` — depends on `epc-ew`, Typer, and python-dotenv.

## Auth

Set `EPC_API_ENGLAND_WALES_TOKEN` to the **Base64-encoded** HTTP Basic credential described in the EPC docs (encode `email:api_key`).

## Run

After installing the CLI package:

```bash
epc-ew --uprns 100023336956 --output out.csv
```

From this repo (editable workspace, no wheel install required):

```bash
uv run run_epc_ew.py --uprns 100023336956 --output out.parquet
```

Or:

```bash
uv run python -m epc_ew_cli --uprns 100023336956 --output out.parquet
```

## Import the library

```python
from epc_ew import load_uprns, run_batches, finalise_output
```
