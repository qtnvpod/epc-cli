# EPC Consumer — England & Wales

Importable Python package and CLI to fetch EPC domestic certificates by UPRN from the England & Wales EPC API and save them to `.csv` or `.parquet`.

## Install (dev)

If you use `uv`:

```bash
uv sync
```

## Auth

Set `EPC_API_ENGLAND_WALES_TOKEN` to the **Base64-encoded** HTTP Basic credential described in the EPC docs (encode `email:api_key`).

## Run

After install, the console script is available:

```bash
epc-ew --uprns 100023336956 --output out.csv
```

During development from the repo (without installing the wheel):

```bash
uv run epc_ew.py --uprns 100023336956 --output out.parquet
```
