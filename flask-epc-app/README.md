## Flask EPC demo app (uses `epc_ew`)

### Setup
- Ensure the library is installed (from repo root):

```bash
uv pip install -e .
```

- Install Flask for this demo app:

```bash
uv pip install -r flask-epc-app/requirements.txt
```

### Auth
Set the EPC token env var (Base64-encoded `email:api_key`):

```powershell
$env:EPC_API_ENGLAND_WALES_TOKEN = "<base64 token>"
```

### Run

```bash
uv run python flask-epc-app/app.py
```

Then open `http://127.0.0.1:5000`.

