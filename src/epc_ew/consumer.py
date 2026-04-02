from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

import httpx


API_BASE_URL = "https://epc.opendatacommunities.org"
DOMESTIC_SEARCH_PATH = "/api/v1/domestic/search"
MAX_PAGE_SIZE = 5000

_NUM = re.compile(r"^\d+$")


@dataclass(frozen=True)
class ResumeState:
    output_tmp: str
    total_uprns: int
    uprn_hash: str
    completed_batches: list[int]
    batch_size: int


def _eprint(msg: str) -> None:
    print(msg, file=sys.stderr)


def _debug() -> bool:
    return os.environ.get("EPC_EW_DEBUG", "").strip() not in ("", "0", "false", "False")


def _sha(uprns: list[str]) -> str:
    return hashlib.sha256("\n".join(sorted(uprns)).encode("utf-8")).hexdigest()


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _chunks(items: list[str], n: int) -> list[list[str]]:
    return [items[i : i + n] for i in range(0, len(items), n)]


def load_uprns(file: Optional[Path], uprns: list[str]) -> list[str]:
    if (file is None and not uprns) or (file is not None and uprns):
        raise ValueError("Exactly one of --file or --uprns must be supplied.")

    def norm(v: str) -> str | None:
        v = v.strip().lstrip("\ufeff")
        return v if v and _NUM.match(v) else None

    if file is None:
        parsed: list[str] = []
        for u in uprns:
            n = norm(u)
            if n is None:
                raise ValueError(f"Invalid UPRN: {u!r} (must be numeric)")
            parsed.append(n)
        parsed = _dedupe(parsed)
        if not parsed:
            raise ValueError("No UPRNs supplied.")
        return parsed

    p = Path(file)
    if not p.exists():
        raise ValueError(f"File does not exist: {p}")
    with p.open("r", newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        if r.fieldnames is None:
            raise ValueError(f"CSV has no header row: {p}")
        names = list(r.fieldnames)
        m = {n.lower(): n for n in names}
        col = m.get("uprn") or next((n for n in names if "uprn" in n.lower()), None)
        if col is None:
            raise ValueError(f"Could not find a UPRN column in {p}.")
        collected: list[str] = []
        for row in r:
            raw = (row.get(col) or "").strip()
            if not raw:
                continue
            n = norm(raw)
            if n is None:
                raise ValueError(f"Invalid UPRN value in CSV column {col!r}: {raw!r}")
            collected.append(n)
    collected = _dedupe(collected)
    if not collected:
        raise ValueError(f"No UPRNs found in CSV: {p}")
    return collected


def _read_state(path: Path) -> ResumeState | None:
    if not path.exists():
        return None
    d = json.loads(path.read_text(encoding="utf-8"))
    return ResumeState(
        output_tmp=d["output_tmp"],
        total_uprns=int(d["total_uprns"]),
        uprn_hash=str(d["uprn_hash"]),
        completed_batches=list(map(int, d.get("completed_batches", []))),
        batch_size=int(d["batch_size"]),
    )


def _write_state(path: Path, s: ResumeState) -> None:
    path.write_text(json.dumps(asdict(s), indent=2, sort_keys=True), encoding="utf-8")


def out_paths(output: Path, *, overwrite: bool) -> tuple[Path, Path, Path]:
    out = Path(output)
    if out.suffix.lower() not in (".csv", ".parquet"):
        raise ValueError("Output must end with .csv or .parquet")
    resume = out.with_suffix(out.suffix + ".resume")
    tmp = Path(str(out) + ".tmp")
    if out.exists() and not overwrite:
        raise FileExistsError(str(out))
    if overwrite:
        if out.exists():
            out.unlink()
        if resume.exists():
            resume.unlink()
        if tmp.exists():
            tmp.unlink()
    if out.parent and not out.parent.exists():
        out.parent.mkdir(parents=True, exist_ok=True)
    return out, tmp, resume


def fetch_page(
    client: httpx.Client,
    *,
    token: str,
    uprns: list[str],
    page_size: int,
    search_after: str | None,
) -> tuple[str, str | None]:
    h = {"Accept": "text/csv", "Authorization": f"Basic {token}"}
    p: list[tuple[str, str | int | float | bool | None]] = [("size", page_size)]
    p += [("uprn", u) for u in uprns]
    if search_after is not None:
        p.append(("search-after", search_after))
    qp = httpx.QueryParams(p)
    if _debug():
        _eprint(f"GET {API_BASE_URL}{DOMESTIC_SEARCH_PATH}?{qp}")
    backoffs = (1, 2, 4)
    last: Exception | None = None
    for i in range(len(backoffs) + 1):
        try:
            r = client.get(DOMESTIC_SEARCH_PATH, params=qp, headers=h)
            if r.status_code in (401, 403):
                raise PermissionError(f"HTTP {r.status_code}")
            if 400 <= r.status_code < 500:
                raise ValueError(f"HTTP {r.status_code} error: {r.text}")
            if r.status_code >= 500:
                raise httpx.HTTPStatusError("server error", request=r.request, response=r)
            nxt = r.headers.get("X-Next-Search-After")
            if _debug():
                _eprint(f"-> {r.status_code} bytes={len(r.text)} next={nxt!r}")
            return r.text, nxt
        except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError, httpx.HTTPStatusError) as exc:
            last = exc
            if i >= len(backoffs):
                break
            time.sleep(backoffs[i])
    raise RuntimeError(f"Network/server error after retries: {last}")


def fetch_all_for_batch(
    client: httpx.Client,
    *,
    token: str,
    uprns: list[str],
    page_size: int,
) -> list[str]:
    out: list[str] = []
    first, sa, n = True, None, 0
    while first or sa is not None:
        txt, sa = fetch_page(
            client, token=token, uprns=uprns, page_size=page_size, search_after=None if first else sa
        )
        if not txt.strip():
            break
        out.append(txt)
        n += 1
        if _debug():
            _eprint(f"page {n}: got {len(txt)} chars; continuing={sa is not None}")
        first = False
    return out


def _split_header(txt: str) -> tuple[str, str]:
    if not txt:
        return "", ""
    lines = txt.splitlines(True)
    return (lines[0], "".join(lines[1:])) if lines else ("", "")


def run_batches(
    *,
    token: str,
    uprns: list[str],
    output_tmp: Path,
    resume_path: Path,
    batch_size: int,
    page_size: int,
    overwrite: bool,
) -> None:
    h = _sha(uprns)
    batches = _chunks(uprns, batch_size)
    st = _read_state(resume_path)
    done: set[int] = set()
    if st is not None and not overwrite:
        if st.uprn_hash != h or st.batch_size != batch_size:
            _eprint("Resume state exists but input changed; aborting.")
            raise RuntimeError("Resume state exists but input changed.")
        done = set(st.completed_batches)
        _eprint(f"Resuming: {len(done)}/{len(batches)} batches already complete.")
        output_tmp = Path(st.output_tmp)
    output_tmp.parent.mkdir(parents=True, exist_ok=True)
    write_header = not output_tmp.exists() or output_tmp.stat().st_size == 0
    with httpx.Client(base_url=API_BASE_URL, timeout=httpx.Timeout(60.0), follow_redirects=True) as c:
        with output_tmp.open("a", encoding="utf-8", newline="") as f:
            wrote = False
            for i, b in enumerate(batches):
                if i in done:
                    continue
                first, any_rows = True, False
                for txt in fetch_all_for_batch(c, token=token, uprns=b, page_size=page_size):
                    if first:
                        head, body = _split_header(txt)
                        if write_header and not wrote:
                            f.write(head)
                            wrote = True
                        f.write(body)
                        first = False
                    else:
                        _, body = _split_header(txt)
                        f.write(body)
                    any_rows = any_rows or bool(txt.strip())
                if not any_rows:
                    _eprint(f"Batch {i}: empty result set; continuing.")
                done.add(i)
                _write_state(
                    resume_path,
                    ResumeState(
                        output_tmp=str(output_tmp),
                        total_uprns=len(uprns),
                        uprn_hash=h,
                        completed_batches=sorted(done),
                        batch_size=batch_size,
                    ),
                )


def finalise_output(output_tmp: Path, output: Path, resume_path: Path) -> None:
    if not output_tmp.exists():
        raise FileNotFoundError(str(output_tmp))
    if output.suffix.lower() == ".csv":
        if output.exists():
            output.unlink()
        output_tmp.replace(output)
        if resume_path.exists():
            resume_path.unlink()
        return
    if output.suffix.lower() != ".parquet":
        raise ValueError("Output must end with .csv or .parquet")
    try:
        import duckdb
        duckdb.read_csv(str(output_tmp)).write_parquet(str(output))
    except Exception as exc:
        if output.exists():
            output.unlink()
        print(str(exc), file=sys.stderr)
        print(str(output_tmp), file=sys.stderr)
        raise RuntimeError(str(exc))
    output_tmp.unlink(missing_ok=True)
    if resume_path.exists():
        resume_path.unlink()

