from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from dotenv import load_dotenv

from epc_ew.consumer import MAX_PAGE_SIZE, finalise_output, load_uprns, out_paths, run_batches


app = typer.Typer(add_completion=False, help="Fetch EPC (England & Wales) domestic records by UPRN.")


@app.command(help="Read UPRNs, fetch EPC records, and save to a file.")
def cli(
    file: Optional[Path] = typer.Option(
        None, "--file", "-f", exists=True, dir_okay=False, readable=True, help="CSV file containing UPRNs."
    ),
    uprns: list[str] = typer.Option([], "--uprns", help="UPRNs passed directly on the command line (multiple)."),
    output: Path = typer.Option(
        ..., "--output", "-o", help="Output file; extension drives format (.csv or .parquet)."
    ),
    batch_size: int = typer.Option(50, "--batch-size", min=1, help="UPRNs per HTTP request."),
    page_size: int = typer.Option(
        5000, "--page-size", min=1, max=MAX_PAGE_SIZE, help="Records per API page (max allowed)."
    ),
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        help="Allow overwriting an existing output file (default: false).",
        show_default=True,
    ),
    token: Optional[str] = typer.Option(
        None,
        "--token",
        help="Falls back to EPC_API_ENGLAND_WALES_TOKEN.",
        envvar="EPC_API_ENGLAND_WALES_TOKEN",
        show_default=False,
    ),
) -> None:
    load_dotenv()
    if token is None or not token.strip():
        raise typer.BadParameter(
            "Missing token. Provide --token or set EPC_API_ENGLAND_WALES_TOKEN (Base64-encoded basic auth token)."
        )
    try:
        u = load_uprns(file, uprns)
        out, tmp, resume = out_paths(output, overwrite=overwrite)
        run_batches(
            token=token.strip(),
            uprns=u,
            output_tmp=tmp,
            resume_path=resume,
            batch_size=batch_size,
            page_size=page_size,
            overwrite=overwrite,
        )
        finalise_output(tmp, out, resume)
    except FileExistsError as exc:
        raise typer.BadParameter(f"Output already exists: {exc}") from exc
    except PermissionError as exc:
        import sys

        print(f"Auth error: {exc}", file=sys.stderr)
        raise typer.Exit(code=1)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except RuntimeError as exc:
        import sys

        print(str(exc), file=sys.stderr)
        raise typer.Exit(code=1)


def run() -> None:
    app()
