from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from flask import Flask, render_template, request

from epc_ew import EpcEwClient


@dataclass
class PageModel:
    uprns_text: str
    token_missing: bool
    error: str | None
    results: dict[str, list[dict[str, Any]]] | None
    missing: list[str] | None


def _parse_uprns(text: str) -> list[str]:
    out: list[str] = []
    for line in text.splitlines():
        v = line.strip().lstrip("\ufeff")
        if not v:
            continue
        out.append(v)
    return out


def create_app() -> Flask:
    app = Flask(__name__)

    @app.get("/")
    def index_get():
        return render_template(
            "index.html",
            m=PageModel(
                uprns_text="",
                token_missing=not bool(os.environ.get("EPC_API_ENGLAND_WALES_TOKEN", "").strip()),
                error=None,
                results=None,
                missing=None,
            ),
        )

    @app.post("/")
    def index_post():
        uprns_text = (request.form.get("uprns") or "").strip()
        uprns = _parse_uprns(uprns_text)

        if not uprns:
            return render_template(
                "index.html",
                m=PageModel(
                    uprns_text=uprns_text,
                    token_missing=not bool(os.environ.get("EPC_API_ENGLAND_WALES_TOKEN", "").strip()),
                    error="No UPRNs provided. Paste one UPRN per line.",
                    results=None,
                    missing=None,
                ),
            )

        try:
            client = EpcEwClient()
            results = client.get_epc_by_uprn(uprns)
            missing = sorted([u for u, rows in results.items() if not rows])
            return render_template(
                "index.html",
                m=PageModel(
                    uprns_text=uprns_text,
                    token_missing=False,
                    error=None,
                    results=results,
                    missing=missing,
                ),
            )
        except Exception as exc:
            return render_template(
                "index.html",
                m=PageModel(
                    uprns_text=uprns_text,
                    token_missing=not bool(os.environ.get("EPC_API_ENGLAND_WALES_TOKEN", "").strip()),
                    error=str(exc),
                    results=None,
                    missing=None,
                ),
            )

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host="127.0.0.1", port=5000, debug=True)

