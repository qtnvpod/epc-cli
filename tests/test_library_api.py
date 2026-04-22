import httpx


def _resp_csv(text: str, *, next_search_after: str | None = None) -> httpx.Response:
    headers = {"Content-Type": "text/csv"}
    if next_search_after is not None:
        headers["X-Next-Search-After"] = next_search_after
    return httpx.Response(200, headers=headers, text=text)


def test_get_epc_rows_no_results_returns_empty_list() -> None:
    from epc_ew import EpcEwClient

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/domestic/search"
        # header only -> no rows
        return _resp_csv("lmk-key,uprn\n")

    client = EpcEwClient(token="dummy", transport=httpx.MockTransport(handler))
    rows = client.get_epc_as_list([100])
    assert rows == []


def test_get_epc_by_uprn_includes_missing_as_empty_list() -> None:
    from epc_ew import EpcEwClient

    def handler(request: httpx.Request) -> httpx.Response:
        return _resp_csv("lmk-key,uprn\nabc,100\n")

    client = EpcEwClient(token="dummy", transport=httpx.MockTransport(handler))
    m = client.get_epc_as_map([100, 101])
    assert set(m.keys()) == {"100", "101"}
    assert len(m["100"]) == 1
    assert m["101"] == []


def test_paging_combines_pages_and_drops_duplicate_headers() -> None:
    from epc_ew import EpcEwClient

    def handler(request: httpx.Request) -> httpx.Response:
        sa = request.url.params.get("search-after")
        if sa is None:
            return _resp_csv("lmk-key,uprn\nid1,100\n", next_search_after="NEXT")
        assert sa == "NEXT"
        return _resp_csv("lmk-key,uprn\nid2,100\n")

    client = EpcEwClient(token="dummy", transport=httpx.MockTransport(handler), page_size=1, batch_size=1)
    rows = client.get_epc_as_list([100])
    keys = [r.get("lmk-key") for r in rows]
    assert keys == ["id1", "id2"]
