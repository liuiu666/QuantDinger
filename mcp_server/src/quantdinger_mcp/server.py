"""
QuantDinger MCP server — exposes the Agent Gateway as MCP tools.

This is intentionally a thin wrapper:
  * REST stays the source of truth (`/api/agent/v1`).
  * Only Read-class (R) and Backtest-class (B) tools are exposed.
  * The user-supplied agent token's scopes still gate every call server-side.

If you want to expose more (e.g. trading), prefer issuing a token with the
right scopes and keep this server unchanged — that way the security boundary
stays in the Gateway, not in the MCP layer.
"""
from __future__ import annotations

import os
import sys
from typing import Any

import httpx
from mcp.server.fastmcp import FastMCP


def _env(name: str, required: bool = True) -> str:
    value = (os.environ.get(name) or "").strip()
    if not value and required:
        print(
            f"[quantdinger-mcp] missing required env var: {name}",
            file=sys.stderr,
        )
        sys.exit(2)
    return value


BASE_URL = _env("QUANTDINGER_BASE_URL").rstrip("/")
AGENT_TOKEN = _env("QUANTDINGER_AGENT_TOKEN")
TIMEOUT_S = float(os.environ.get("QUANTDINGER_TIMEOUT_S", "60"))


_client = httpx.Client(
    base_url=BASE_URL,
    timeout=TIMEOUT_S,
    headers={"Authorization": f"Bearer {AGENT_TOKEN}"},
)


def _get(path: str, params: dict | None = None) -> Any:
    r = _client.get(path, params=params or {})
    return _unwrap(r)


def _post(path: str, json: dict | None = None, headers: dict | None = None) -> Any:
    r = _client.post(path, json=json or {}, headers=headers or {})
    return _unwrap(r)


def _unwrap(r: httpx.Response) -> Any:
    try:
        body = r.json()
    except Exception:
        return {
            "error": True,
            "status": r.status_code,
            "text": r.text[:2000],
        }
    if r.status_code >= 400:
        return {
            "error": True,
            "status": r.status_code,
            "body": body,
        }
    if isinstance(body, dict) and "data" in body:
        return body["data"]
    return body


mcp = FastMCP(
    "quantdinger",
    instructions=(
        "Tools for the QuantDinger self-hosted quant platform. "
        "All tools are tenant-scoped via the configured agent token. "
        "Trading is intentionally NOT exposed via MCP; use the REST API for that."
    ),
)


# ───────────────────────────── Read-class tools ─────────────────────────────

@mcp.tool()
def whoami() -> Any:
    """Return the calling token's identity, scopes, and allowlists."""
    return _get("/api/agent/v1/whoami")


@mcp.tool()
def list_markets() -> Any:
    """List markets the configured token is allowed to query."""
    return _get("/api/agent/v1/markets")


@mcp.tool()
def search_symbols(market: str, keyword: str = "", limit: int = 20) -> Any:
    """Find symbols in a market.

    Args:
        market: Market id, e.g. "Crypto", "USStock", "Forex".
        keyword: Substring/code; empty returns hot symbols.
        limit:   1..100, default 20.
    """
    return _get(
        f"/api/agent/v1/markets/{market}/symbols",
        params={"keyword": keyword, "limit": limit},
    )


@mcp.tool()
def get_klines(
    market: str,
    symbol: str,
    timeframe: str = "1D",
    limit: int = 300,
    before_time: int | None = None,
) -> Any:
    """OHLCV bars.

    Args:
        market:      e.g. "Crypto"
        symbol:      e.g. "BTC/USDT"
        timeframe:   "1m"/"5m"/"15m"/"30m"/"1H"/"4H"/"1D"/"1W"
        limit:       1..2000
        before_time: unix seconds; for paging older bars.
    """
    params = {"market": market, "symbol": symbol, "timeframe": timeframe, "limit": limit}
    if before_time is not None:
        params["before_time"] = int(before_time)
    return _get("/api/agent/v1/klines", params=params)


@mcp.tool()
def get_price(market: str, symbol: str) -> Any:
    """Latest price for a symbol."""
    return _get("/api/agent/v1/price", params={"market": market, "symbol": symbol})


@mcp.tool()
def list_strategies(limit: int = 50) -> Any:
    """List the tenant's strategies (compact projection)."""
    return _get("/api/agent/v1/strategies", params={"limit": limit})


@mcp.tool()
def get_strategy(strategy_id: int) -> Any:
    """Get a strategy by id (tenant-scoped)."""
    return _get(f"/api/agent/v1/strategies/{int(strategy_id)}")


@mcp.tool()
def get_job(job_id: str) -> Any:
    """Poll a previously-submitted backtest / experiment job."""
    return _get(f"/api/agent/v1/jobs/{job_id}")


@mcp.tool()
def list_jobs(kind: str | None = None, limit: int = 50) -> Any:
    """List recent jobs for this tenant. Optional `kind` filter."""
    params: dict[str, Any] = {"limit": limit}
    if kind:
        params["kind"] = kind
    return _get("/api/agent/v1/jobs", params=params)


# ───────────────────────────── Backtest-class tools ─────────────────────────────

@mcp.tool()
def submit_backtest(
    code: str,
    market: str,
    symbol: str,
    timeframe: str,
    start_date: str,
    end_date: str,
    initial_capital: float = 10000,
    commission: float = 0.001,
    slippage: float = 0.0,
    leverage: int = 1,
    trade_direction: str = "long",
    idempotency_key: str | None = None,
) -> Any:
    """Submit a backtest. Returns `{job_id, status, ...}` — poll with `get_job`.

    Args:
        code:           Indicator code (Python).
        market/symbol/timeframe: Series identification.
        start_date/end_date:     YYYY-MM-DD.
        initial_capital, commission, slippage, leverage, trade_direction:
                       standard backtest knobs.
        idempotency_key: optional; repeat calls with the same key return the
                         original job instead of submitting a duplicate.
    """
    import re
    risk_cfg = {}
    pos_cfg = {}
    
    # Extract config from comments, e.g. # @strategy stopLossPct 0.02
    for match in re.finditer(r"#\s*@strategy\s+([a-zA-Z0-9_]+)\s+([0-9a-zA-Z._-]+)", code):
        key = match.group(1)
        val_str = match.group(2)
        if val_str.lower() == "true":
            val = True
        elif val_str.lower() == "false":
            val = False
        elif "." in val_str:
            try:
                val = float(val_str)
            except ValueError:
                val = val_str
        else:
            try:
                val = int(val_str)
            except ValueError:
                val = val_str
                
        if key in ("stopLossPct", "takeProfitPct"):
            risk_cfg[key] = val
        elif key in ("entryPct",):
            pos_cfg[key] = val

    payload = {
        "code": code,
        "market": market,
        "symbol": symbol,
        "timeframe": timeframe,
        "start_date": start_date,
        "end_date": end_date,
        "initial_capital": initial_capital,
        "commission": commission,
        "slippage": slippage,
        "leverage": leverage,
        "trade_direction": trade_direction,
    }
    
    if risk_cfg or pos_cfg:
        payload["strategy_config"] = {
            "risk": risk_cfg,
            "position": pos_cfg
        }

    headers = {"Idempotency-Key": idempotency_key} if idempotency_key else None
    return _post("/api/agent/v1/backtests", json=payload, headers=headers)


@mcp.tool()
def regime_detect(
    market: str,
    symbol: str,
    timeframe: str,
    start_date: str,
    end_date: str,
) -> Any:
    """Detect the current market regime (synchronous)."""
    return _post(
        "/api/agent/v1/experiments/regime/detect",
        json={
            "market": market, "symbol": symbol, "timeframe": timeframe,
            "startDate": start_date, "endDate": end_date,
        },
    )


@mcp.tool()
def submit_structured_tune(payload: dict) -> Any:
    """Submit a grid/random tuning job. Returns a job for polling.

    `payload` should include `base` (a backtest spec) and either `parameterSpace`
    (grid) or `randomTrials` (random). See `docs/AI_TRADING_SYSTEM_PLAN_CN.md`.
    """
    return _post("/api/agent/v1/experiments/structured-tune", json=payload)


_TRANSPORTS = {"stdio", "sse", "streamable-http"}


def _resolve_transport() -> str:
    raw = (os.environ.get("QUANTDINGER_MCP_TRANSPORT") or "stdio").strip().lower()
    # Accept a few obvious aliases so users don't have to look this up.
    if raw in ("http", "streaming-http", "streamable_http"):
        raw = "streamable-http"
    if raw not in _TRANSPORTS:
        print(
            f"[quantdinger-mcp] unknown transport '{raw}'. "
            f"Expected one of: {sorted(_TRANSPORTS)} (or http/streaming-http alias).",
            file=sys.stderr,
        )
        sys.exit(2)
    return raw


def _apply_http_settings_from_env() -> None:
    """Bind host/port for HTTP transports without forcing a CLI dance.

    FastMCP exposes these via its `settings` attribute. We only touch them when
    the transport is HTTP-flavored, so the stdio default stays untouched.
    """
    host = (os.environ.get("QUANTDINGER_MCP_HOST") or "").strip()
    port_raw = (os.environ.get("QUANTDINGER_MCP_PORT") or "").strip()
    settings = getattr(mcp, "settings", None)
    if settings is None:
        return
    if host:
        try:
            settings.host = host
        except Exception:
            pass
    if port_raw:
        try:
            settings.port = int(port_raw)
        except Exception:
            print(
                f"[quantdinger-mcp] invalid QUANTDINGER_MCP_PORT='{port_raw}', ignoring.",
                file=sys.stderr,
            )


def main() -> None:
    """Entrypoint.

    Transport selection (env-only — works in both desktop and cloud):
      QUANTDINGER_MCP_TRANSPORT=stdio              (default; stdin/stdout)
      QUANTDINGER_MCP_TRANSPORT=sse                (SSE over HTTP)
      QUANTDINGER_MCP_TRANSPORT=streamable-http    (newer MCP HTTP transport)
      QUANTDINGER_MCP_HOST=0.0.0.0                 (bind for HTTP transports)
      QUANTDINGER_MCP_PORT=7800                    (port for HTTP transports)
    """
    transport = _resolve_transport()
    if transport in ("sse", "streamable-http"):
        _apply_http_settings_from_env()
    mcp.run(transport=transport)


if __name__ == "__main__":
    main()
