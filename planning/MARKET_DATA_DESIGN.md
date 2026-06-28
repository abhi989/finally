# Market Data Backend — Detailed Design

Implementation-ready design for the FinAlly market data subsystem. It covers the
unified `MarketDataSource` interface, the in-memory `PriceCache`, the GBM
simulator (default), the Massive (Polygon.io) API client (optional), the SSE
streaming endpoint, and FastAPI lifecycle integration.

Everything in this document lives under `backend/app/market/`. The code snippets
here match the shipped implementation in that package.

> **Status note.** This subsystem is already built, tested (73 tests), and
> reviewed — see `planning/MARKET_DATA_SUMMARY.md`. This document is the
> standalone design reference describing how it works and why.

---

## Table of Contents

1. [Design Goals](#1-design-goals)
2. [File Structure](#2-file-structure)
3. [Data Model — `models.py`](#3-data-model--modelspy)
4. [Price Cache — `cache.py`](#4-price-cache--cachepy)
5. [Abstract Interface — `interface.py`](#5-abstract-interface--interfacepy)
6. [Seed Prices & Ticker Parameters — `seed_prices.py`](#6-seed-prices--ticker-parameters--seed_pricespy)
7. [GBM Simulator — `simulator.py`](#7-gbm-simulator--simulatorpy)
8. [Massive API Client — `massive_client.py`](#8-massive-api-client--massive_clientpy)
9. [Factory — `factory.py`](#9-factory--factorypy)
10. [SSE Streaming Endpoint — `stream.py`](#10-sse-streaming-endpoint--streampy)
11. [Package Public API — `__init__.py`](#11-package-public-api--__init__py)
12. [FastAPI Lifecycle Integration](#12-fastapi-lifecycle-integration)
13. [Watchlist Coordination](#13-watchlist-coordination)
14. [Data Flow Walkthroughs](#14-data-flow-walkthroughs)
15. [Testing Strategy](#15-testing-strategy)
16. [Error Handling & Edge Cases](#16-error-handling--edge-cases)
17. [Configuration Summary](#17-configuration-summary)

---

## 1. Design Goals

The market data layer has one job: keep a fresh price for every tracked ticker
available to the rest of the backend, regardless of where that price comes from.
The design is shaped by five goals:

| Goal | How it's met |
|------|--------------|
| **Source-agnostic consumers** | Everything downstream reads from a single `PriceCache`; it never knows whether prices come from the simulator or a real API. |
| **Hot-swappable providers** | Simulator and Massive both implement one ABC (`MarketDataSource`); a factory picks one at startup from an env var. |
| **Zero external deps by default** | The simulator path needs only `numpy`; no API key, no network. Students can run the whole app offline. |
| **Always-fresh, never-blank** | The cache is seeded synchronously at `start()`, so the SSE stream has data on its very first push. |
| **Resilient background tasks** | Both producers catch per-cycle exceptions and keep running; a single bad tick or failed poll never kills the feed. |

The architecture is a textbook **producer/consumer with a shared buffer**:

```
        ┌──────────────────────┐
        │  MarketDataSource    │   (one of two implementations)
        │  ───────────────     │
        │  SimulatorDataSource │  ── GBM, ticks every 0.5s
        │  MassiveDataSource   │  ── REST poll every 15s
        └──────────┬───────────┘
                   │ writes
                   ▼
            ┌──────────────┐
            │  PriceCache  │   thread-safe, in-memory, single source of truth
            └──────┬───────┘
                   │ reads
       ┌───────────┼─────────────┐
       ▼           ▼             ▼
   SSE stream   trade exec   portfolio valuation
   (/api/stream/prices)
```

---

## 2. File Structure

```
backend/
  app/
    market/
      __init__.py          # Public API re-exports
      models.py            # PriceUpdate dataclass
      cache.py             # PriceCache (thread-safe in-memory store)
      interface.py         # MarketDataSource ABC
      seed_prices.py       # SEED_PRICES, TICKER_PARAMS, DEFAULT_PARAMS, correlation consts
      simulator.py         # GBMSimulator + SimulatorDataSource
      massive_client.py    # MassiveDataSource
      factory.py           # create_market_data_source()
      stream.py            # SSE endpoint (FastAPI router factory)
  tests/
    market/                # mirrors the modules above (see §15)
```

Each module has a single responsibility. `__init__.py` re-exports the public API
so the rest of the backend imports from `app.market` without reaching into
submodules.

---

## 3. Data Model — `models.py`

`PriceUpdate` is the only data structure that leaves the market data layer. Every
downstream consumer — SSE streaming, portfolio valuation, trade execution — works
exclusively with this type.

```python
from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        """Absolute price change from previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat'."""
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

### Design decisions

- **`frozen=True`** — Price updates are immutable value objects. Once created they
  never change, so they're safe to share across async tasks without copying.
- **`slots=True`** — Memory optimization; many of these are created per second.
- **Computed properties** (`change`, `change_percent`, `direction`) — derived from
  `price` and `previous_price` so they can never drift out of sync. There is no
  stored `direction` field that could go stale.
- **`to_dict()`** — single serialization point shared by the SSE endpoint and any
  REST response that returns prices.

---

## 4. Price Cache — `cache.py`

The price cache is the central data hub. Data sources write to it; the SSE
stream, portfolio valuation, and trade execution read from it. It is thread-safe
because the Massive client's synchronous calls run in `asyncio.to_thread()` (a
real OS thread) while SSE reads happen on the event loop.

```python
from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker.

    Writers: SimulatorDataSource or MassiveDataSource (one at a time).
    Readers: SSE streaming endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Monotonically increasing; bumped on every update

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        """Record a new price for a ticker. Returns the created PriceUpdate.

        Automatically derives previous_price from the prior entry.
        If this is the first update for the ticker, previous_price == price
        (direction='flat').
        """
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices. Returns a shallow copy."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        """Convenience: get just the price float, or None."""
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        """Current version counter. Useful for SSE change detection."""
        return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

### Why a version counter?

The SSE loop polls the cache every ~500ms. Without a version counter it would
re-serialize and re-send every price every tick even when nothing changed — a
real concern with the Massive source, which only updates every 15s. The counter
lets the SSE loop skip sends when nothing is new:

```python
last_version = -1
while True:
    if price_cache.version != last_version:
        last_version = price_cache.version
        yield format_sse(price_cache.get_all())
    await asyncio.sleep(0.5)
```

### Why `threading.Lock` (not `asyncio.Lock`)

The Massive client's synchronous `get_snapshot_all()` runs in
`asyncio.to_thread()`, i.e. a real OS thread. An `asyncio.Lock` only serializes
coroutines on the event loop and would not protect against that thread.
`threading.Lock` works correctly from both the sync thread and the event loop.
The critical section is tiny (a dict lookup + assignment), so contention is
negligible at this scale.

---

## 5. Abstract Interface — `interface.py`

```python
from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their own
    schedule. Downstream code never calls the data source directly for prices —
    it reads from the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        # ... app runs ...
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        # ... app shutting down ...
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers.

        Starts a background task that periodically writes to the PriceCache.
        Must be called exactly once.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources. Safe to call twice."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set. Also removes it from the cache."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

### Why the source writes to the cache instead of returning prices

This push model decouples timing. The simulator ticks at 500ms, Massive polls at
15s, but SSE always reads from the cache at its own 500ms cadence. The SSE layer
never needs to know which source is active or what its update interval is.

---

## 6. Seed Prices & Ticker Parameters — `seed_prices.py`

Constants only — no logic. Shared by the simulator (for initial prices, GBM
params, and correlation structure).

```python
"""Seed prices and per-ticker parameters for the market simulator."""

# Realistic starting prices for the default watchlist
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# Per-ticker GBM parameters
#   sigma: annualized volatility (higher = more price movement)
#   mu:    annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},  # High volatility
    "NVDA": {"sigma": 0.40, "mu": 0.08},  # High volatility, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},  # Low volatility (bank)
    "V": {"sigma": 0.17, "mu": 0.04},  # Low volatility (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}

# Default parameters for tickers not in the list above (dynamically added)
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Correlation groups for the simulator's Cholesky decomposition
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Correlation coefficients
INTRA_TECH_CORR = 0.6     # Tech stocks move together
INTRA_FINANCE_CORR = 0.5  # Finance stocks move together
CROSS_GROUP_CORR = 0.3    # Between sectors / unknown tickers
TSLA_CORR = 0.3           # TSLA does its own thing
```

Tickers added dynamically that are not in `SEED_PRICES` start at a random price
in `[50, 300]` and use `DEFAULT_PARAMS`.

---

## 7. GBM Simulator — `simulator.py`

Two classes:

- **`GBMSimulator`** — pure, synchronous math engine. Stateful: holds current
  prices and advances them one step at a time.
- **`SimulatorDataSource`** — the `MarketDataSource` implementation that wraps
  `GBMSimulator` in an async loop and writes to the `PriceCache`.

### 7.1 The math

At each step a price evolves by Geometric Brownian Motion:

```
S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)
```

with `dt` expressed as a fraction of a trading year:

```
dt = 0.5 / (252 trading days * 6.5 hours * 3600 seconds) ≈ 8.48e-8
```

This tiny `dt` produces sub-cent moves per tick that accumulate naturally. GBM is
multiplicative (`exp()` is always positive) so **prices can never go negative**.

**Correlated moves.** Real sectors move together. Given a correlation matrix `C`,
compute `L = cholesky(C)`; then for independent standard normals `z`,
`L @ z` is a correlated draw. Cholesky guarantees a valid (positive
semi-definite) correlation matrix produces consistent correlated noise.

**Random events.** Each tick, each ticker has a ~0.1% chance of a sudden 2–5%
shock, for visual drama. With 10 tickers at 2 ticks/sec, expect one roughly every
50 seconds.

### 7.2 GBMSimulator — the math engine

```python
from __future__ import annotations

import asyncio
import logging
import math
import random

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource
from .seed_prices import (
    CORRELATION_GROUPS,
    CROSS_GROUP_CORR,
    DEFAULT_PARAMS,
    INTRA_FINANCE_CORR,
    INTRA_TECH_CORR,
    SEED_PRICES,
    TICKER_PARAMS,
    TSLA_CORR,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices."""

    # 252 trading days * 6.5 hours/day * 3600 seconds/hour = 5,896,800 seconds
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    # --- Public API ---

    def step(self) -> dict[str, float]:
        """Advance all tickers by one time step. Returns {ticker: new_price}.

        Hot path — called every 500ms. Keep it fast.
        """
        n = len(self._tickers)
        if n == 0:
            return {}

        z_independent = np.random.standard_normal(n)
        if self._cholesky is not None:
            z_correlated = self._cholesky @ z_independent
        else:
            z_correlated = z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            params = self._params[ticker]
            mu = params["mu"]
            sigma = params["sigma"]

            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Random shock event (~0.1% per tick per ticker)
            if random.random() < self._event_prob:
                shock_magnitude = random.uniform(0.02, 0.05)
                shock_sign = random.choice([-1, 1])
                self._prices[ticker] *= 1 + shock_magnitude * shock_sign
                logger.debug(
                    "Random event on %s: %.1f%% %s",
                    ticker, shock_magnitude * 100, "up" if shock_sign > 0 else "down",
                )

            result[ticker] = round(self._prices[ticker], 2)

        return result

    def add_ticker(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internals ---

    def _add_ticker_internal(self, ticker: str) -> None:
        """Add a ticker without rebuilding Cholesky (for batch init)."""
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        """Rebuild Cholesky of the ticker correlation matrix. O(n^2), n < 50."""
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = rho
                corr[j, i] = rho

        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        """Sector-based correlation between two tickers."""
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]

        # TSLA is in the tech set but behaves independently — check it first
        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR
```

> Note the ordering in `_pairwise_correlation`: the TSLA check comes **before**
> the tech-group check, because TSLA is a member of the tech set but is
> intentionally decorrelated from it.

### 7.3 SimulatorDataSource — async wrapper

```python
class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by the GBM simulator."""

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)
        # Seed the cache with initial prices so SSE has data immediately
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(tickers))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
            logger.info("Simulator: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        while True:
            try:
                if self._sim:
                    prices = self._sim.step()
                    for ticker, price in prices.items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

### Key behaviors

- **Immediate seeding** — `start()` (and `add_ticker()`) populate the cache
  *before* the loop runs, so the SSE endpoint has data on its first tick with no
  blank-screen delay.
- **Graceful cancellation** — `stop()` cancels the task and awaits it, swallowing
  `CancelledError`, for clean shutdown during FastAPI lifespan teardown.
- **Exception resilience** — the loop catches exceptions per step, so one bad tick
  never kills the feed.

---

## 8. Massive API Client — `massive_client.py`

Polls the Massive (formerly Polygon.io) REST snapshot endpoint on a configurable
interval. The synchronous `RESTClient` runs inside `asyncio.to_thread()` so it
never blocks the event loop.

```python
from __future__ import annotations

import asyncio
import logging

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Polls the multi-ticker snapshot endpoint for all watched tickers in a single
    API call, then writes results to the PriceCache.

    Rate limits:
      - Free tier: 5 req/min → poll every 15s (default)
      - Paid tiers: higher limits → poll every 2-5s
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)
        await self._poll_once()  # immediate first poll so the cache has data
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(tickers), self._interval,
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            logger.info("Massive: added ticker %s (appears on next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internal ---

    async def _poll_loop(self) -> None:
        """Poll on interval. First poll already happened in start()."""
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        """One poll cycle: fetch snapshots, update cache."""
        if not self._tickers or not self._client:
            return
        try:
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            processed = 0
            for snap in snapshots:
                try:
                    price = snap.last_trade.price
                    timestamp = snap.last_trade.timestamp / 1000.0  # ms → seconds
                    self._cache.update(ticker=snap.ticker, price=price, timestamp=timestamp)
                    processed += 1
                except (AttributeError, TypeError) as e:
                    logger.warning(
                        "Skipping snapshot for %s: %s",
                        getattr(snap, "ticker", "???"), e,
                    )
            logger.debug("Massive poll: updated %d/%d tickers", processed, len(self._tickers))
        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Don't re-raise — loop retries next interval.
            # Common failures: 401 (bad key), 429 (rate limit), network errors.

    def _fetch_snapshots(self) -> list:
        """Synchronous Massive REST call. Runs in a thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

### The snapshot endpoint

`get_snapshot_all` maps to
`GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,GOOGL,…` and
returns **all requested tickers in one call** — critical for staying under the
free tier's 5 req/min. The fields FinAlly extracts per snapshot:

| Field | Use |
|-------|-----|
| `snap.ticker` | cache key |
| `snap.last_trade.price` | current price (display + trading) |
| `snap.last_trade.timestamp` | when the trade occurred (Unix **ms** → ÷1000 for seconds) |

### Error-handling philosophy

The poller is intentionally resilient — a long-running feed must not die on a
transient failure:

| Error | Behavior |
|-------|----------|
| **401 Unauthorized** | Logged; poller keeps running (user can fix `.env` and restart). |
| **429 Rate Limited** | Logged; retried after `poll_interval`. |
| **Network timeout** | Logged; retried next cycle. |
| **Malformed snapshot** | That ticker skipped with a warning; others still processed. |
| **All tickers fail** | Cache keeps last-known prices; SSE keeps streaming (stale > nothing). |

### A note on dependencies

`massive` and `numpy` are declared as core dependencies in `pyproject.toml`, so
imports are at module top level (not lazy). The factory simply never instantiates
`MassiveDataSource` when no API key is present, so the simulator-only path still
needs no API key and makes no network calls.

---

## 9. Factory — `factory.py`

```python
from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Select the market data source from the environment.

    - MASSIVE_API_KEY set and non-empty → MassiveDataSource (real data)
    - Otherwise → SimulatorDataSource (GBM simulation)

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    logger.info("Market data source: GBM Simulator")
    return SimulatorDataSource(price_cache=price_cache)
```

Usage at startup:

```python
price_cache = PriceCache()
source = create_market_data_source(price_cache)
await source.start(initial_tickers)  # e.g., ["AAPL", "GOOGL", ...]
```

---

## 10. SSE Streaming Endpoint — `stream.py`

A FastAPI route that holds open a long-lived `text/event-stream` connection and
pushes price updates to the browser's native `EventSource`.

```python
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stream", tags=["streaming"])


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Create the SSE router bound to a price cache (no globals)."""

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # disable nginx buffering if proxied
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    """Yield SSE-formatted price events; stop when the client disconnects."""
    yield "retry: 1000\n\n"  # browser reconnects 1s after a drop

    last_version = -1
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while True:
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client_ip)
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()
                if prices:
                    data = {t: u.to_dict() for t, u in prices.items()}
                    yield f"data: {json.dumps(data)}\n\n"

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("SSE stream cancelled for: %s", client_ip)
```

### Wire format

Each event is a JSON map of ticker → snapshot:

```
data: {"AAPL":{"ticker":"AAPL","price":190.50,"previous_price":190.42,"timestamp":1707580800.5,"change":0.08,"change_percent":0.042,"direction":"up"},"GOOGL":{...}}

```

Client side:

```javascript
const es = new EventSource('/api/stream/prices');
es.onmessage = (event) => {
    const prices = JSON.parse(event.data);
    // prices = { "AAPL": { ticker, price, previous_price, change, direction, ... }, ... }
};
```

### Design points

- **Poll-and-push, not event-driven.** The loop reads the cache on a fixed 500ms
  interval rather than being notified by the producer. This produces evenly
  spaced updates — important for the frontend's sparkline accumulation — and keeps
  the SSE layer decoupled from producer timing.
- **Version gate.** Sends only when `price_cache.version` changed, avoiding
  redundant payloads (especially with the 15s Massive source).
- **Auto-reconnect.** `retry: 1000` plus `EventSource`'s built-in reconnection
  means the client recovers from drops with no custom logic.
- **Disconnect detection.** `request.is_disconnected()` ends the generator so the
  task doesn't leak after a client closes the tab.

---

## 11. Package Public API — `__init__.py`

```python
"""Market data subsystem for FinAlly.

Public API:
    PriceUpdate               - Immutable price snapshot dataclass
    PriceCache                - Thread-safe in-memory price store
    MarketDataSource          - Abstract interface for data providers
    create_market_data_source - Factory that selects simulator or Massive
    create_stream_router      - FastAPI router factory for the SSE endpoint
"""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
]
```

The rest of the backend imports only from `app.market`:

```python
from app.market import PriceCache, create_market_data_source, create_stream_router
```

---

## 12. FastAPI Lifecycle Integration

The market data system starts and stops with the app via the `lifespan` context
manager. The cache and source are stashed on `app.state` for dependency
injection.

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.market import (
    PriceCache,
    MarketDataSource,
    create_market_data_source,
    create_stream_router,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- STARTUP ---
    price_cache = PriceCache()
    app.state.price_cache = price_cache

    source = create_market_data_source(price_cache)
    app.state.market_source = source

    initial_tickers = await load_watchlist_tickers()  # reads from SQLite
    await source.start(initial_tickers)

    app.include_router(create_stream_router(price_cache))

    yield  # app runs

    # --- SHUTDOWN ---
    await source.stop()


app = FastAPI(title="FinAlly", lifespan=lifespan)


def get_price_cache() -> PriceCache:
    return app.state.price_cache


def get_market_source() -> MarketDataSource:
    return app.state.market_source
```

> **Router registration caveat.** `stream.py` defines a module-level `router` and
> `create_stream_router()` attaches the `/prices` route to it. Call the factory
> exactly **once** per process (as the lifespan does); calling it twice would
> register the route twice on the shared router. In tests, build a fresh
> `FastAPI()` app per test rather than calling the factory repeatedly.

### Consuming prices from other routes

```python
from fastapi import APIRouter, Depends, HTTPException

router = APIRouter(prefix="/api")


@router.post("/portfolio/trade")
async def execute_trade(
    trade: TradeRequest,
    price_cache: PriceCache = Depends(get_price_cache),
):
    price = price_cache.get_price(trade.ticker)
    if price is None:
        raise HTTPException(400, f"Price not yet available for {trade.ticker}.")
    # ... execute at `price` ...


@router.post("/watchlist")
async def add_to_watchlist(
    payload: WatchlistAdd,
    source: MarketDataSource = Depends(get_market_source),
):
    # insert into watchlist table ...
    await source.add_ticker(payload.ticker)


@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(
    ticker: str,
    source: MarketDataSource = Depends(get_market_source),
):
    # delete from watchlist table ...
    await source.remove_ticker(ticker)
```

---

## 13. Watchlist Coordination

When the watchlist changes (via REST or LLM chat), the data source must be told
so it tracks the right set of tickers.

### Adding

```
POST /api/watchlist {ticker: "PYPL"}
  → INSERT into watchlist table (SQLite)
  → await source.add_ticker("PYPL")
        Simulator: adds to GBMSimulator, rebuilds Cholesky, seeds cache now
        Massive:   appends to ticker list; appears on next poll
  → 200 (ticker + current price if available)
```

### Removing

```
DELETE /api/watchlist/PYPL
  → DELETE from watchlist table (SQLite)
  → await source.remove_ticker("PYPL")
        Simulator: removes from GBMSimulator, rebuilds Cholesky, drops from cache
        Massive:   removes from ticker list, drops from cache
```

### Edge case: removed ticker still has an open position

If the user removes a ticker from the watchlist but still holds shares, keep
tracking it so portfolio valuation stays accurate:

```python
@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(ticker: str, source = Depends(get_market_source)):
    await db.delete_watchlist_entry(ticker)
    position = await db.get_position(ticker)
    if position is None or position.quantity == 0:
        await source.remove_ticker(ticker)  # only stop tracking if no holding
    return {"status": "ok"}
```

---

## 14. Data Flow Walkthroughs

**Simulator tick (every 0.5s):**

```
asyncio loop  → GBMSimulator.step()  → {ticker: price, ...}
              → PriceCache.update(...) per ticker   (version += 1 each)
SSE loop      → sees version changed → get_all() → json → "data: {...}\n\n"
browser       → EventSource.onmessage → updates UI, flashes green/red
```

**Massive poll (every 15s):**

```
asyncio loop  → asyncio.to_thread(get_snapshot_all)   (off event loop)
              → for each snap: PriceCache.update(price, ts/1000)
SSE loop      → version changed once per poll → one push to each client
```

**Trade:** route handler calls `price_cache.get_price(ticker)` synchronously —
no awaiting the source, no API call — and fills at the cached price.

---

## 15. Testing Strategy

The shipped suite has **73 tests** across `backend/tests/market/`, mirroring the
modules. Run them with:

```bash
cd backend
uv run --extra dev pytest -v           # all tests
uv run --extra dev pytest --cov=app    # with coverage (84% overall)
```

| Test module | Focus |
|-------------|-------|
| `test_models.py` | `change`/`change_percent`/`direction` math, `to_dict`, immutability |
| `test_cache.py` | update/get/remove, first-update-is-flat, version increments, direction |
| `test_simulator.py` | `step()` shape, prices stay positive, add/remove, Cholesky rebuild, empty step |
| `test_simulator_source.py` | start seeds cache, prices move over time, clean double-stop, add/remove |
| `test_massive.py` | poll updates cache (mocked), malformed snapshot skipped, API error doesn't crash |
| `test_factory.py` | env var selects the right source |

### Representative cases

```python
# Prices can never go negative (GBM is multiplicative)
def test_prices_are_positive():
    sim = GBMSimulator(tickers=["AAPL"])
    for _ in range(10_000):
        assert sim.step()["AAPL"] > 0

# First update for a ticker is flat with previous_price == price
def test_first_update_is_flat():
    cache = PriceCache()
    u = cache.update("AAPL", 190.50)
    assert u.direction == "flat" and u.previous_price == 190.50

# Cholesky only exists with 2+ tickers
def test_cholesky_rebuilds_on_add():
    sim = GBMSimulator(tickers=["AAPL"])
    assert sim._cholesky is None
    sim.add_ticker("GOOGL")
    assert sim._cholesky is not None
```

### Massive tests are mocked

The Massive tests construct fake snapshot objects and patch
`MassiveDataSource._fetch_snapshots`, so they never hit the network and need no
API key. Use a long `poll_interval` (e.g. 60s) so the background loop doesn't
auto-poll mid-test, and drive `_poll_once()` directly:

```python
def _make_snapshot(ticker, price, ts_ms):
    snap = MagicMock()
    snap.ticker = ticker
    snap.last_trade.price = price
    snap.last_trade.timestamp = ts_ms
    return snap

@pytest.mark.asyncio
async def test_poll_updates_cache():
    cache = PriceCache()
    source = MassiveDataSource("test-key", cache, poll_interval=60.0)
    snaps = [_make_snapshot("AAPL", 190.50, 1707580800000)]
    with patch.object(source, "_fetch_snapshots", return_value=snaps):
        await source._poll_once()
    assert cache.get_price("AAPL") == 190.50
```

---

## 16. Error Handling & Edge Cases

| Scenario | Behavior |
|----------|----------|
| **Empty watchlist at startup** | `start([])` is fine — simulator produces no prices, Massive skips its call; SSE sends nothing until a ticker is added. |
| **Trade on a ticker with no cached price** | `get_price()` returns `None`; route returns `HTTP 400` with a "try again in a moment" message. The simulator avoids this by seeding on `add_ticker()`; Massive may have a brief first-poll gap. |
| **Invalid Massive API key** | First poll 401s; logged; poller keeps running; SSE shows "connected" but no data. Fix the key and restart. |
| **Concurrent cache access** | `threading.Lock` serializes writes (from the Massive thread) and reads (from the loop). Critical section is a dict op — negligible contention. |
| **Float precision** | Prices are `round()`ed to 2 dp; GBM's `exp()` form is numerically stable and always positive. |
| **Correlation matrix validity** | Cholesky requires a positive semi-definite matrix; the sector-based structure (with `1.0` on the diagonal) satisfies this for the ranges used. |

---

## 17. Configuration Summary

| Parameter | Location | Default | Description |
|-----------|----------|---------|-------------|
| `MASSIVE_API_KEY` | env var | `""` | If set → Massive API; else simulator |
| `update_interval` | `SimulatorDataSource.__init__` | `0.5` s | Simulator tick period |
| `poll_interval` | `MassiveDataSource.__init__` | `15.0` s | Massive poll period (free tier safe) |
| `event_probability` | `GBMSimulator.__init__` | `0.001` | Per-tick, per-ticker shock chance |
| `dt` | `GBMSimulator.__init__` | `~8.5e-8` | GBM step (fraction of a trading year) |
| SSE push interval | `_generate_events()` | `0.5` s | Cache poll/push cadence |
| SSE retry directive | `_generate_events()` | `1000` ms | Browser reconnect delay |

### Quick start (downstream code)

```python
from app.market import PriceCache, create_market_data_source

cache = PriceCache()
source = create_market_data_source(cache)        # reads MASSIVE_API_KEY
await source.start(["AAPL", "GOOGL", "MSFT"])    # seeds + starts producing

cache.get("AAPL")        # PriceUpdate | None
cache.get_price("AAPL")  # float | None
cache.get_all()          # dict[str, PriceUpdate]

await source.add_ticker("TSLA")
await source.remove_ticker("GOOGL")
await source.stop()
```
