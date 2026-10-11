# broker-bridge

A **broker-neutral HTTP + Server-Sent Events API on localhost** for trading programs.
It exposes quotes, snapshots, historical bars, contract lookup, orders, order status
and fills as plain JSON, in broker-neutral terms: `instrument`, `side`, `LIMIT`,
`fillId`, `brokerId`.

Any program in any language can use it over a socket. A client never links a broker's
client library, never compiles against one and never learns its message formats — that
is the point of the bridge, and it is what keeps a broker's licence terms off the
calling program.

## Broker support

| Broker | Status | How |
|---|---|---|
| **Interactive Brokers** | implemented, the only one today | IB's official Java client (TWS API 10.50.02) against a logged-in TWS or IB Gateway |
| anything else | not implemented | API v1 reserves an optional `venue` field, which defaults to IB (see Compatibility) |

The API is broker-neutral; this program is not broker-agnostic. Six classes compile
against IB's client (`BrokerService`, `Orders`, `Contracts`, `IbCallbacks`, `IbClient`,
`SocketIbClient`), so IB types reach into the service layer, not just an edge adapter.
Adding a second broker means a second implementation under the same v1 wire API, not a
configuration switch. Nothing IB-specific crosses the API: no IB class name, enum or
message layout appears in a request, a response or an event.

- **Licence**: GNU GPL v3 or later (see `LICENSE`). IB's client, included unmodified in
  `third_party/ib-tws-api`, is GPLv3, which is why it lives in this separate program
  rather than inside any caller.
- **Runtime**: JDK 25; HTTP requests and broker message dispatch run on virtual threads.
- **Safety**: binds to `127.0.0.1`; order placement is **refused** unless `BRIDGE_ORDERS_ENABLED=true`.
- Not affiliated with or endorsed by Interactive Brokers. "Interactive Brokers", "IBKR",
  "TWS" and "IB Gateway" are trademarks of their owners, used here only to say what this
  program connects to.

## Build and run

```bash
mvn package                                  # JDK 25 + Maven 3.9
IB_HOST=127.0.0.1 IB_PORT=4002 java -jar bridge/target/broker-bridge.jar
curl -s localhost:8090/v1/status

docker build -t broker-bridge .              # or as a container
docker run --rm --network host broker-bridge
```

GitHub Actions (`.github/workflows/ci.yml`) builds and tests on every push and pull request (`mvn -B verify`, 61 tests) and
then builds the image, starts it without a Gateway and checks that `/v1/status` answers with orders refused.

Bridge settings, independent of the broker:

| Variable | Default | |
|---|---|---|
| `BRIDGE_BIND` / `BRIDGE_PORT` | `127.0.0.1` / `8090` | API listen address; the API has **no authentication** |
| `BRIDGE_ORDERS_ENABLED` | `false` | only `true` (any case) lets orders through; `1` and `yes` do not |
| `BRIDGE_DATA_DIR` | unset (image: `/data`) | journal of order events and order ids, so replay survives a restart |
| `BRIDGE_REPLAY_MAX` | `100000` | order events kept for replay |
| `BRIDGE_WATCHDOG` | `on` | `off` stops the bridge from ending itself when its connection machinery is stuck (see Behaviour) |
| `LOG_LEVEL` | `info` | `trace`, `debug`, `info`, `warn` or `error` (logging is `slf4j-simple`) |

Settings of the IB adapter. They keep their `IB_` names because they configure IB's own
client and nothing else reads them:

| Variable | Default | |
|---|---|---|
| `IB_HOST` / `IB_PORT` | `127.0.0.1` / `4002` | Gateway API socket (4002 paper, 4001 live) |
| `IB_CLIENT_ID` | `11` | API client id |
| `IB_MARKET_DATA_TYPE` | `1` | 1 live, 2 frozen, 3 delayed, 4 delayed-frozen |

## API v1

All bodies are JSON; errors are `{"error": "…"}` (plus `"retryable": true` when a retry may succeed).

| Method | Path | Body → response |
|---|---|---|
| GET | `/v1/status` | → `{connected, serverVersion, accounts (masked), lastError, ordersEnabled, since, dataFarms, startedAt}` (`startedAt` changes only when the bridge restarts) |
| POST | `/v1/subscriptions` | `{instrument, tag, snapshot, genericTicks}` → `{subscriptionId}` · 503 disconnected |
| DELETE | `/v1/subscriptions/{id}` | → 204 |
| POST | `/v1/snapshots` | `{instrument, field: BID|ASK|LAST|CLOSE, timeoutMs}` → `{bid, ask, last, close, time}` · 504 timeout |
| POST | `/v1/history` | `{instrument, end, duration, barSize, what, regularHoursOnly, timeoutMs}` → `{bars: [{time, open, high, low, close, volume, count, wap}]}` · 429 pacing, with `Retry-After` when the broker named a wait |
| POST | `/v1/contracts/resolve` | `{instrument}` → `{instrument, marketName, minTick, priceMagnifier, orderTypes, validExchanges}` · 404 none |
| POST | `/v1/orders` | `{clientOrderId, tag, instrument, side, quantity, type: LIMIT|MARKET|STOP|STOP_LIMIT|LIMIT_IF_TOUCHED, limitPrice, stopPrice (also the touch price), timeInForce: DAY|GTC|IOC, allOrNone, orderRef, modify}` → 202 · 403 disabled · 409 duplicate id · 503 disconnected |
| DELETE | `/v1/orders/{clientOrderId}` | → 202 · 403 disabled · 404 unknown |
| GET | `/v1/events` | `text/event-stream`: `connection`, `quote`, `snapshot-end`, `order-status`, `fill`, `error`; `: heartbeat` every 15 s |

**Instrument**: `{symbol, type: STOCK|OPTION|FUTURE|FUTURE_OPTION|INDEX|FX|COMBO, exchange, primaryExchange, currency,
expiry, right: CALL|PUT, strike, multiplier, brokerId, legs: [{brokerId, ratio, side, exchange}]}`.

**Quote event**: `{tag, subscriptionId, field, code, price, size, option: {impliedVol, delta, gamma, theta, vega,
optionPrice, underlyingPrice}, time}`. `code` is the broker's own numeric field code (with the IB adapter, an IB tick
type) (0 bid size, 1 bid, 2 ask, 3 ask size,
4 last, 5 last size, 6 high, 7 low, 8 volume, 9 close, 10–13 option computations, 14 open, 45 last timestamp);
delayed-data codes are reported as their live equivalents.

**Event ids and resume**: every event has an SSE `id:` (a number that keeps increasing, also
across restarts). Order events (`order-status`, `fill`, and `error` with a `clientOrderId`)
are kept and, with `BRIDGE_DATA_DIR`, journaled to disk (fsync per event). Reconnect with the
standard `Last-Event-ID: <id>` header (or `/v1/events?lastEventId=<id>`) to get every kept order
event after that id, in order, followed by live events, with no gap or repeat in between. Quotes
are not replayed. If the id is older than what was kept you first get
`event: replay-truncated` `{after, oldestKept}`. After those replayed order events (and when there is
nothing to replay) the bridge sends one `event: replay-complete` with id 0, `{replayed, after}` (`after` only when the
client asked to resume). It is not journaled. On every broker connection the bridge also asks
the broker for the day's executions, so fills made while it was down are sent too. Fills are unique by
`fillId`; delivery is at-least-once, so clients should ignore a `fillId` they already have.

**Tags**: `tag` is any string the client chooses for a subscription or order (e.g. which
of its components should get the data). Every quote, snapshot-end, order-status and fill
event that results from it carries the same `tag`. `orderRef` is passed to the broker as
the order reference.

**Compatibility**: API v1 only grows by adding optional fields and new event types. Clients
must ignore fields and event types they don't know; the bridge ignores unknown request fields.
A future optional `venue` field (which broker or exchange account an instrument, order or
event belongs to) will default to the only configured venue, IB, when absent, so v1 clients
keep working. Removing or renaming a field, or changing its meaning, needs `/v2`.

## Behaviour

- One broker session (with the IB adapter: client id `IB_CLIENT_ID`). On disconnect (e.g. the Gateway's daily restart)
  it retries every 5 s, doubling up to 120 s, then re-sends active streaming subscriptions. A handshake the Gateway
  accepts and then drops (error 502 while it is restarting) waits that same backoff and does not connect again
  immediately. The handshake itself is bounded (10 s to connect, 15 s for the Gateway's answer): a Gateway that
  accepts the socket and says nothing is given up on. Pending snapshot/history/resolve requests fail with 503 when
  the session drops.
- **Watchdog.** IB's client holds its own lock while it reports a closed socket, so the connection loop never takes a
  lock of its own while it asks the client (a loop that did deadlocked once, on 2026-10-07, and stayed down for three
  days with the Gateway logged in; `IbConnectionLockTest` reproduces it). As a last defence a watchdog thread checks
  every 15 s that the client answers within 45 s and that the connection loop has gone round within 5 minutes; if not,
  it logs a thread dump (locks included) and ends the process (`Runtime.halt`, exit 70), so that the container's
  restart policy starts a new bridge in seconds. `BRIDGE_WATCHDOG=off` turns it off.
- Order ids: the caller's `clientOrderId` is mapped to the broker's order id inside the bridge; status, fills and order
  errors come back with the `clientOrderId`. With `BRIDGE_DATA_DIR` the mapping is journaled, so events for
  earlier orders still arrive after a bridge restart.
- Account ids are masked in logs, status and events (`DU*****67`).

## Layout

```
pom.xml                    parent (JDK 25)
third_party/ib-tws-api/    IB's client, unmodified (see its README for source and checksum)
bridge/                    the bridge (io.github.wildfly8.brokerbridge)
                           no IB import:  BridgeHttpServer, Model, Json, EventHub, EventLog, BridgeState,
                                          BridgeConfig, BridgeMain, ApiException, IbConnection (reconnect loop),
                                          QuoteFields (the numeric codes are IB tick types)
                           uses IB's client: BrokerService, Orders, Contracts, IbCallbacks, IbClient, SocketIbClient
Dockerfile                 eclipse-temurin 25 JRE image
```
