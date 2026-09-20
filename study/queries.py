"""Canonical query definitions and the independent oracle that validates them.

Every query covers the half-open window [lo, hi) in UTC and returns columns
c0, c1, ... in a stable order. SQL and Flux variants must agree with
`reference`, apart from the explicitly approximate quantile engines.
"""

import datetime as dt
import json
import math
import statistics
from collections import defaultdict

SQL_ENGINES = ("postgres", "timescaledb", "clickhouse", "questdb", "cratedb")
EXACT_QUANTILE_ENGINES = ("postgres", "timescaledb", "clickhouse")
IDS = [str(i) for i in range(1, 21)]
QUARTILES = (0.25, 0.5, 0.75)

# Queries ranked by c1 descending with c0 as the tie-breaker, and their row limits.
RANKED_LIMITS = {"9": 10, "12": 10, "13": 100, "19": 5, "20": 10, "D1": 10, "A1": 10}


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def iso(seconds):
    moment = dt.datetime.fromtimestamp(seconds, dt.timezone.utc)
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def definition(engine, qid, lo, hi, now=None, event=None):
    """Build one engine-specific query. `hi` is exclusive."""
    qid = str(qid)
    now = hi if now is None else now

    if engine == "influxdb":
        text, columns = flux(qid, lo, hi, now, event)
    else:
        text, columns = sql(engine, qid, lo, hi, now, event)

    if qid != "14":
        mode = "exact"
    elif engine in EXACT_QUANTILE_ENGINES:
        mode = "exact_r7"
    else:
        mode = "approximate"

    parameters = {"lo": lo, "hi": hi, "now": now}
    if event:
        parameters["event"] = event

    return {
        "id": "Q" + qid if qid.isdigit() else qid,
        "query": text,
        "columns": columns,
        "mode": mode,
        "parameters": parameters,
    }


# --------------------------------------------------------------------------- SQL


def sql(engine, q, lo, hi, now, event):
    clickhouse = engine == "clickhouse"
    questdb = engine == "questdb"
    postgres = engine in ("postgres", "timescaledb")

    def timestamp(seconds):
        if clickhouse:
            return f"toDateTime64({seconds},6,'UTC')"
        return literal(iso(seconds))

    def where(start, end):
        return f"WHERE ts >= {timestamp(start)} AND ts < {timestamp(end)}"

    def bucket(unit):
        if clickhouse:
            suffix = {"hour": "Hour", "day": "Day", "minute": "Minute"}[unit]
            return f"toStartOf{suffix}(ts)"
        if questdb:
            interval = {"hour": "h", "day": "d", "minute": "m"}[unit]
            return f"timestamp_floor('{interval}',ts)"
        return f"date_trunc('{unit}',ts)"

    if clickhouse:
        epoch = "toUnixTimestamp(ts)"
        hour = "toHour(ts)"
        variance = "varSamp(rssi)"
    elif questdb:
        epoch = "cast(ts as long)/1000000"
        hour = "hour(ts)"
        variance = "variance(rssi)"
    else:  # PostgreSQL, TimescaleDB and CrateDB
        epoch = "extract(epoch from ts)"
        hour = "extract(hour from ts)"
        variance = "variance(rssi)"
    if engine == "cratedb":
        # Rescale CrateDB's population variance to the sample variance.
        variance = (
            "CASE WHEN count(*) > 1 THEN variance(rssi)*count(*)/(count(*)-1) "
            "ELSE NULL END"
        )

    if q == "probe":
        return (
            "SELECT count(*) AS c0 FROM user_events"
            f" WHERE user_id={literal(event['user'])}"
            f" AND ssid={literal(event['ssid'])}"
            f" AND ts={timestamp(event['ts'])}"
            f" AND rssi={event['rssi']}",
            1,
        )

    if q == "14":

        def quantile(p):
            if postgres:
                return f"percentile_cont({p}) WITHIN GROUP (ORDER BY rssi)"
            if clickhouse:
                return f"quantileExactInclusive({p})(rssi)"
            if questdb:
                return f"-approx_percentile(-rssi,{1 - p})"
            return f"percentile(rssi,{p})"

        parts = " UNION ALL ".join(
            f"SELECT {p} AS c0,{quantile(p)} AS c1 FROM user_events" for p in QUARTILES
        )
        return f"SELECT * FROM ({parts}) quantiles ORDER BY c0", 2

    middle = (lo + hi) // 2
    top = "ORDER BY c1 DESC,c0 LIMIT"
    # Query ID -> (select list, column count, clauses after FROM).
    queries = {
        "1": ("min(ts) AS c0,max(ts) AS c1", 2, ""),
        "2": ("count(*) AS c0", 1, ""),
        "3": ("count(DISTINCT user_id) AS c0", 1, ""),
        "4": ("avg(rssi) AS c0", 1, ""),
        "5": ("count(*) AS c0", 1, where(lo, middle)),
        "6": ("count(*) AS c0", 1, where(middle, hi)),
        "7": ("count(*) AS c0", 1, where(middle - 3600, middle + 3600)),
        "8": (
            f"{bucket('hour')} AS c0,count(*) AS c1",
            2,
            f"{where(middle, middle + 86400)} GROUP BY c0 ORDER BY c0",
        ),
        "9": ("user_id AS c0,count(*) AS c1", 2, f"GROUP BY user_id {top} 10"),
        "10": ("count(*) AS c0", 1, "WHERE rssi > -50"),
        "11": ("count(*) AS c0", 1, "WHERE rssi < -80"),
        "12": ("ssid AS c0,count(*) AS c1", 2, f"GROUP BY ssid {top} 10"),
        "13": (
            "user_id AS c0,avg(rssi) AS c1,min(rssi) AS c2,max(rssi) AS c3",
            4,
            f"GROUP BY user_id {top} 100",
        ),
        "15": ("count(*) AS c0", 1, where(lo, middle)),
        "16": ("count(*) AS c0", 1, where(middle, hi)),
        "17": (f"{hour} AS c0,count(*) AS c1", 2, "GROUP BY c0 ORDER BY c0"),
        "18": (
            f"{bucket('day')} AS c0,{variance} AS c1",
            2,
            "GROUP BY c0 ORDER BY c0 LIMIT 30",
        ),
        "19": (f"{bucket('hour')} AS c0,count(*) AS c1", 2, f"GROUP BY c0 {top} 5"),
        "20": (
            f"user_id AS c0,max({epoch})-min({epoch}) AS c1",
            2,
            f"GROUP BY user_id {top} 10",
        ),
        "D1": (
            "ssid AS c0,count(DISTINCT user_id) AS c1",
            2,
            f"{where(now - 60, now)} GROUP BY ssid {top} 10",
        ),
        "D2": (
            f"{bucket('minute')} AS c0,count(*) AS c1",
            2,
            f"{where(now - 900, now)} GROUP BY c0 ORDER BY c0",
        ),
        "A1": (
            "ssid AS c0,count(*) AS c1",
            2,
            f"{where(now - 60, now)} AND rssi < -80 GROUP BY ssid {top} 10",
        ),
    }
    select, columns, clauses = queries[q]
    return f"SELECT {select} FROM user_events {clauses}", columns


# -------------------------------------------------------------------------- Flux

FLUX_PREFIX = 'import "date"\nimport "math"\n'
EPOCH_SECONDS = "int(v:r._time)/1000000000"

# Per-group minimum (a) and maximum (b) timestamp in epoch seconds.
EPOCH_RANGE_REDUCER = (
    "reduce(identity:{a:9223372036,b:0},fn:(r,accumulator)=>({"
    f"a:if {EPOCH_SECONDS}<accumulator.a then {EPOCH_SECONDS} else accumulator.a,"
    f"b:if {EPOCH_SECONDS}>accumulator.b then {EPOCH_SECONDS} else accumulator.b}}))"
)

# Per-group count (n), sum (s), minimum (lo) and maximum (hi) RSSI.
RSSI_SUMMARY_REDUCER = (
    "reduce(identity:{n:0.0,s:0.0,"
    "lo:1000000000000000000000000000000.0,hi:-1000000000000000000000000000000.0},"
    "fn:(r,accumulator)=>({n:accumulator.n+1.0,s:accumulator.s+r._value,"
    "lo:if r._value<accumulator.lo then r._value else accumulator.lo,"
    "hi:if r._value>accumulator.hi then r._value else accumulator.hi}))"
)

# Running count (n), mean (m) and sum of squared deviations (s), Welford's method.
RSSI_VARIANCE_REDUCER = (
    "reduce(identity:{n:0.0,m:0.0,s:0.0},fn:(r,accumulator)=>({"
    "n:accumulator.n+1.0,"
    "m:accumulator.m+(r._value-accumulator.m)/(accumulator.n+1.0),"
    "s:accumulator.s+(r._value-accumulator.m)"
    "*(r._value-(accumulator.m+(r._value-accumulator.m)/(accumulator.n+1.0)))}))"
)

FLUX_SCALAR_QUERIES = ("2", "3", "4", "5", "6", "7", "10", "11", "15", "16", "probe")


def flux_top(limit):
    """Flux equivalent of SQL `ORDER BY c1 DESC,c0 LIMIT n`."""
    return (
        "|> map(fn:(r)=>({r with rank: -float(v:r.c1)})) "
        '|> sort(columns:["rank","c0"]) |> drop(columns:["rank"]) '
        f"|> limit(n:{limit})"
    )


def flux(q, lo, hi, now, event):
    middle = (lo + hi) // 2

    def source(start=lo, end=hi):
        # Flux rejects empty ranges; a false filter preserves SQL's empty-set result.
        stop = max(start + 1, end)
        pipeline = (
            'from(bucket:"bench") '
            f"|> range(start:time(v:{start * 10**9}), stop:time(v:{stop * 10**9})) "
            '|> filter(fn:(r)=>r._measurement=="user_events" and r._field=="rssi")'
        )
        if end <= start:
            pipeline += " |> filter(fn:(r)=>false)"
        return pipeline

    def finish(pipeline, columns, count=1, tail=""):
        output = f" |> map(fn:(r)=>({{{columns}}})) |> group() {tail}"
        return FLUX_PREFIX + pipeline + output, count

    everything = source()
    ungrouped = everything + " |> group()"

    if q == "1":
        return finish(f"{ungrouped} |> {EPOCH_RANGE_REDUCER}", "c0:r.a,c1:r.b", 2)

    if q in FLUX_SCALAR_QUERIES:
        windows = {
            "5": (lo, middle),
            "15": (lo, middle),
            "6": (middle, hi),
            "16": (middle, hi),
            "7": (middle - 3600, middle + 3600),
        }
        if q == "probe":
            pipeline = (
                source(event["ts"], event["ts"] + 1)
                + " |> filter(fn:(r)=>"
                + f"r.user_id=={json.dumps(event['user'])}"
                + f" and r.ssid=={json.dumps(event['ssid'])}"
                + f" and r._value=={event['rssi']}"
                + ") |> group()"
            )
        else:
            pipeline = source(*windows.get(q, (lo, hi))) + " |> group()"

        if q == "3":
            pipeline += ' |> distinct(column:"user_id")'
        elif q == "10":
            pipeline += " |> filter(fn:(r)=>r._value > -50.0)"
        elif q == "11":
            pipeline += " |> filter(fn:(r)=>r._value < -80.0)"

        pipeline += " |> mean()" if q == "4" else ' |> count(column:"_value")'
        return finish(pipeline, "c0:r._value")

    if q in ("9", "12", "D1", "A1"):
        key = "user_id" if q == "9" else "ssid"
        pipeline = source(now - 60, now) if q in ("D1", "A1") else everything
        if q == "A1":
            pipeline += " |> filter(fn:(r)=>r._value < -80.0)"
        pipeline += f' |> group(columns:["{key}"])'
        if q == "D1":
            pipeline += ' |> distinct(column:"user_id")'
        pipeline += " |> count()"
        return finish(pipeline, f"c0:r.{key},c1:r._value", 2, flux_top(10))

    if q == "13":
        pipeline = (
            f'{everything} |> group(columns:["user_id"]) |> {RSSI_SUMMARY_REDUCER}'
        )
        columns = "c0:r.user_id,c1:r.s/r.n,c2:r.lo,c3:r.hi"
        return finish(pipeline, columns, 4, flux_top(100))

    if q == "14":
        parts = [
            f'q{i} = {ungrouped} |> quantile(q:{p},method:"estimate_tdigest")'
            f" |> map(fn:(r)=>({{c0:{p},c1:r._value}}))"
            for i, p in enumerate(QUARTILES)
        ]
        union = 'union(tables:[q0,q1,q2]) |> group() |> sort(columns:["c0"])'
        return FLUX_PREFIX + "\n".join(parts) + "\n" + union, 2

    if q == "20":
        pipeline = (
            f'{everything} |> group(columns:["user_id"]) |> {EPOCH_RANGE_REDUCER}'
        )
        return finish(pipeline, "c0:r.user_id,c1:r.b-r.a", 2, flux_top(10))

    if q in ("8", "17", "18", "19", "D2"):
        if q == "8":
            pipeline = source(middle, middle + 86400)
        elif q == "D2":
            pipeline = source(now - 900, now)
        else:
            pipeline = everything

        if q == "17":
            bucket = "date.hour(t:r._time)"
        else:
            unit = {"18": "1d", "D2": "1m"}.get(q, "1h")
            bucket = f"int(v:date.truncate(t:r._time,unit:{unit}))/1000000000"
        pipeline += (
            f" |> map(fn:(r)=>({{r with bucket:{bucket}}}))"
            ' |> group(columns:["bucket"])'
        )

        if q == "18":
            # Fixtures and study inputs must include >=2 records in each retained day.
            return finish(
                f"{pipeline} |> {RSSI_VARIANCE_REDUCER}",
                'c0:r.bucket,c1:if r.n>1.0 then r.s/(r.n-1.0) else float(v:"NaN")',
                2,
                '|> sort(columns:["c0"]) |> limit(n:30)',
            )

        tail = flux_top(5) if q == "19" else '|> sort(columns:["c0"])'
        return finish(pipeline + " |> count()", "c0:r.bucket,c1:r._value", 2, tail)

    raise ValueError(q)


# ------------------------------------------------------------------------ Oracle


def fixture():
    """Three days of events on minute boundaries that stress hour and day edges."""
    start = 1735689600
    events = []
    for day in range(3):
        for minute in (0, 1, 59, 60, 61, 719, 720, 721, 1438, 1439):
            for user in range(12):
                events.append(
                    {
                        "user": f"user-{user:02d}",
                        "ssid": f"ap-{(user + minute + day) % 4}",
                        "ts": start + day * 86400 + minute * 60 + user,
                        "rssi": float(-95 + (user * 7 + minute + day) % 65),
                    }
                )
    return events


def quantile_r7(ordered, p):
    """Linear interpolation between closest ranks (R type 7)."""
    position = (len(ordered) - 1) * p
    below = int(position)
    above = min(below + 1, len(ordered) - 1)
    return ordered[below] + (ordered[above] - ordered[below]) * (position - below)


def reference(events, q, lo, hi, now=None, event=None):
    """Compute the expected result of query `q` directly in Python."""
    now = hi if now is None else now
    middle = (lo + hi) // 2

    def window(start, end):
        return [e for e in events if start <= e["ts"] < end]

    if q == "1":
        return [[min(e["ts"] for e in events), max(e["ts"] for e in events)]]
    if q == "2":
        return [[len(events)]]
    if q == "3":
        return [[len({e["user"] for e in events})]]
    if q == "4":
        return [[statistics.mean(e["rssi"] for e in events)]]
    if q in ("5", "15"):
        return [[len(window(lo, middle))]]
    if q in ("6", "16"):
        return [[len(window(middle, hi))]]
    if q == "7":
        return [[len(window(middle - 3600, middle + 3600))]]
    if q == "10":
        return [[sum(e["rssi"] > -50 for e in events)]]
    if q == "11":
        return [[sum(e["rssi"] < -80 for e in events)]]
    if q == "probe":
        fields = ("user", "ssid", "ts", "rssi")
        return [[sum(all(e[k] == event[k] for k in fields) for e in events)]]
    if q == "14":
        ordered = sorted(e["rssi"] for e in events)
        return [[p, quantile_r7(ordered, p)] for p in QUARTILES]

    if q == "8":
        subset = window(middle, middle + 86400)
    elif q == "D1":
        subset = window(now - 60, now)
    elif q == "A1":
        subset = [e for e in window(now - 60, now) if e["rssi"] < -80]
    elif q == "D2":
        subset = window(now - 900, now)
    else:
        subset = events

    group_keys = {
        "8": lambda e: e["ts"] // 3600 * 3600,
        "9": lambda e: e["user"],
        "12": lambda e: e["ssid"],
        "13": lambda e: e["user"],
        "17": lambda e: (e["ts"] // 3600) % 24,
        "18": lambda e: e["ts"] // 86400 * 86400,
        "19": lambda e: e["ts"] // 3600 * 3600,
        "20": lambda e: e["user"],
        "D1": lambda e: e["ssid"],
        "D2": lambda e: e["ts"] // 60 * 60,
        "A1": lambda e: e["ssid"],
    }
    groups = defaultdict(list)
    for e in subset:
        groups[group_keys[q](e)].append(e)

    rows = []
    for key, members in groups.items():
        rssi = [e["rssi"] for e in members]
        if q == "13":
            values = [statistics.mean(rssi), min(rssi), max(rssi)]
        elif q == "18":
            values = [statistics.variance(rssi) if len(rssi) > 1 else None]
        elif q == "20":
            timestamps = [e["ts"] for e in members]
            values = [max(timestamps) - min(timestamps)]
        elif q == "D1":
            values = [len({e["user"] for e in members})]
        else:
            values = [len(members)]
        rows.append([key, *values])

    if q in RANKED_LIMITS:
        rows.sort(key=lambda row: (-row[1], row[0]))
        return rows[: RANKED_LIMITS[q]]
    rows.sort(key=lambda row: row[0])
    return rows[:30] if q == "18" else rows


def matches(actual, expected, approx=False):
    """Compare result sets; `approx` relaxes only the quantile value column."""
    if len(actual) != len(expected):
        return False
    for actual_row, expected_row in zip(actual, expected):
        if len(actual_row) != len(expected_row):
            return False
        for column, (got, want) in enumerate(zip(actual_row, expected_row)):
            if got is None or want is None:
                if got is not want:
                    return False
            elif isinstance(want, (int, float)):
                tolerance = 5.0 if approx and column == 1 else 1e-5
                try:
                    if not math.isclose(
                        float(got), want, rel_tol=1e-6, abs_tol=tolerance
                    ):
                        return False
                except (ValueError, TypeError):
                    return False
            elif got != want:
                return False
    return True
