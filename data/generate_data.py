import csv
import math
import random
from datetime import datetime, timedelta

# ── Greek calculations ────────────────────────────────────────────────────────


def norm_cdf(x: float) -> float:
    """Approximation of the standard normal CDF."""
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def black_scholes(
    S: float,  # underlying price
    K: float,  # strike
    T: float,  # time to expiry in years
    r: float,  # risk-free rate
    sigma: float,  # implied volatility
    option_type: str = "C",
) -> dict:
    if T <= 0:
        intrinsic = max(0, S - K) if option_type == "C" else max(0, K - S)
        return {
            "price": intrinsic,
            "delta": 0,
            "gamma": 0,
            "theta": 0,
            "vega": 0,
            "iv": sigma,
        }

    d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)

    nd1 = norm_cdf(d1)
    nd2 = norm_cdf(d2)
    nd1_ = norm_cdf(-d1)
    nd2_ = norm_cdf(-d2)

    pdf_d1 = math.exp(-0.5 * d1**2) / math.sqrt(2 * math.pi)

    if option_type == "C":
        price = S * nd1 - K * math.exp(-r * T) * nd2
        delta = nd1
    else:
        price = K * math.exp(-r * T) * nd2_ - S * nd1_
        delta = nd1 - 1

    gamma = pdf_d1 / (S * sigma * math.sqrt(T))
    theta = (
        -(S * pdf_d1 * sigma) / (2 * math.sqrt(T))
        - r * K * math.exp(-r * T) * (nd2 if option_type == "C" else nd2_)
    ) / 365
    vega = S * pdf_d1 * math.sqrt(T) / 100  # per 1% vol move

    return {
        "price": round(price, 4),
        "delta": round(delta, 4),
        "gamma": round(gamma, 6),
        "theta": round(theta, 4),
        "vega": round(vega, 4),
        "iv": round(sigma, 4),
    }


# ── Data generation ───────────────────────────────────────────────────────────


def generate_equity_csv(
    path: str,
    symbol: str,
    start: datetime,
    days: int = 252,
    start_price: float = 100.0,
    drift: float = 0.0003,
    volatility: float = 0.015,
    seed: int = 42,
):
    random.seed(seed)
    rows = []
    price = start_price
    date = start

    for _ in range(days):
        # Skip weekends
        while date.weekday() >= 5:
            date += timedelta(days=1)

        daily_return = random.gauss(drift, volatility)
        open_ = round(price, 2)
        close = round(price * (1 + daily_return), 2)
        high = round(max(open_, close) * (1 + abs(random.gauss(0, 0.003))), 2)
        low = round(min(open_, close) * (1 - abs(random.gauss(0, 0.003))), 2)
        volume = int(random.gauss(1_000_000, 200_000))

        rows.append(
            {
                "timestamp": date.strftime("%Y-%m-%d"),
                "symbol": symbol,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": max(volume, 100_000),
            }
        )

        price = close
        date += timedelta(days=1)

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"✓ Wrote {len(rows)} equity rows to {path}")
    return rows


def generate_options_csv(
    path: str,
    equity_rows: list[dict],
    underlying: str,
    risk_free: float = 0.05,
    base_iv: float = 0.25,
    strikes_pct: list[float] = None,  # strikes as % of spot
    expiry_cycles: int = 4,  # how many monthly expiries to model
):
    """
    For each trading day, emit one option chain row per
    (strike, expiry, type) combination that hasn't expired yet.
    """
    if strikes_pct is None:
        strikes_pct = [0.90, 0.95, 1.00, 1.05, 1.10]

    # Build monthly expiry dates (3rd Friday of each month)
    def third_friday(year, month):
        d = datetime(year, month, 1)
        fridays = [
            d + timedelta(days=i)
            for i in range(31)
            if (d + timedelta(days=i)).month == month
            and (d + timedelta(days=i)).weekday() == 4
        ]
        return fridays[2]

    first_date = datetime.strptime(equity_rows[0]["timestamp"], "%Y-%m-%d")
    last_date = datetime.strptime(equity_rows[-1]["timestamp"], "%Y-%m-%d")

    expiries = []
    year, month = first_date.year, first_date.month
    while True:
        tf = third_friday(year, month)
        if tf > last_date:
            break
        if tf >= first_date:
            expiries.append(tf)
        month += 1
        if month > 12:
            month = 1
            year += 1
    expiries = expiries[:expiry_cycles]

    rows = []
    for eq in equity_rows:
        date = datetime.strptime(eq["timestamp"], "%Y-%m-%d")
        spot = eq["close"]

        for expiry in expiries:
            if expiry <= date:
                continue
            T = (expiry - date).days / 365.0

            for pct in strikes_pct:
                strike = round(spot * pct, 0)  # snap to whole dollar
                # Add some vol skew: lower strikes have higher IV
                skew_iv = base_iv + (1 - pct) * 0.15

                for opt_type in ("C", "P"):
                    bs = black_scholes(spot, strike, T, risk_free, skew_iv, opt_type)
                    symbol = (
                        f"{underlying}"
                        f"{expiry.strftime('%y%m%d')}"
                        f"{opt_type}"
                        f"{int(strike):05d}"
                    )
                    rows.append(
                        {
                            "timestamp": eq["timestamp"],
                            "symbol": symbol,
                            "underlying": underlying,
                            "strike": strike,
                            "expiry": expiry.strftime("%Y-%m-%d"),
                            "option_type": opt_type,
                            "open": bs["price"],
                            "high": round(bs["price"] * 1.02, 4),
                            "low": round(bs["price"] * 0.98, 4),
                            "close": bs["price"],
                            "volume": random.randint(10, 500),
                            "iv": bs["iv"],
                            "delta": bs["delta"],
                            "gamma": bs["gamma"],
                            "theta": bs["theta"],
                            "vega": bs["vega"],
                        }
                    )

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"✓ Wrote {len(rows)} option rows to {path}")
    return rows
