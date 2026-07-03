# agents/features.py
"""Technical indicators computed from tick DataFrames (ingestion.SCHEMA).

Used by the baseline strategy for its trading rule, and by the AI strategy to
send a compact summary of the market to the model instead of raw ticks.
"""
import os

import polars as pl

# Rolling windows are counted in ticks, not seconds
SHORT_WINDOW = int(os.environ.get("SHORT_WINDOW", "20"))
LONG_WINDOW = int(os.environ.get("LONG_WINDOW", "60"))


def compute_indicators(df, short=SHORT_WINDOW, long=LONG_WINDOW):
    """Returns df with rolling SMA and VWAP columns appended (the SMA columns
    hold nulls until their window has enough ticks)."""
    return df.with_columns(
        sma_short=pl.col("price").rolling_mean(short),
        sma_long=pl.col("price").rolling_mean(long),
        vwap=(pl.col("price") * pl.col("volume")).cum_sum()
             / pl.col("volume").cum_sum(),
    )


def summarize(df, short=SHORT_WINDOW, long=LONG_WINDOW):
    """One compact dict describing the tick window — cheap to embed in an LLM
    prompt and handy for logging decisions."""
    last = compute_indicators(df, short, long).row(-1, named=True)
    returns = df["price"].pct_change().drop_nulls()

    def rnd(value, digits=4):
        return None if value is None else round(value, digits)

    return {
        "ticker": last["ticker"],
        "as_of": last["timestamp"].isoformat(),
        "ticks": len(df),
        "last_price": last["price"],
        "vwap": rnd(last["vwap"]),
        "sma_short": rnd(last["sma_short"]),
        "sma_long": rnd(last["sma_long"]),
        "window_low": df["price"].min(),
        "window_high": df["price"].max(),
        "momentum_pct": rnd((last["price"] / df["price"][0] - 1) * 100),
        "volatility_pct": rnd((returns.std() or 0.0) * 100),
        "total_volume": int(df["volume"].sum()),
    }
