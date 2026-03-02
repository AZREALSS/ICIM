import argparse
from pathlib import Path

import pandas as pd


def load_ohlc(path: Path, prefix: str) -> pd.DataFrame:
    required = ["time", "open", "close"]
    df = pd.read_csv(path)
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{path} missing columns: {missing}")

    df = df[required].copy()
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df["open"] = pd.to_numeric(df["open"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna().sort_values("time").drop_duplicates(subset=["time"])
    return df.rename(columns={"open": f"{prefix}_open", "close": f"{prefix}_close"})


def build_positions(df: pd.DataFrame) -> pd.DataFrame:
    ratio_open = df["ic_open"] / df["im_open"]
    ratio_close = df["ic_close"] / df["im_close"]

    bull = ratio_close > ratio_open
    bear = ratio_close < ratio_open
    bull2 = bull & bull.shift(1, fill_value=False)
    bear2 = bear & bear.shift(1, fill_value=False)

    raw_signal = pd.Series(0, index=df.index, dtype="int64")
    raw_signal.loc[bull2] = 1
    raw_signal.loc[bear2] = -1

    pos = []
    cur = 0
    for sig in raw_signal:
        if sig == 1 and cur != 1:
            cur = 1
        elif sig == -1 and cur != -1:
            cur = -1
        pos.append(cur)

    out = df.copy()
    out["ratio_open"] = ratio_open
    out["ratio_close"] = ratio_close
    out["bull"] = bull
    out["bear"] = bear
    out["bull2"] = bull2
    out["bear2"] = bear2
    out["raw_signal"] = raw_signal
    out["position"] = pd.Series(pos, index=out.index, dtype="int64")
    return out


def calc_returns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["next_time"] = out["time"].shift(-1)
    out["next_ic_close"] = out["ic_close"].shift(-1)
    out["next_im_close"] = out["im_close"].shift(-1)
    out["ret_next_ic"] = out["next_ic_close"] / out["ic_close"] - 1.0
    out["ret_next_im"] = out["next_im_close"] / out["im_close"] - 1.0

    # position=1: long IC short IM; position=-1: short IC long IM
    out["pair_ret"] = out["position"] * (out["ret_next_ic"] - out["ret_next_im"])
    out = out.dropna(subset=["next_time", "pair_ret"]).reset_index(drop=True)
    out["equity"] = (1.0 + out["pair_ret"]).cumprod()
    out["cum_ret"] = out["equity"] - 1.0
    return out


def summarize_trades(df: pd.DataFrame) -> pd.DataFrame:
    active = df[df["position"] != 0].copy()
    if active.empty:
        return pd.DataFrame(
            columns=[
                "trade_id",
                "direction",
                "entry_signal_date",
                "exit_signal_date",
                "exit_realize_date",
                "holding_days",
                "trade_ret",
            ]
        )

    active["group"] = (active["position"] != active["position"].shift(1)).cumsum()
    rows = []
    for gid, g in active.groupby("group", sort=True):
        direction = "long_IC_short_IM" if int(g["position"].iloc[0]) == 1 else "short_IC_long_IM"
        trade_ret = (1.0 + g["pair_ret"]).prod() - 1.0
        rows.append(
            {
                "trade_id": int(gid),
                "direction": direction,
                "entry_signal_date": g["time"].iloc[0].date(),
                "exit_signal_date": g["time"].iloc[-1].date(),
                "exit_realize_date": g["next_time"].iloc[-1].date(),
                "holding_days": int(len(g)),
                "trade_ret": float(trade_ret),
            }
        )
    return pd.DataFrame(rows)


def parse_args():
    p = argparse.ArgumentParser(description="IC/IM consecutive 2-candle switch backtest")
    p.add_argument("--ic", default="IC500.csv", help="IC csv path")
    p.add_argument("--im", default="IM1000.csv", help="IM csv path")
    p.add_argument("--start", default=None, help="start date, e.g. 2024-01-01")
    p.add_argument("--end", default=None, help="end date, e.g. 2025-12-31")
    p.add_argument("--out-daily", default="trade_daily_ic_im_two_candle.csv", help="daily output csv")
    p.add_argument("--out-trades", default="trade_summary_ic_im_two_candle.csv", help="trade summary csv")
    return p.parse_args()


def apply_date_filter(df: pd.DataFrame, start: str | None, end: str | None) -> pd.DataFrame:
    out = df
    if start:
        out = out[out["time"] >= pd.to_datetime(start)]
    if end:
        out = out[out["time"] <= pd.to_datetime(end)]
    return out


def main():
    args = parse_args()
    ic_path = Path(args.ic)
    im_path = Path(args.im)
    if not ic_path.exists():
        raise FileNotFoundError(f"missing file: {ic_path}")
    if not im_path.exists():
        raise FileNotFoundError(f"missing file: {im_path}")

    ic = load_ohlc(ic_path, "ic")
    im = load_ohlc(im_path, "im")
    df = ic.merge(im, on="time", how="inner", validate="one_to_one").sort_values("time").reset_index(drop=True)
    df = apply_date_filter(df, args.start, args.end).reset_index(drop=True)
    if df.empty:
        raise RuntimeError("no data after merge/filter")

    with_pos = build_positions(df)
    daily = calc_returns(with_pos)
    trades = summarize_trades(daily)

    daily.to_csv(args.out_daily, index=False, float_format="%.10f")
    trades.to_csv(args.out_trades, index=False, float_format="%.10f")

    active_daily = daily[daily["position"] != 0]
    total_return = float((1.0 + daily["pair_ret"]).prod() - 1.0) if not daily.empty else 0.0
    trade_count = int(len(trades))
    trade_win_rate = float((trades["trade_ret"] > 0).mean()) if trade_count else 0.0
    avg_trade_ret = float(trades["trade_ret"].mean()) if trade_count else 0.0
    daily_win_rate = float((active_daily["pair_ret"] > 0).mean()) if not active_daily.empty else 0.0

    print("Rule: two bullish ratio candles -> long IC short IM; two bearish -> short IC long IM")
    print(f"Date range       : {daily['time'].iloc[0].date()} ~ {daily['next_time'].iloc[-1].date()}")
    print(f"Trading days     : {len(daily)}")
    print(f"Active days      : {len(active_daily)}")
    print(f"Trades           : {trade_count}")
    print(f"Trade win rate   : {trade_win_rate:.2%}")
    print(f"Daily win rate   : {daily_win_rate:.2%}")
    print(f"Total return     : {total_return:.2%}")
    print(f"Avg trade return : {avg_trade_ret:.2%}")
    if not daily.empty:
        print(f"Final equity     : {daily['equity'].iloc[-1]:.6f}")
    print(f"Daily file       : {args.out_daily}")
    print(f"Trades file      : {args.out_trades}")


if __name__ == "__main__":
    main()
