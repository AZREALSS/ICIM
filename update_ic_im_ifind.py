import argparse
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from iFinDPy import THS_HQ, THS_iFinDLogin, THS_iFinDLogout


OUTPUT_COLUMNS = ["number", "time", "thscode", "open", "high", "low", "close", "amount", "volume"]


def normalize_col_name(name: str) -> str:
    return "".join(ch for ch in str(name).lower() if ch.isalnum())


def choose_column(df: pd.DataFrame, candidates: list[str], required: bool = True) -> Optional[str]:
    normalized = {normalize_col_name(c): c for c in df.columns}
    for cand in candidates:
        key = normalize_col_name(cand)
        if key in normalized:
            return normalized[key]
    if required:
        raise KeyError(f"missing required columns: {candidates}, got: {list(df.columns)}")
    return None


def format_date_str(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def load_existing_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = pd.read_csv(path)
    if "time" not in df.columns:
        raise ValueError(f"{path} missing time column")

    for col in ["open", "high", "low", "close", "amount", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = pd.NA

    if "thscode" not in df.columns:
        df["thscode"] = pd.NA

    df["time"] = pd.to_datetime(df["time"], errors="coerce").dt.normalize()
    df = df.dropna(subset=["time"])
    df = df.sort_values("time").drop_duplicates(subset=["time"], keep="last").reset_index(drop=True)
    return df[["time", "thscode", "open", "high", "low", "close", "amount", "volume"]]


def to_standard_frame(raw_df: pd.DataFrame, fallback_code: str) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        return pd.DataFrame(columns=["time", "thscode", "open", "high", "low", "close", "amount", "volume"])

    time_col = choose_column(raw_df, ["time", "date", "tradedate", "trade_date"])
    open_col = choose_column(raw_df, ["open"])
    high_col = choose_column(raw_df, ["high"])
    low_col = choose_column(raw_df, ["low"])
    close_col = choose_column(raw_df, ["close"])
    amount_col = choose_column(raw_df, ["amount", "amt", "turnover"], required=False)
    volume_col = choose_column(raw_df, ["volume", "vol"], required=False)
    thscode_col = choose_column(raw_df, ["thscode", "code", "securitycode", "symbol"], required=False)

    out = pd.DataFrame()
    out["time"] = pd.to_datetime(raw_df[time_col], errors="coerce").dt.normalize()
    out["thscode"] = raw_df[thscode_col].astype(str) if thscode_col else fallback_code
    out["open"] = pd.to_numeric(raw_df[open_col], errors="coerce")
    out["high"] = pd.to_numeric(raw_df[high_col], errors="coerce")
    out["low"] = pd.to_numeric(raw_df[low_col], errors="coerce")
    out["close"] = pd.to_numeric(raw_df[close_col], errors="coerce")
    out["amount"] = pd.to_numeric(raw_df[amount_col], errors="coerce") if amount_col else pd.NA
    out["volume"] = pd.to_numeric(raw_df[volume_col], errors="coerce") if volume_col else pd.NA

    out = out.dropna(subset=["time", "open", "high", "low", "close"])
    out = out.sort_values("time").drop_duplicates(subset=["time"], keep="last").reset_index(drop=True)
    return out


def fetch_ifind_hq(code: str, start_date: date, end_date: date) -> pd.DataFrame:
    indicators = "open,high,low,close,amount,volume"
    # period:D requests daily frequency.
    params = "period:D"
    result = THS_HQ(code, indicators, params, format_date_str(start_date), format_date_str(end_date), "format:dataframe")

    error_code = getattr(result, "errorcode", None)
    error_msg = getattr(result, "errmsg", "")
    if error_code is None:
        raise RuntimeError("THS_HQ returned unexpected object without errorcode")
    if int(error_code) != 0:
        raise RuntimeError(f"THS_HQ failed for {code}: errorcode={error_code}, errmsg={error_msg}")

    data = getattr(result, "data", None)
    if data is None:
        return pd.DataFrame(columns=["time", "thscode", "open", "high", "low", "close", "amount", "volume"])
    return to_standard_frame(data, fallback_code=code)


def merge_and_number(existing: pd.DataFrame, new_data: pd.DataFrame) -> pd.DataFrame:
    merged = pd.concat([existing, new_data], ignore_index=True)
    merged = merged.sort_values("time").drop_duplicates(subset=["time"], keep="last").reset_index(drop=True)
    merged["number"] = range(len(merged) - 1, -1, -1)
    merged["time"] = merged["time"].dt.strftime("%Y-%m-%d")
    return merged[OUTPUT_COLUMNS]


def write_outputs(df: pd.DataFrame, csv_path: Path, sync_xlsx: bool) -> None:
    df.to_csv(csv_path, index=False, float_format="%.10f")
    if sync_xlsx:
        xlsx_path = csv_path.with_suffix(".xlsx")
        try:
            df.to_excel(xlsx_path, index=False)
        except Exception as exc:
            print(f"warn: failed to write {xlsx_path}: {exc}")


def resolve_start_date(existing: pd.DataFrame, cli_start: Optional[str], default_start: str) -> date:
    if cli_start:
        return pd.to_datetime(cli_start).date()
    if not existing.empty:
        last_date = pd.to_datetime(existing["time"]).max().date()
        return last_date + timedelta(days=1)
    return pd.to_datetime(default_start).date()


def update_one_symbol(
    code: str,
    out_csv: Path,
    start_date: Optional[str],
    end_date: str,
    default_start: str,
    sync_xlsx: bool,
) -> None:
    existing = load_existing_csv(out_csv)
    begin = resolve_start_date(existing, start_date, default_start)
    end = pd.to_datetime(end_date).date()

    if begin > end:
        print(f"{code}: up-to-date, no new data needed ({begin} > {end})")
        return

    print(f"{code}: fetching {begin} -> {end} ...")
    fetched = fetch_ifind_hq(code, begin, end)
    if fetched.empty:
        print(f"{code}: no rows returned from iFinD")
        return

    merged = merge_and_number(existing, fetched)
    before = len(existing)
    after = len(merged)
    delta = after - before
    write_outputs(merged, out_csv, sync_xlsx=sync_xlsx)
    print(f"{code}: {before} -> {after} rows (delta {delta:+d}), latest={merged['time'].iloc[-1]}")
    print(f"{code}: wrote {out_csv}")


def parse_args():
    p = argparse.ArgumentParser(description="Update IC/IM daily data from iFinD")
    p.add_argument("--username", default=os.getenv("IFIND_USERNAME", ""), help="iFinD username")
    p.add_argument("--password", default=os.getenv("IFIND_PASSWORD", ""), help="iFinD password")
    p.add_argument("--ic-code", default="ICZL.CFE", help="IC symbol in iFinD")
    p.add_argument("--im-code", default="IMZL.CFE", help="IM symbol in iFinD")
    p.add_argument("--ic-out", default="IC500.csv", help="IC output csv path")
    p.add_argument("--im-out", default="IM1000.csv", help="IM output csv path")
    p.add_argument("--start", default=None, help="force start date (YYYY-MM-DD)")
    p.add_argument("--end", default=date.today().strftime("%Y-%m-%d"), help="end date (YYYY-MM-DD)")
    p.add_argument("--default-start", default="2024-01-01", help="used when output file does not exist")
    p.add_argument("--no-sync-xlsx", action="store_true", help="do not write xlsx file")
    return p.parse_args()


def main():
    args = parse_args()
    username = args.username.strip()
    password = args.password.strip()
    if not username or not password:
        raise RuntimeError("missing iFinD credentials: pass --username/--password or set IFIND_USERNAME/IFIND_PASSWORD")

    login_code = THS_iFinDLogin(username, password)
    if int(login_code) != 0:
        raise RuntimeError(f"iFinD login failed: {login_code}")

    try:
        update_one_symbol(
            code=args.ic_code,
            out_csv=Path(args.ic_out),
            start_date=args.start,
            end_date=args.end,
            default_start=args.default_start,
            sync_xlsx=(not args.no_sync_xlsx),
        )
        update_one_symbol(
            code=args.im_code,
            out_csv=Path(args.im_out),
            start_date=args.start,
            end_date=args.end,
            default_start=args.default_start,
            sync_xlsx=(not args.no_sync_xlsx),
        )
    finally:
        THS_iFinDLogout()


if __name__ == "__main__":
    main()
