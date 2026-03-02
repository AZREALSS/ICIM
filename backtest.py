import pandas as pd
from pathlib import Path

# Files
FILE_518800 = '518800.xlsx'
FILE_1000 = '中证1000.xlsx'
FILE_500 = '中证500.xlsx'
FILE_IC500 = 'IC500.xlsx'
FILE_IM1000 = 'IM1000.xlsx'
FILE_SSE = '上证指数.xlsx'
PLOT_FILE = 'returns_vs_sse.png'

# Optional backtest window (inclusive). Use None for no bound.
START_DATE = '2024-01-01'  # e.g. '2022-01-01'
END_DATE = '2026-01-01'    # e.g. '2025-12-31'


def load_clean(path):
    df = pd.read_excel(path)
    # Keep expected columns and drop trailing source rows
    if 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time'], errors='coerce')
    df = df.dropna(subset=['time', 'close'])
    # Ensure numeric close
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    df = df.dropna(subset=['close'])
    # Sort ascending by date
    df = df.sort_values('time').reset_index(drop=True)
    return df


def to_csv_basename(xlsx_path):
    return Path(xlsx_path).with_suffix('.csv')


def safe_to_csv(df, path):
    try:
        df.to_csv(path, index=False)
        return path
    except PermissionError:
        alt = Path(path).with_name(f"{Path(path).stem}_out{Path(path).suffix}")
        df.to_csv(alt, index=False)
        print(f"Permission denied writing {path}. Wrote {alt} instead.")
        return str(alt)


def apply_date_filter(df, start_date, end_date):
    if start_date is not None:
        df = df[df['time'] >= pd.to_datetime(start_date)]
    if end_date is not None:
        df = df[df['time'] <= pd.to_datetime(end_date)]
    return df


def main():
    for f in [FILE_518800, FILE_1000, FILE_500, FILE_IC500, FILE_IM1000, FILE_SSE]:
        if not Path(f).exists():
            raise FileNotFoundError(f"Missing file: {f}")

    df_518800 = apply_date_filter(load_clean(FILE_518800), START_DATE, END_DATE)
    df_1000 = apply_date_filter(load_clean(FILE_1000), START_DATE, END_DATE)
    df_500 = apply_date_filter(load_clean(FILE_500), START_DATE, END_DATE)
    df_ic500 = apply_date_filter(load_clean(FILE_IC500), START_DATE, END_DATE)
    df_im1000 = apply_date_filter(load_clean(FILE_IM1000), START_DATE, END_DATE)
    df_sse = apply_date_filter(load_clean(FILE_SSE), START_DATE, END_DATE)

    # Export all input files to CSV
    safe_to_csv(df_518800, to_csv_basename(FILE_518800))
    safe_to_csv(df_1000, to_csv_basename(FILE_1000))
    safe_to_csv(df_500, to_csv_basename(FILE_500))
    safe_to_csv(df_ic500, to_csv_basename(FILE_IC500))
    safe_to_csv(df_im1000, to_csv_basename(FILE_IM1000))
    safe_to_csv(df_sse, to_csv_basename(FILE_SSE))

    # Compute daily returns
    df_518800['ret_518800'] = df_518800['close'].pct_change()
    df_1000['ret_1000'] = df_1000['close'].pct_change()
    df_500['ret_500'] = df_500['close'].pct_change()
    df_ic500['ret_ic500'] = df_ic500['close'].pct_change()
    df_im1000['ret_im1000'] = df_im1000['close'].pct_change()

    # Merge on time
    df = df_1000[['time', 'close', 'ret_1000']].merge(
        df_518800[['time', 'ret_518800']], on='time', how='inner'
    ).merge(
        df_500[['time', 'close', 'ret_500']], on='time', how='inner', suffixes=('_1000', '_500')
    )

    # Need next-day close for exit
    df['next_time'] = df['time'].shift(-1)
    df['next_close_1000'] = df['close_1000'].shift(-1)
    df['next_close_500'] = df['close_500'].shift(-1)

    # Keep rows where we can form a trade (today signal + next day exit)
    df = df.dropna(subset=['ret_1000', 'ret_518800', 'ret_500', 'next_time', 'next_close_1000', 'next_close_500'])

    # Signal: compare today's returns
    df['signal'] = 0
    df.loc[df['ret_1000'] > df['ret_518800'], 'signal'] = 1   # long 1000, short 500
    df.loc[df['ret_1000'] < df['ret_518800'], 'signal'] = -1  # short 1000, long 500

    # If equal, no trade
    trades = df[df['signal'] != 0].copy()

    # Next-day returns for each leg
    trades['ret_next_1000'] = trades['next_close_1000'] / trades['close_1000'] - 1
    trades['ret_next_500'] = trades['next_close_500'] / trades['close_500'] - 1

    # Portfolio return: long-short pair
    trades['trade_ret'] = trades['signal'] * trades['ret_next_1000'] + (-trades['signal']) * trades['ret_next_500']

    # Save trade details to Excel
    trades_out = trades.copy()
    trades_out['direction'] = trades_out['signal'].map({1: 'long_1000_short_500', -1: 'short_1000_long_500'})
    out_cols = [
        'time',
        'direction',
        'signal',
        'close_1000',
        'close_500',
        'next_close_1000',
        'next_close_500',
        'ret_next_1000',
        'ret_next_500',
        'trade_ret',
    ]
    safe_to_csv(trades_out[out_cols], 'trade_details.csv')

    total_trades = len(trades)
    win_rate = (trades['trade_ret'] > 0).mean() if total_trades else 0.0
    total_pnl = trades['trade_ret'].sum() if total_trades else 0.0
    avg_ret = trades['trade_ret'].mean() if total_trades else 0.0

    print("Spot-based execution (spot 1000/500):")
    print(f"Trades: {total_trades}")
    print(f"Win rate: {win_rate:.2%}")
    print(f"Total P&L (sum of returns): {total_pnl:.4f}")
    print(f"Average return per trade: {avg_ret:.4f}")

    # Spot signal, futures execution
    df_fut = df_1000[['time', 'ret_1000']].merge(
        df_518800[['time', 'ret_518800']], on='time', how='inner'
    ).merge(
        df_im1000[['time', 'close', 'ret_im1000']], on='time', how='inner'
    ).merge(
        df_ic500[['time', 'close', 'ret_ic500']], on='time', how='inner', suffixes=('_im1000', '_ic500')
    )

    df_fut['next_time'] = df_fut['time'].shift(-1)
    df_fut['next_close_im1000'] = df_fut['close_im1000'].shift(-1)
    df_fut['next_close_ic500'] = df_fut['close_ic500'].shift(-1)

    df_fut = df_fut.dropna(subset=[
        'ret_1000', 'ret_518800', 'next_time', 'next_close_im1000', 'next_close_ic500'
    ])

    df_fut['signal'] = 0
    df_fut.loc[df_fut['ret_1000'] > df_fut['ret_518800'], 'signal'] = 1
    df_fut.loc[df_fut['ret_1000'] < df_fut['ret_518800'], 'signal'] = -1

    trades_fut = df_fut[df_fut['signal'] != 0].copy()
    trades_fut['ret_next_im1000'] = trades_fut['next_close_im1000'] / trades_fut['close_im1000'] - 1
    trades_fut['ret_next_ic500'] = trades_fut['next_close_ic500'] / trades_fut['close_ic500'] - 1
    trades_fut['trade_ret'] = trades_fut['signal'] * trades_fut['ret_next_im1000'] + (-trades_fut['signal']) * trades_fut['ret_next_ic500']

    trades_fut['direction'] = trades_fut['signal'].map({1: 'long_IM1000_short_IC500', -1: 'short_IM1000_long_IC500'})
    out_cols_fut = [
        'time',
        'direction',
        'signal',
        'close_im1000',
        'close_ic500',
        'next_close_im1000',
        'next_close_ic500',
        'ret_next_im1000',
        'ret_next_ic500',
        'trade_ret',
    ]
    safe_to_csv(trades_fut[out_cols_fut], 'trade_details_futures.csv')

    total_trades_fut = len(trades_fut)
    win_rate_fut = (trades_fut['trade_ret'] > 0).mean() if total_trades_fut else 0.0
    total_pnl_fut = trades_fut['trade_ret'].sum() if total_trades_fut else 0.0
    avg_ret_fut = trades_fut['trade_ret'].mean() if total_trades_fut else 0.0

    print("Futures execution (IM/IC):")
    print(f"Trades: {total_trades_fut}")
    print(f"Win rate: {win_rate_fut:.2%}")
    print(f"Total P&L (sum of returns): {total_pnl_fut:.4f}")
    print(f"Average return per trade: {avg_ret_fut:.4f}")

    # Plot cumulative returns vs SSE Index
    import matplotlib.pyplot as plt

    # Spot strategy daily returns realized on exit day
    strat_spot = trades[['next_time', 'trade_ret']].copy()
    strat_spot = strat_spot.rename(columns={'next_time': 'time'})
    strat_spot = strat_spot.groupby('time', as_index=False)['trade_ret'].sum()
    strat_spot = strat_spot.sort_values('time')
    strat_spot['cum_ret'] = (1 + strat_spot['trade_ret']).cumprod() - 1

    # Strategy daily returns realized on exit day
    strat_daily = trades_fut[['next_time', 'trade_ret']].copy()
    strat_daily = strat_daily.rename(columns={'next_time': 'time'})
    strat_daily = strat_daily.groupby('time', as_index=False)['trade_ret'].sum()
    strat_daily = strat_daily.sort_values('time')
    strat_daily['cum_ret'] = (1 + strat_daily['trade_ret']).cumprod() - 1

    # SSE index cumulative returns
    df_sse['ret_sse'] = df_sse['close'].pct_change()
    sse = df_sse.dropna(subset=['ret_sse']).copy()
    sse['cum_ret'] = (1 + sse['ret_sse']).cumprod() - 1

    # Long-only IM/IC cumulative returns
    df_im1000['ret_im1000'] = df_im1000['close'].pct_change()
    im = df_im1000.dropna(subset=['ret_im1000']).copy()
    im['cum_ret'] = (1 + im['ret_im1000']).cumprod() - 1

    df_ic500['ret_ic500'] = df_ic500['close'].pct_change()
    ic = df_ic500.dropna(subset=['ret_ic500']).copy()
    ic['cum_ret'] = (1 + ic['ret_ic500']).cumprod() - 1

    plt.figure(figsize=(10, 5))
    plt.plot(strat_daily['time'], strat_daily['cum_ret'], label='Strategy (Futures)')
    plt.plot(strat_spot['time'], strat_spot['cum_ret'], label='Strategy (Spot)')
    plt.plot(sse['time'], sse['cum_ret'], label='SSE Index')
    plt.plot(im['time'], im['cum_ret'], label='IM Long Only')
    plt.plot(ic['time'], ic['cum_ret'], label='IC Long Only')
    plt.title('Cumulative Returns vs SSE Index')
    plt.xlabel('Date')
    plt.ylabel('Cumulative Return')
    plt.legend()
    plt.tight_layout()
    plt.savefig(PLOT_FILE, dpi=150)


if __name__ == '__main__':
    main()
