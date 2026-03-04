import argparse
import json
from pathlib import Path

import pandas as pd


def load_ohlc(path: Path, prefix: str) -> pd.DataFrame:
    required = ["time", "open", "high", "low", "close"]
    df = pd.read_csv(path)
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{path} missing columns: {missing}")

    df = df[required].copy()
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    for c in required[1:]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna().sort_values("time").drop_duplicates(subset=["time"])

    return df.rename(
        columns={
            "open": f"{prefix}_open",
            "high": f"{prefix}_high",
            "low": f"{prefix}_low",
            "close": f"{prefix}_close",
        }
    )


def build_ratio_ohlc(ic: pd.DataFrame, im: pd.DataFrame) -> pd.DataFrame:
    # Start plotting window from IM's first available date.
    im_start = im["time"].min()
    ic = ic[ic["time"] >= im_start].copy()
    merged = ic.merge(im, on="time", how="inner", validate="one_to_one")

    out = pd.DataFrame({"time": merged["time"]})
    out["open"] = merged["ic_open"] / merged["im_open"]
    out["close"] = merged["ic_close"] / merged["im_close"]
    ratio_high = merged["ic_high"] / merged["im_high"]
    ratio_low = merged["ic_low"] / merged["im_low"]

    # Ensure high/low always cover open/close.
    out["high"] = pd.concat([out["open"], out["close"], ratio_high, ratio_low], axis=1).max(axis=1)
    out["low"] = pd.concat([out["open"], out["close"], ratio_high, ratio_low], axis=1).min(axis=1)

    out = out.replace([float("inf"), float("-inf")], pd.NA).dropna()
    out = out.sort_values("time").reset_index(drop=True)
    out["ma3"] = out["close"].rolling(3).mean()
    out["ma5"] = out["close"].rolling(5).mean()
    return out


def to_float_list(series: pd.Series):
    return [None if pd.isna(v) else float(v) for v in series]


def make_plotly_html(df: pd.DataFrame, html_path: Path) -> None:
    payload = {
        "dates": df["time"].dt.strftime("%Y-%m-%d").tolist(),
        "open": to_float_list(df["open"]),
        "high": to_float_list(df["high"]),
        "low": to_float_list(df["low"]),
        "close": to_float_list(df["close"]),
        "ma3": to_float_list(df["ma3"]),
        "ma5": to_float_list(df["ma5"]),
    }
    latest_date = df["time"].iloc[-1].strftime("%Y-%m-%d")
    latest_close = df["close"].iloc[-1]

    html = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>IC/IM 比值K线图</title>
  <script src="https://cdn.plot.ly/plotly-2.30.0.min.js"></script>
  <style>
    body {
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
      background: #f7f9fc;
      color: #1f2937;
    }
    .wrap {
      max-width: 1280px;
      margin: 0 auto;
      padding: 16px;
    }
    .meta {
      margin: 0 0 10px;
      font-size: 14px;
      color: #4b5563;
    }
    #chart {
      width: 100%;
      height: min(78vh, 820px);
      background: #fff;
      border: 1px solid #e5e7eb;
      border-radius: 10px;
    }
  </style>
</head>
<body>
  <div class="wrap">
    <p class="meta">最新收盘(IC/IM)：<b>__LATEST_CLOSE__</b>（__LATEST_DATE__）</p>
    <div id="chart"></div>
  </div>
  <script>
    const d = __PAYLOAD_JSON__;
    const candle = {
      type: "candlestick",
      x: d.dates,
      open: d.open,
      high: d.high,
      low: d.low,
      close: d.close,
      name: "IC/IM",
      increasing: {line: {color: "#d1495b"}, fillcolor: "#d1495b"},
      decreasing: {line: {color: "#2a9d8f"}, fillcolor: "#2a9d8f"},
      whiskerwidth: 0.35
    };
    const ma3 = {
      type: "scatter",
      mode: "lines",
      x: d.dates,
      y: d.ma3,
      name: "MA3",
      line: {color: "#f59e0b", width: 1.2}
    };
    const ma5 = {
      type: "scatter",
      mode: "lines",
      x: d.dates,
      y: d.ma5,
      name: "MA5",
      line: {color: "#1d4ed8", width: 1.2}
    };

    const layout = {
      title: "IC/IM 比值K线图（日线）",
      template: "plotly_white",
      hovermode: "x unified",
      margin: {l: 64, r: 24, t: 56, b: 48},
      xaxis: {
        type: "date",
        rangeslider: {visible: true},
        rangeselector: {
          buttons: [
            {count: 1, label: "1月", step: "month", stepmode: "backward"},
            {count: 3, label: "3月", step: "month", stepmode: "backward"},
            {count: 6, label: "6月", step: "month", stepmode: "backward"},
            {count: 1, label: "1年", step: "year", stepmode: "backward"},
            {step: "all", label: "全部"}
          ]
        }
      },
      yaxis: {title: "比值"}
    };

    Plotly.newPlot("chart", [candle, ma3, ma5], layout, {
      responsive: true,
      displaylogo: false,
      modeBarButtonsToRemove: ["select2d", "lasso2d"]
    });
  </script>
</body>
</html>
"""

    html = html.replace("__PAYLOAD_JSON__", json.dumps(payload, ensure_ascii=False))
    html = html.replace("__LATEST_DATE__", latest_date)
    html = html.replace("__LATEST_CLOSE__", f"{latest_close:.6f}")
    html_path.write_text(html, encoding="utf-8")


def make_tradingview_html(df: pd.DataFrame, html_path: Path) -> None:
    candle_data = [
        {
            "time": t.strftime("%Y-%m-%d"),
            "open": float(o),
            "high": float(h),
            "low": float(l),
            "close": float(c),
        }
        for t, o, h, l, c in zip(df["time"], df["open"], df["high"], df["low"], df["close"])
    ]
    ma3_data = [
        {"time": t.strftime("%Y-%m-%d"), "value": float(v)}
        for t, v in zip(df["time"], df["ma3"])
        if not pd.isna(v)
    ]
    ma5_data = [
        {"time": t.strftime("%Y-%m-%d"), "value": float(v)}
        for t, v in zip(df["time"], df["ma5"])
        if not pd.isna(v)
    ]

    latest_date = df["time"].iloc[-1].strftime("%Y-%m-%d")
    latest_close = df["close"].iloc[-1]

    payload = {
        "candles": candle_data,
        "ma3": ma3_data,
        "ma5": ma5_data,
    }

    html = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>IC/IM 比值K线图（TradingView）</title>
  <script src="https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.production.js"></script>
  <style>
    :root {
      --bg: #f4f6fb;
      --fg: #101827;
      --card: #ffffff;
      --line: #dbe2ea;
      --up: #d1495b;
      --down: #2a9d8f;
      --ma3: #f59e0b;
      --ma5: #1d4ed8;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: radial-gradient(circle at top left, #eef4ff 0%, var(--bg) 45%, #edf7f5 100%);
      color: var(--fg);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
    }
    .wrap {
      max-width: 1280px;
      margin: 0 auto;
      padding: 16px;
    }
    .title {
      margin: 0 0 6px;
      font-size: 18px;
      font-weight: 600;
    }
    .meta {
      margin: 0 0 12px;
      color: #4b5563;
      font-size: 14px;
    }
    .card {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 12px;
      overflow: hidden;
      box-shadow: 0 4px 14px rgba(16, 24, 39, 0.06);
    }
    .toolbar {
      display: flex;
      gap: 8px;
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      flex-wrap: wrap;
    }
    .toolbar button {
      border: 1px solid #c9d4e0;
      background: #f8fafc;
      color: #0f172a;
      border-radius: 8px;
      padding: 6px 10px;
      cursor: pointer;
      font-size: 13px;
      line-height: 1;
      min-height: 30px;
    }
    .toolbar button:hover {
      background: #eef4fb;
    }
    .detail {
      display: flex;
      flex-wrap: wrap;
      gap: 14px;
      padding: 8px 12px;
      border-bottom: 1px solid var(--line);
      font-size: 13px;
      color: #1f2937;
      background: #fcfdff;
    }
    .detail .label {
      color: #6b7280;
      margin-right: 4px;
    }
    .detail .up {
      color: #d1495b;
      font-weight: 600;
    }
    .detail .down {
      color: #2a9d8f;
      font-weight: 600;
    }
    #chart {
      width: 100%;
      height: min(78vh, 840px);
    }
  </style>
</head>
<body>
  <div class="wrap">
    <h1 class="title">IC/IM 比值K线图（TradingView Lightweight Charts）</h1>
    <p class="meta">最新收盘(IC/IM)：<b>__LATEST_CLOSE__</b>（__LATEST_DATE__）</p>
    <div class="card">
      <div class="toolbar">
        <button data-range="60">近3月</button>
        <button data-range="120">近6月</button>
        <button data-range="250">近1年</button>
        <button data-range="all">全部</button>
      </div>
      <div id="k-detail" class="detail"></div>
      <div id="chart"></div>
    </div>
  </div>

  <script>
    const payload = __PAYLOAD_JSON__;
    const chartEl = document.getElementById("chart");
    const detailEl = document.getElementById("k-detail");
    const chart = LightweightCharts.createChart(chartEl, {
      width: chartEl.clientWidth,
      height: chartEl.clientHeight,
      layout: {
        background: { color: "#ffffff" },
        textColor: "#111827"
      },
      rightPriceScale: {
        borderVisible: true,
        borderColor: "#dbe2ea"
      },
      timeScale: {
        borderVisible: true,
        borderColor: "#dbe2ea",
        timeVisible: true
      },
      grid: {
        vertLines: { color: "#eef2f7" },
        horzLines: { color: "#eef2f7" }
      },
      crosshair: {
        mode: LightweightCharts.CrosshairMode.Normal
      },
      localization: {
        priceFormatter: (p) => p.toFixed(6)
      }
    });

    const candleSeries = chart.addCandlestickSeries({
      upColor: "#d1495b",
      downColor: "#2a9d8f",
      borderVisible: true,
      wickUpColor: "#d1495b",
      wickDownColor: "#2a9d8f"
    });

    const ma3Series = chart.addLineSeries({
      color: "#f59e0b",
      lineWidth: 1.2,
      priceLineVisible: false,
      crosshairMarkerVisible: false
    });

    const ma5Series = chart.addLineSeries({
      color: "#1d4ed8",
      lineWidth: 1.2,
      priceLineVisible: false,
      crosshairMarkerVisible: false
    });

    candleSeries.setData(payload.candles);
    ma3Series.setData(payload.ma3);
    ma5Series.setData(payload.ma5);
    chart.timeScale().fitContent();

    function formatTime(time) {
      if (typeof time === "string") return time;
      if (typeof time === "number") return new Date(time * 1000).toISOString().slice(0, 10);
      if (time && typeof time === "object" && "year" in time) {
        const y = String(time.year).padStart(4, "0");
        const m = String(time.month).padStart(2, "0");
        const d = String(time.day).padStart(2, "0");
        return `${y}-${m}-${d}`;
      }
      return "";
    }

    function fmt(num) {
      return Number(num).toFixed(6);
    }

    function setDetail(bar, timeValue) {
      if (!bar) return;
      const dateText = formatTime(timeValue || bar.time);
      const change = bar.close - bar.open;
      const changePct = bar.open ? (change / bar.open) * 100 : 0;
      const amplitudePct = bar.open ? ((bar.high - bar.low) / bar.open) * 100 : 0;
      const cls = change >= 0 ? "up" : "down";
      const sign = change >= 0 ? "+" : "";
      detailEl.innerHTML =
        `<span><span class="label">日期</span>${dateText}</span>` +
        `<span><span class="label">开</span>${fmt(bar.open)}</span>` +
        `<span><span class="label">高</span>${fmt(bar.high)}</span>` +
        `<span><span class="label">低</span>${fmt(bar.low)}</span>` +
        `<span><span class="label">收</span>${fmt(bar.close)}</span>` +
        `<span class="${cls}"><span class="label">涨跌</span>${sign}${fmt(change)}</span>` +
        `<span class="${cls}"><span class="label">涨跌幅</span>${sign}${changePct.toFixed(2)}%</span>` +
        `<span><span class="label">振幅</span>${amplitudePct.toFixed(2)}%</span>`;
    }

    const latestBar = payload.candles[payload.candles.length - 1];
    setDetail(latestBar, latestBar ? latestBar.time : "");

    chart.subscribeCrosshairMove((param) => {
      const point = param && param.seriesData ? param.seriesData.get(candleSeries) : null;
      if (point && point.open != null && point.high != null && point.low != null && point.close != null) {
        setDetail(point, param.time);
      } else {
        setDetail(latestBar, latestBar ? latestBar.time : "");
      }
    });

    const rangeButtons = Array.from(document.querySelectorAll("button[data-range]"));
    rangeButtons.forEach((btn) => {
      btn.addEventListener("click", () => {
        const v = btn.getAttribute("data-range");
        if (v === "all") {
          chart.timeScale().fitContent();
          return;
        }
        const n = Number(v);
        const total = payload.candles.length;
        if (!Number.isFinite(n) || total === 0) return;
        const startIdx = Math.max(0, total - n);
        chart.timeScale().setVisibleLogicalRange({
          from: startIdx - 1,
          to: total + 2
        });
      });
    });

    window.addEventListener("resize", () => {
      chart.applyOptions({
        width: chartEl.clientWidth,
        height: chartEl.clientHeight
      });
    });
  </script>
</body>
</html>
"""

    html = html.replace("__PAYLOAD_JSON__", json.dumps(payload, ensure_ascii=False))
    html = html.replace("__LATEST_DATE__", latest_date)
    html = html.replace("__LATEST_CLOSE__", f"{latest_close:.6f}")
    html_path.write_text(html, encoding="utf-8")


def parse_args():
    parser = argparse.ArgumentParser(description="Generate interactive IC/IM ratio kline")
    parser.add_argument("--ic", default="IC500.csv", help="IC csv path")
    parser.add_argument("--im", default="IM1000.csv", help="IM csv path")
    parser.add_argument(
        "--engine",
        choices=["plotly", "tradingview"],
        default="plotly",
        help="chart engine",
    )
    parser.add_argument("--out-html", default="ic_im_ratio_kline.html", help="output html path")
    parser.add_argument("--out-csv", default="ic_im_ratio_ohlc.csv", help="output csv path")
    return parser.parse_args()


def main():
    args = parse_args()
    ic_path = Path(args.ic)
    im_path = Path(args.im)
    out_html = Path(args.out_html)
    out_csv = Path(args.out_csv)

    if not ic_path.exists():
        raise FileNotFoundError(f"missing file: {ic_path}")
    if not im_path.exists():
        raise FileNotFoundError(f"missing file: {im_path}")

    ic_df = load_ohlc(ic_path, "ic")
    im_df = load_ohlc(im_path, "im")
    ratio_df = build_ratio_ohlc(ic_df, im_df)
    if ratio_df.empty:
        raise RuntimeError("empty result after merge")

    ratio_df.to_csv(out_csv, index=False, float_format="%.10f")

    if args.engine == "plotly":
        make_plotly_html(ratio_df, out_html)
    else:
        make_tradingview_html(ratio_df, out_html)

    print(f"generated html: {out_html}")
    print(f"generated csv : {out_csv}")
    print(
        "date range     : "
        f"{ratio_df['time'].iloc[0].date()} ~ {ratio_df['time'].iloc[-1].date()} "
        f"({len(ratio_df)} rows)"
    )


if __name__ == "__main__":
    main()
