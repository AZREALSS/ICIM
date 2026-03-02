@echo off
setlocal

cd /d "%~dp0"

if "%IFIND_USERNAME%"=="" (
  echo [ERROR] IFIND_USERNAME is empty.
  echo Set IFIND_USERNAME and IFIND_PASSWORD first.
  exit /b 1
)

if "%IFIND_PASSWORD%"=="" (
  echo [ERROR] IFIND_PASSWORD is empty.
  echo Set IFIND_USERNAME and IFIND_PASSWORD first.
  exit /b 1
)

python update_ic_im_ifind.py
if errorlevel 1 (
  echo [ERROR] update failed.
  exit /b 1
)

python plot_ic_im_ratio_kline.py --engine tradingview --ic IC500.csv --im IM1000.csv --out-csv ic_im_ratio_ohlc.csv --out-html ic_im_ratio_kline.html
if errorlevel 1 (
  echo [ERROR] ratio kline generation failed.
  exit /b 1
)

echo [OK] IC/IM data updated and ratio kline regenerated.
exit /b 0
