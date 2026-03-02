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

where git >nul 2>nul
if errorlevel 1 (
  echo [ERROR] git not found in PATH.
  exit /b 1
)

git rev-parse --is-inside-work-tree >nul 2>nul
if errorlevel 1 (
  echo [ERROR] current folder is not a git repository.
  exit /b 1
)

for /f %%i in ('git rev-parse --abbrev-ref HEAD') do set "CUR_BRANCH=%%i"
if "%CUR_BRANCH%"=="" (
  echo [ERROR] cannot detect current git branch.
  exit /b 1
)

git add IC500.csv IM1000.csv IC500.xlsx IM1000.xlsx ic_im_ratio_ohlc.csv ic_im_ratio_kline.html
if errorlevel 1 (
  echo [ERROR] git add failed.
  exit /b 1
)

git diff --cached --quiet
set "GIT_DIFF_EXIT=%ERRORLEVEL%"

if "%GIT_DIFF_EXIT%"=="0" (
  echo [OK] no file changes to commit.
  echo [OK] IC/IM data updated and ratio kline regenerated.
  exit /b 0
)

if not "%GIT_DIFF_EXIT%"=="1" (
  echo [ERROR] git diff --cached failed.
  exit /b 1
)

git commit -m "daily auto update: IC IM and ratio chart"
if errorlevel 1 (
  echo [ERROR] git commit failed.
  exit /b 1
)

git push origin %CUR_BRANCH%
if errorlevel 1 (
  echo [ERROR] git push failed.
  exit /b 1
)

echo [OK] IC/IM data updated, ratio kline regenerated, and pushed to origin/%CUR_BRANCH%.
exit /b 0
