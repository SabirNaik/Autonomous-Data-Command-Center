@echo off
TITLE 🚀 Retail Analytics Automation Pipeline
color 0A

:: --- 1. FORCE CORRECT FOLDER ---
cd /d "C:\pg_cron_project"

echo ======================================================
echo    STARTING AUTOMATED DATA PIPELINE (Retail 360)
echo ======================================================
echo.

:: --- STEP 1: REFRESH DASHBOARD ---
echo [Step 1/3] 🔄 Refreshing Power BI Dashboard...
echo ------------------------------------------------------
if exist refresh_dashboard.py (
    python refresh_dashboard.py
    IF %ERRORLEVEL% NEQ 0 GOTO ERROR
) else (
    echo ❌ ERROR: refresh_dashboard.py missing!
    goto ERROR
)

:: --- STEP 2: DATA QUALITY ALERT ---
echo.
echo [Step 2/3] 🚨 Checking Data Quality and Sending Alerts...
echo ------------------------------------------------------
if exist email_alert_ai.py (
    python email_alert_ai.py
    :: We do NOT goto ERROR here. If DQ fails, we still want the CEO Report.
    IF %ERRORLEVEL% NEQ 0 echo ⚠️ DQ Script had a warning, but proceeding...
) else (
    echo ⚠️ WARNING: email_alert_ai.py not found. Skipping.
)

:: --- STEP 3: EXECUTIVE BRIEF ---
echo.
echo [Step 3/3] 📊 Generating CEO Executive Brief...
echo ------------------------------------------------------
if exist daily_brief.py (
    python daily_brief.py
    IF %ERRORLEVEL% NEQ 0 GOTO ERROR
) else (
    echo ❌ ERROR: daily_brief.py missing!
    goto ERROR
)

echo.
echo ======================================================
echo    ✅ PIPELINE COMPLETED SUCCESSFULLY!
echo ======================================================
echo.
timeout /t 10
exit

:ERROR
color 0C
echo.
echo ❌ CRITICAL FAILURE! The pipeline stopped.
pause