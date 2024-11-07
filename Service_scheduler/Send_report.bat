cd /d "S:\Users\THoang\Tech\LucidMA"

echo Current PYTHONPATH before: %PYTHONPATH%
set PYTHONPATH=%PYTHONPATH%;S:\Users\THoang\Tech\LucidMA
echo Modified PYTHONPATH: %PYTHONPATH%

cd "S:\Users\THoang\Tech\LucidMA\Reporting\Daily_report"
"S:\Users\THoang\Tech\LucidMA\venv\Scripts\python.exe" "Trade_allocations_report.py"
if %ERRORLEVEL% neq 0 (
    set "status=Failed"
    echo Python script execution failed.
) else (
    set "status=Success"
)

:: Navigate to the logs directory and append the date, time, and status to the log file
cd "S:\Users\THoang\Tech\BatchLogs"
echo %date% %time% ; "send_trade_allocation_report - Script execution: %status%">>BatchLogs.txt