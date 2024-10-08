cd /d "S:\Users\THoang\Tech\LucidMA"

cd "S:\Users\THoang\Tech\LucidMA\Reporting\Bronze_tables"
"S:\Users\THoang\Tech\LucidMA\venv\Scripts\python.exe" "Bronze_HELIX_price_factor_table.py"
if %ERRORLEVEL% neq 0 (
    set "status=Failed"
    echo Python script execution failed.
) else (
    set "status=Success"
)

:: Navigate to the logs directory and append the date, time, and status to the log file
cd "S:\Users\THoang\Tech\BatchLogs"
echo %date% %time% ; "used_HELIX_prices_factor_table - Script execution: %status%">>BatchLogs.txt