cd /d "S:\Users\THoang\Tech\LucidMA"

cd "S:\Users\THoang\Tech\LucidMA\Reporting\Silver_tables"
"S:\Users\THoang\Tech\LucidMA\venv\Scripts\python.exe" "Silver_bloomberg_factor_interest_accrued_table.py"
if %ERRORLEVEL% neq 0 (
    set "status=Failed"
    echo Python script execution failed.
) else (
    set "status=Success"
)

:: Navigate to the logs directory and append the date, time, and status to the log file
cd "S:\Users\THoang\Tech\LucidMA\Service_scheduler\BatchLogs"
echo %date% %time% ; "silver_bloomberg_factor_interest_accrued_table - Script execution: %status%">>BatchLogs.txt

cd "S:\Users\THoang\Tech\LucidMA\Reporting\Silver_tables"
"S:\Users\THoang\Tech\LucidMA\venv\Scripts\python.exe" "Silver_clean_and_dirty_prices_table.py"
if %ERRORLEVEL% neq 0 (
    set "status=Failed"
    echo Python script execution failed.
) else (
    set "status=Success"
)

:: Navigate to the logs directory and append the date, time, and status to the log file
cd "S:\Users\THoang\Tech\LucidMA\Service_scheduler\BatchLogs"
echo %date% %time% ; "silver_clean_and_dirty_price - Script execution: %status%">>BatchLogs.txt
