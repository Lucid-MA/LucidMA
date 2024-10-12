cd /d "S:\Users\THoang\Tech\LucidMA"

:: Activate the virtual environment
call "S:\Users\THoang\Tech\LucidMA\venv\Scripts\activate.bat"

cd "S:\Users\THoang\Tech\LucidMA\Reporting\Bronze_tables"
python "Bronze_BNY_raw_tables.py"
if %ERRORLEVEL% neq 0 (
    set "status=Failed"
    echo Python script execution failed.
) else (
    set "status=Success"
)

:: Navigate to the logs directory and append the date, time, and status to the log file
cd "S:\Users\THoang\Tech\BatchLogs"
echo %date% %time% ; "Bronze_BNY_raw_tables - Script execution: %status%">>BatchLogs.txt

cd "S:\Users\THoang\Tech\LucidMA\Reporting\Silver_tables"
python "Silver_BNY_tables.py"
if %ERRORLEVEL% neq 0 (
    set "status=Failed"
    echo Python script execution failed.
) else (
    set "status=Success"
)

:: Navigate to the logs directory and append the date, time, and status to the log file
cd "S:\Users\THoang\Tech\BatchLogs"
echo %date% %time% ; "Silver_BNY_tables - Script execution: %status%">>BatchLogs.txt

:: Deactivate the virtual environment
call "S:\Users\THoang\Tech\LucidMA\venv\Scripts\deactivate.bat"