cd /d "S:\Users\THoang\Tech\LucidMA"

echo Current PYTHONPATH before: %PYTHONPATH%
set PYTHONPATH=%PYTHONPATH%;S:\Users\THoang\Tech\LucidMA
echo Modified PYTHONPATH: %PYTHONPATH%

cd "S:\Users\THoang\Tech\LucidMA\Reporting\Bronze_tables"
"S:\Users\THoang\Tech\LucidMA\venv\Scripts\python.exe" "Bronze_bond_data_table.py"
if %ERRORLEVEL% neq 0 (
    set "status=Failed"
    echo Python script execution failed.
) else (
    set "status=Success"
)

:: Navigate to the logs directory and append the date, time, and status to the log file
cd "S:\Users\THoang\Tech\BatchLogs"
echo %date% %time% ; "bronze_bond_data - Script execution: %status%">>BatchLogs.txt