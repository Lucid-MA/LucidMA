cd /d "S:\Users\THoang\Tech\LucidMA"

:: Pull the latest changes from the repository
git pull

if %ERRORLEVEL% neq 0 (
    echo Git pull failed. Exiting script.
    exit /b 1
)

echo Current PYTHONPATH before: %PYTHONPATH%
set PYTHONPATH=%PYTHONPATH%;S:\Users\THoang\Tech\LucidMA
echo Modified PYTHONPATH: %PYTHONPATH%

cd "S:\Users\THoang\Tech\LucidMA\Reporting\Bronze_tables"
"S:\Users\THoang\Tech\LucidMA\venv\Scripts\python.exe" "Bronze_cash_balance_table.py"
if %ERRORLEVEL% neq 0 (
    echo Python script execution failed. Exiting script.
    exit /b 1
)

echo PYTHONPATH after script: %PYTHONPATH%


:: Navigate to the logs directory and append the date to the log file
cd "S:\Users\THoang\Tech\LucidMA\Service_scheduler\BatchLogs"
echo %date% ; "cash_balance_table">>BatchLogs.txt

:: Add the log changes to git, commit, and push them
cd /d "S:\Users\THoang\Tech\LucidMA"
git add BatchLogs.txt
git commit -m "Update cash balance log on %date%"
if %ERRORLEVEL% neq 0 (
    echo Git commit failed. Exiting script.
    exit /b 1
)

git push
if %ERRORLEVEL% neq 0 (
    echo Git push failed. Exiting script.
    exit /b 1
)