cd /d "S:\Users\THoang\Tech\LucidMA"

:: Pull the latest changes from the repository
git pull

if %ERRORLEVEL% neq 0 (
    echo Git pull failed. Exiting script.
    exit /b 1
)

cd "S:\Users\THoang\Tech\LucidMA\Reporting\Bronze_tables"
"S:\Users\THoang\Tech\LucidMA\venv\Scripts\python.exe" "Bronze_HELIX_price_factor_table.py"
if %ERRORLEVEL% neq 0 (
    set "status=Failed"
    echo Python script execution failed.
) else (
    set "status=Success"
)

:: Navigate to the logs directory and append the date, time, and status to the log file
cd "S:\Users\THoang\Tech\LucidMA\Service_scheduler\BatchLogs"
echo %date% %time% ; "used_HELIX_prices_factor_table - Script execution: %status%">>BatchLogs.txt


:: Add the log changes to git, commit, and push them
cd /d "S:\Users\THoang\Tech\LucidMA"
git add BatchLogs.txt
git commit -m "Update daily HELIX price and factor log on %date%"
if %ERRORLEVEL% neq 0 (
    echo Git commit failed. Exiting script.
    exit /b 1
)

git push
if %ERRORLEVEL% neq 0 (
    echo Git push failed. Exiting script.
    exit /b 1
)