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
    echo Python script execution failed. Exiting script.
    pause
    exit /b 1
)

pause

:: Navigate to the logs directory and append the date to the log file
cd "S:\Users\THoang\Tech\LucidMA\Service_scheduler\BatchLogs"
echo %date% ; "used_HELIX_prices_factor_table">>BatchLogs.txt

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