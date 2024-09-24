cd /d "S:\Users\THoang\Tech\LucidMA"
git pull

echo Current PYTHONPATH before: %PYTHONPATH%
set PYTHONPATH=%PYTHONPATH%;S:\Users\THoang\Tech\LucidMA
echo Modified PYTHONPATH: %PYTHONPATH%

cd "S:\Users\THoang\Tech\LucidMA\Reporting\Bronze_tables\Price"
"S:\Users\THoang\Tech\LucidMA\Reporting\venv\Scripts\python.exe" "Bronze_daily_used_prices_table.py"

echo PYTHONPATH after script: %PYTHONPATH%

cd "S:\Users\THoang\Tech\LucidMA\BatchLogs"
echo %date% ; "used_prices_table">>BatchLogs.txt

cd /d "S:\Users\THoang\Tech\LucidMA"
git add .
git commit -m "update daily price %date%"
git push