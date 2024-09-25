cd /d "S:\Users\THoang\Tech\LucidMA"
git pull

echo Current PYTHONPATH before: %PYTHONPATH%
set PYTHONPATH=%PYTHONPATH%;S:\Users\THoang\Tech\LucidMA
echo Modified PYTHONPATH: %PYTHONPATH%

cd "S:\Users\THoang\Tech\LucidMA\Reporting\Bronze_tables"
S:\Users\THoang\Tech\LucidMA\Reporting\venv\Scripts\python.exe "Bronze_cash_balance_table.py"

echo PYTHONPATH after script: %PYTHONPATH%

cd "S:\Users\THoang\Tech\LucidMA\BatchLogs"
echo %date% ; "cash_balance_table">>BatchLogs.txt

cd /d "S:\Users\THoang\Tech\LucidMA"
git add .
git commit -m "update cash balance %date%"
git push