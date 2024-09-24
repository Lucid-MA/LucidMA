cd /d "S:\Users\THoang\Tech\LucidMA"
git pull

cd /d "S:\Users\THoang\Tech\LucidMA\Reporting\Bronze_tables\Price"
"S:\Users\THoang\Tech\LucidMA\venv\Scripts\python.exe" "Bronze_daily_used_prices_table.py"

cd /d "S:\Users\THoang\Tech\LucidMA\BatchLogs"
echo (%date% ; "used_prices_table")>>BatchLogs.txt

cd /d "S:\Users\THoang\Tech\LucidMA"
git add .
git commit -m "update daily price %date%"
git push