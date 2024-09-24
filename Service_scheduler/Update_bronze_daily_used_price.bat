s:
cd "S:\Users\THoang\Tech\LucidMA\Reporting\Bronze_tables\Price"
"S:\Users\THoang\Tech\LucidMA\Reporting\venv\Scripts\python.exe" "Bronze_daily_used_prices_table.py"
cd "S:\Users\THoang\Tech\LucidMA\BatchLogs"
echo (%date% ; "used_prices_table")>>BatchLogs.txt