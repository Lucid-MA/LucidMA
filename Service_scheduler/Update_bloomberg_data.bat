cd /d "C:\LucidMA"

:: Activate the virtual environment
call "C:\LucidMA\venv\Scripts\activate.bat"

:: First Python script execution
cd /d "C:\LucidMA\Reporting\Bronze_tables\Price"
python "bronze_daily_bloomberg_collateral_data_table.py"
if %ERRORLEVEL% neq 0 (
    set "status=Failed"
    echo Python script execution failed for bronze_daily_bloomberg_collateral_data_table.py
) else (
    set "status=Success"
)



:: Log the result of the first script
cd /d "S:\Users\THoang\Tech\BatchLogs"
echo %date% %time% ; "bb_collateral_data_fetch - Script execution: %status%">>BatchLogs.txt

:: Second Python script execution
cd /d "C:\LucidMA\Reporting\Bronze_tables"
python "Bronze_daily_bloomberg_rates_table.py"
if %ERRORLEVEL% neq 0 (
    set "status=Failed"
    echo Python script execution failed for Bronze_daily_bloomberg_rates_table.py
) else (
    set "status=Success"
)

:: Log the result of the second script
cd /d "S:\Users\THoang\Tech\BatchLogs"
echo %date% %time% ; "bb_rates_data_fetch - Script execution: %status%">>BatchLogs.txt


:: Third Python script execution
cd /d "C:\LucidMA\Reporting\Silver_tables"
python "Silver_collateral_ratings_table.py"
if %ERRORLEVEL% neq 0 (
    set "status=Failed"
    echo Python script execution failed for Silver_collateral_ratings_table.py
) else (
    set "status=Success"
)

:: Log the result of the third script
cd /d "S:\Users\THoang\Tech\BatchLogs"
echo %date% %time% ; "silver_collateral_rating - Script execution: %status%">>BatchLogs.txt