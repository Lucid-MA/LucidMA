@echo off

:: Navigate to the repository folder
cd /d "S:\Users\THoang\Tech\LucidMA"

:: Add all changes in the repository
git add .

:: Get the current date in a usable format (YYYY-MM-DD)
for /f "tokens=2-4 delims=/ " %%a in ("%date%") do (
    set formattedDate=%%c-%%a-%%b
)

:: Commit the changes with the date in the message
git commit -m "Update git EOD %formattedDate%"
if %ERRORLEVEL% neq 0 (
    echo [%date% %time%] Git commit failed >> BatchLogs.txt
    echo Git commit failed. Exiting script.
    exit /b 1
)

:: Push the changes to the remote repository
git push
if %ERRORLEVEL% neq 0 (
    echo [%date% %time%] Git push failed >> BatchLogs.txt
    echo Git push failed. Exiting script.
    exit /b 1
)

:: Log success if everything worked
echo [%date% %time%] Git commit and push succeeded >> BatchLogs.txt

echo Script executed successfully.