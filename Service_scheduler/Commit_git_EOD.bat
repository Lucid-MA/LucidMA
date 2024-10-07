@echo off

:: Navigate to the repository folder
cd /d "S:\Users\THoang\Tech\LucidMA"

:: Add all changes in the repository
git add .

:: Get the current date in a usable format (YYYY-MM-DD)
for /f "tokens=2-4 delims=/ " %%a in ("%date%") do (
    set formattedDate=%%c-%%a-%%b
)

:: Log the status before committing and pushing
echo [%date% %time%] Git commit and push started >> "S:\Users\THoang\Tech\LucidMA\Service_scheduler\BatchLogs\BatchLogs.txt"

:: Commit the changes with the date in the message
git commit -m "Update git EOD %formattedDate%"
if %ERRORLEVEL% neq 0 (
    echo [%date% %time%] Git commit failed >> "S:\Users\THoang\Tech\LucidMA\Service_scheduler\BatchLogs\BatchLogs.txt"
    echo Git commit failed. Exiting script.
    exit /b 1
)

:: Push the changes to the remote repository
git push
if %ERRORLEVEL% neq 0 (
    echo [%date% %time%] Git push failed >> "S:\Users\THoang\Tech\LucidMA\Service_scheduler\BatchLogs\BatchLogs.txt"
    echo Git push failed. Exiting script.
    exit /b 1
)

echo Script executed successfully.