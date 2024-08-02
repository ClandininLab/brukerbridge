@echo off
@echo watching for files
:: I resent this but there is some sort of environment running it elsewhere and I cannot be asked
cd /d "C:\Users\User\src\brukerbridge"
py -3 -m brukerbridge >nul 2>&1
