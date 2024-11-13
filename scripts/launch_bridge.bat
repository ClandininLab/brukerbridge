@echo off
@echo watching for files
@echo follow C:\Users\User\logs\bridge.log using mtail for info
@echo logs from previous days are renamed
:: I resent this but there is some sort of environment running it elsewhere and I cannot be asked
cd /d "C:\Users\User\src\brukerbridge"
python -m brukerbridge >nul 2>&1
