@echo off
echo Started ripper: %date% %time%
set FOLDER_NAME=%1
echo Ripping from folder %FOLDER_NAME%.
::"C:\Program Files\Prairie 5.8.64.300\Prairie View\Utilities\Image-Block Ripping Utility.exe" -isf -arfwsf %FOLDER_NAME% -cnv
"C:\Program Files\Prairie 5.5.64.600\Prairie View\Utilities\Image-Block Ripping Utility.exe" -isf -arfwsf %FOLDER_NAME% -cnv
echo Finished ripping: %date% %time%