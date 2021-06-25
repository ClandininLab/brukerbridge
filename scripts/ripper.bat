@echo off
echo Started ripper.
set FOLDER_NAME=%1
echo Ripping from folder %FOLDER_NAME%.
rem "C:\Program Files\Prairie 5.5.64.300\Prairie View\Utilities\Image-Block Ripping Utility.exe" -isf -arfwsf %FOLDER_NAME% -cnv
"C:\Program Files\Prairie\Prairie View\Utilities\Image-Block Ripping Utility.exe" -isf -arfwsf %FOLDER_NAME% -cnv
echo Finished ripping.