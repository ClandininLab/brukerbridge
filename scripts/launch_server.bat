@echo off
@echo HELLO I AM THE SERVER THAT TRANSFERS DATA FROM BRUKER TO THIS COMPUTER.
@echo LEAVE ME OPEN AND I WILL DO GOOD THINGS.
@echo YOU MUST START FILE TRANSFER FROM THE BRUKER COMPUTER.
@echo LOOK AT MY OUTPUT IN DATAFLOW_LOGS\SERVER_LOG.TXT. USING MTAIL PROGRAM IS CONVENIENT.
C:\Users\User\AppData\Local\Programs\Python\Python37\python.exe C:\Users\User\projects\brukerbridge\brukerbridge\server.py >> C:\Users\User\Desktop\dataflow_logs\server_log.txt 2>&1 
cmd /k