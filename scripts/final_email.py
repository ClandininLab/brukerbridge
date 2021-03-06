import brukerbridge as bridge
import os
from time import sleep

my_email = ["brezovec@stanford.edu"]
email_file = 'C:/Users/User/projects/brukerbridge/scripts/email.txt'

try:
    with open(email_file, 'r') as f:
        user_email = f.read()
    if user_email == my_email[0]:
        emails = my_email
    else:
        emails = [my_email[0], user_email]
    print('Emails: {}'.format(emails))
    # Get latest log file
    log_folder = 'C:/Users/User/Desktop/dataflow_logs/'
    list_of_files = os.listdir(log_folder) # * means all if need specific format then *.csv
    list_of_files_full = [os.path.join(log_folder, file) for file in list_of_files]
    latest_file = max(list_of_files_full, key=os.path.getctime)

    # Get error file with same name
    error_folder = 'C:/Users/User/Desktop/dataflow_error/'
    file = latest_file.split('/')[-1]
    error_file = os.path.join(error_folder, file)
    if os.stat(error_file).st_size != 0:
        with open(error_file, 'r') as f:
            error_info = f.read()
        for email in emails:
            bridge.send_email(subject='BrukerBridge FAILED', message=error_info, recipient=email)
            sleep(1)
    else:
        for email in emails:
            bridge.send_email(subject='BrukerBridge SUCCESS', message='.', recipient=email)
            sleep(1)
    try:
        os.remove(email_file)
    except:
        print('Could not remove email file.')

except:
    print('No email file, probably due to incorrectly setup user metadata.')