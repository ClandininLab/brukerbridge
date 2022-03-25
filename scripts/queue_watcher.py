import os
import sys
import time
import subprocess
from time import strftime

log_folder = 'C:/Users/User/Desktop/dataflow_logs'
root_directory = "H:/"

def main():

	while True:
		queued_folder = get_queued_folder()
		if queued_folder is not None:
			launch_main_processing(queued_folder, log_folder) 
		time.sleep(0.1)

def get_queued_folder():
	low_queue = None
	for user_folder in os.listdir(root_directory):
		### need to skip this weird file
		if user_folder == 'System Volume Information':
			continue
		user_folder = os.path.join(root_directory, user_folder)

		if os.path.isdir(user_folder):
			for potential_queued_folder in os.listdir(user_folder):
				potential_queued_folder = os.path.join(user_folder, potential_queued_folder)

				if potential_queued_folder.endswith('__queue__'):
					return potential_queued_folder ### Immediately return any queued folder found

				if potential_queued_folder.endswith('__lowqueue__'):
					low_queue = potential_queued_folder

	return low_queue

def launch_main_processing(dir_to_process, log_folder):
	log_file = 'dataflow_log_' + strftime("%Y%m%d-%H%M%S") + '.txt'
	full_log_file = os.path.join(log_folder, log_file)

	# the >> redirects stdout with appending. the 2>&1 does the same with stderr
	# double quotes to accommodates spaces in directory name
	# this command is blocking so this watcher will just wait here until main.py is finished
	print(F"launching {dir_to_process}")
	print(F"log file {full_log_file}")

	stderr=subprocess.STDOUT
	f = open(full_log_file, 'w')
	exit_status = subprocess.call(['python', 'C:/Users/User/projects/brukerbridge/scripts/main.py', dir_to_process],stdout=f)
	if exit_status != 0:
		print("ERROR! EXITING!")
		raise SystemExit


	#os.system('python C:/Users/User/projects/brukerbridge/scripts/main.py "{}" >> {} 2>&1'.format(dir_to_process, full_log_file))
	time.sleep(100)

if __name__ == '__main__':
	main()