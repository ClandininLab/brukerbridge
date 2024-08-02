import os
import sys
import time
import subprocess
from time import strftime
import json

log_folder = 'C:/Users/User/Desktop/dataflow_logs'
root_directory = "H:/"

def main():

	#banned_dirs = get_banned_dirs()
	while True:
		queued_folder, stripped_folder = get_queued_folder()
		if queued_folder is not None:
			launch_main_processing(queued_folder, stripped_folder, log_folder)
		time.sleep(0.1)

def get_queued_folder():
	candidate_dir_fullpaths = []

	for user_dir in os.listdir(root_directory):
		user_dir_fullpath = os.path.join(root_directory, user_dir)

		if user_dir != 'System Volume Information' and os.path.isdir(user_dir_fullpath):
			candidate_dir_fullpaths += [os.path.join(user_dir_fullpath, imaging_dir) for imaging_dir in os.listdir(user_dir_fullpath)]

	high_priority_dir_fullpaths = [x for x in candidate_dir_fullpaths if x.endswith('__priority__')]
	regular_dir_fullpaths = [x for x in candidate_dir_fullpaths if x.endswith('__queue__')]
	low_priority_dir_fullpaths = [x for x in candidate_dir_fullpaths if x.endswith('__lowqueue__')]

	all_candidates = high_priority_dir_fullpaths + regular_dir_fullpaths + low_priority_dir_fullpaths
	if len(all_candidates) > 0:
		print('Candidate imaging data directories:')
		for cdfp in high_priority_dir_fullpaths + regular_dir_fullpaths + low_priority_dir_fullpaths:
			print(f'  {cdfp}')

	if len(high_priority_dir_fullpaths) > 0:
		# Get the earliest date directory among the high_priority_dir_fullpaths
		leaf_dirs = [os.path.basename(os.path.normpath(x)) for x in high_priority_dir_fullpaths]
		sorted_indices = [i for i, value in sorted(enumerate(leaf_dirs), key=lambda x: x[1])]
		chosen_dir_fullpath = high_priority_dir_fullpaths[sorted_indices[0]]
		stripped_fullpath = chosen_dir_fullpath[:-12]
	elif len(regular_dir_fullpaths) > 0:
		leaf_dirs = [os.path.basename(os.path.normpath(x)) for x in regular_dir_fullpaths]
		sorted_indices = [i for i, value in sorted(enumerate(leaf_dirs), key=lambda x: x[1])]
		chosen_dir_fullpath = regular_dir_fullpaths[sorted_indices[0]]
		stripped_fullpath = chosen_dir_fullpath[:-9]
	elif len(low_priority_dir_fullpaths) > 0:
		leaf_dirs = [os.path.basename(os.path.normpath(x)) for x in low_priority_dir_fullpaths]
		sorted_indices = [i for i, value in sorted(enumerate(leaf_dirs), key=lambda x: x[1])]
		chosen_dir_fullpath = low_priority_dir_fullpaths[sorted_indices[0]]
		stripped_fullpath = chosen_dir_fullpath[:-12]
	else:
		chosen_dir_fullpath = None
		stripped_fullpath = None

	if chosen_dir_fullpath is not None:
		print(f'Chosen: {chosen_dir_fullpath}')

	return chosen_dir_fullpath, stripped_fullpath

def get_banned_dirs():
	banned_dir = 'C:/Users/User/projects/brukerbridge/banned_dirs'
	return os.listdir(banned_dir)

def attempt_rename(source, target):
	print(F'Attemping rename {source} to {target}')
	attempts = 3
	while attempts > 0:
		attempts-=1
		try:
			os.rename(source, target)
			print(F'Rename successful {source} to {target}')
			return
		except:
			print(F"Rename attempt {attempts} failed")
			time.sleep(60)

def launch_main_processing(dir_to_process, stripped_folder, log_folder):
	log_file = 'dataflow_log_' + strftime("%Y%m%d-%H%M%S") + '.txt'
	full_log_file = os.path.join(log_folder, log_file)

	# the >> redirects stdout with appending. the 2>&1 does the same with stderr
	# double quotes to accommodates spaces in directory name
	# this command is blocking so this watcher will just wait here until main.py is finished
	print(F"launching {dir_to_process}")
	print(F"log file {full_log_file}")

	f = open(full_log_file, 'w')
	exit_status = subprocess.call(['python', 'C:/Users/User/projects/brukerbridge/scripts/main.py', dir_to_process],stdout=f,stderr=f)

	#stderr=subprocess.STDOUT
	if exit_status != 0:
		print("ERROR! Appending __error__ to this folder, then continuing with next in queue.")
		attempt_rename(dir_to_process, stripped_folder + "__error__")
		#raise SystemExit
	else:
		attempt_rename(dir_to_process, stripped_folder)

	#os.system('python C:/Users/User/projects/brukerbridge/scripts/main.py "{}" >> {} 2>&1'.format(dir_to_process, full_log_file))
	#time.sleep(5)

if __name__ == '__main__':
	main()
