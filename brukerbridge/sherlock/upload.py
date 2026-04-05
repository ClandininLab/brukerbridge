from tkinter import Tk
from tkinter.filedialog import askdirectory

import os
import sys
import shutil
import filecmp
from pathlib import PurePosixPath, Path
from datetime import datetime
from tqdm import tqdm

def copytree_with_progress(src, dst):
    total_files = sum([len(files) for r, d, files in os.walk(src)])
    with tqdm(total=total_files, unit='file') as pbar:
        def copy_with_progress(src, dst):
            shutil.copy2(src, dst)
            #TODO: check performance, check if this is needed
            if not filecmp.cmp(src, dst, shallow=False):
                print(f"{dst} failed equivalency check. Attempting 2nd copy")
                os.remove(dst)
                shutil.copy2(src, dst)
            pbar.update(1)
        shutil.copytree(src, dst, copy_function=copy_with_progress)

root = Tk()
root.withdraw()

print("Please first specify source directory to be copied")
print("Please then specify destination (Oak) directory")

src_folder = askdirectory(title="Select Source Folder")
dst_folder = askdirectory(title="Select Destination Folder")
if src_folder == dst_folder:
    print("src and dst folders are the same!")
    sys.exit(1)
dst_path = Path(dst_folder) / Path(src_folder).name
##TODO: get rid of hardcode here
#job_path = Path(dst_path.drive) / 'BrukerBridgeJobs'
#if not job_path:
#    print("cannot find bruker bridge job folder from Oak to queue the workload")
#    sys.exit(1)

if src_folder and dst_folder:
    print(f"Src folder: {src_folder}, dst folder: {dst_folder}")
else:
    print("User cancelled the selection.")
    sys.exit(1)

copytree_with_progress(src_folder, dst_path)

#delete src after it's copied to dst
count_src = sum(len(files) for root, dirs, files in os.walk(src_folder))
count_dst = sum(len(files) for root, dirs, files in os.walk(dst_path))
if count_src == count_dst:
    print("File copy completed, now deleting source directory.")
    shutil.rmtree(src_folder)
    print("DONE. Please launch BrukerBridge conversion job from Sherlock.")
else:
    print(f"Error: there are {count_src} files from source folder, but {count_dst} files after copy")
    sys.exit(1)

#queue the job
#dst_path_without_drive_letter = dst_path.parts[1:]
#with open(job_path / f'brukerbridge_{datetime.now().strftime("%Y%m%d%H%M%S")}.job', "w") as f:
#    #convert from windows mounted drive path to linux path
#    #TODO: get rid of hardcode here
#    f.write('/oak/stanford/groups/trc/'+str(PurePosixPath(*dst_path_without_drive_letter))+'\n')
#print("queued job", str(job_path / f'brukerbridge_{datetime.now().strftime("%Y%m%d%H%M%S")}.job'))

