import os
import sys
import time
from shutil import copyfile
from datetime import datetime

def transfer_to_oak(source, target, allowable_extensions, verbose, size_transfered): 
    for item in os.listdir(source):
        # Create full path to item
        source_path = source + '/' + item
        target_path = target + '/' + item

        # Check if item is a directory
        if os.path.isdir(source_path):
            # Create same directory in target
            try:
                os.mkdir(target_path)
                print('Creating directory {}'.format(os.path.split(target_path)[-1]))
            except FileExistsError:
                if verbose:
                    print('WARNING: Directory already exists  {}'.format(target_path))
                    print('Skipping Directory.')

            # RECURSE!
            transfer_to_oak(source_path, target_path, allowable_extensions, verbose, size_transfered)
        
        # If the item is a file
        else:
            if os.path.isfile(target_path):

                if verbose:
                    print('File already exists. Skipping.  {}'.format(target_path))
                
            elif source_path[-4:] in allowable_extensions:

                #####################
                ### TRANSFER FILE ###
                #####################

                size_transfered += os.path.getsize(target_path)
                size_transfered_MB = size_transfered*10**-6

                now = datetime.now()
                current_time = now.strftime("%H:%M:%S")

                print('{} | Transfering file {}; size = {} MB'.format(current_time, target_path, size_transfered_MB))

                t0 = time.time()
                copyfile(source_path, target_path)
                duration = time.time()-t0

                print('done. duration: {} sec; {} MB/SEC'.format(duration, size_transfered_MB/duration))
                
            else:
                pass

def start_oak_transfer(directory_from, oak_target, allowable_extensions, add_to_build_que, verbose=True):
    directory_to = os.path.join(oak_target, os.path.split(directory_from)[-1])
    try:
        os.mkdir(directory_to)
    except FileExistsError:
        if verbose:
            print('WARNING: Directory already exists  {}'.format(directory_to))
        #print('Skipping directory.')

    print('Moving from  {}'.format(directory_from))
    print('Moving to  {}'.format(directory_to))
    transfer_to_oak(directory_from, directory_to, allowable_extensions, verbose, size_transfered=0)
    print('*** Oak Upload Complete ***')
    if add_to_build_que in ['True', 'true']:
        folder = os.path.split(directory_to)[-1]
        queue_file = os.path.join(oak_target, 'build_queue', folder)
        file = open(queue_file,'w+')
        file.close()
        print('Added {} to build queue.'.format(folder))
    else:
        print('Add to build queue is False.')
        #os.rename(directory_to, directory_to + '__done__')
        #print('Added __done__ flag')
    return size_transfered*10**-9 #report in GB

