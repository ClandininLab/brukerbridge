import logging
import os
from pathlib import Path
import subprocess
import sys
import time


logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

class acquisition:
    def __init__(self, root: Path):
        """
            root (Path): Absolute path to the directory.
        """
        self.root = root

        self.series_list = []


    def discover_series(self):
        """
        Scan root path recursively for valid series directories.
        """
        self.series_list = []
        if not self.root.exists():
            return

        for dir_path, dirs, files in os.walk(self.root):
            path = Path(dir_path)
            if self.is_valid_series(path):
                logger.info(f"Discovered series at: %s", str(path))
                self.series_list.append(path)

        logger.info(f"Discovered {len(self.series_list)} series in {self.root}")

    def is_valid_series(self, path: Path):
        """
        Determine if a path is a valid series directory.
        Criteria:
        1. Contains an XML file.
        2. Contains a file matching pattern CYCLE_<NUMBER>_RAWDATA_<NUMBER> (no suffix).
        """
        has_xml = len(list(path.glob("*.xml"))) > 0

        # Check for raw data file
        # Pattern: CYCLE_..._RAWDATA_... with no suffix
        # We can simulate this with glob, or check all files
        has_rawdata = False
        for f in path.iterdir():
            if f.is_file() and not f.suffix: # No suffix check
                if "CYCLE_" in f.name and "_RAWDATA_" in f.name:
                    # heuristic check, strict enough for typical Bruker structure
                     has_rawdata = True
                     break

        return has_xml and has_rawdata

    def run_all(self):
        """
        Process all discovered series.
        """
        #TODO: determine the job time based on file size
        submitted_jobs = []
        #NOTE: increasing memory for shutil.copy(), not sure why for now
        command_base = ['sbatch', '--parsable', '--partition=trc', '--ntasks=1', '--cpus-per-task=1', '--time=10:00', '--mem=3GB']
        #command_base = ['sbatch', '--parsable', '--partition=owners', '--ntasks=1', '--cpus-per-task=1', '--time=10:00', '--mem=3GB']
        for path in self.series_list:
            #otherwise it's a reference, not a copy
            command = command_base[:]
            command.append(f'--output={str(path)}/brukerbridge.%j.log')
            command.append(f'--job-name={str(path)}_brukerbridge')
            command.append('--wrap')
            command.append(f'module load python/3.9.0; source brukerbridge/bin/activate; python3 series.py {str(path)}')
            logger.info(f"submitting sbatch job using command: {command}")
            result = subprocess.run(command, capture_output=True, text=True)
            job_id = result.stdout.strip()
            submitted_jobs.append(job_id)
            logger.info(f"Submitted {str(path)} processing job with ID {job_id}")

        logger.info(f"Waiting for jobs: {', '.join(submitted_jobs)}")
        while True:
            # Check status of submitted jobs
            status = subprocess.run(
                ['squeue', '-j', ','.join(submitted_jobs)],
                capture_output=True,
                text=True
            )
            # If squeue output is sparse (only header), jobs are finished
            if len(status.stdout.strip().split('\n')) <= 1:
                logger.info("All jobs finished.")
                break
            else:
                logger.info("Job status:\n" + status.stdout)
                self.get_all_state()

            time.sleep(30)

    def get_all_state(self):
        for path in self.series_list:
            logger.info(f"{path.name}: {self.get_state(path)}")

    def get_state(self, path):
        """
        Determine state on-the-fly based on dotfiles.
        Order matters: CONVERTED implies RIPPED.
        """
        if self.check_flag(path, "failed"):
             return 'failed'
        if self.check_flag(path, "converted"):
             return 'converted'
        if self.check_flag(path, "ripped"):
             return 'ripped'
        return 'pending/ripping'

    def check_flag(self, path, name):
        return (path / f".{name}").exists()
#=================
a = acquisition(Path("/scratch/groups/trc/yilin/260303"))
#TODO: how to pick up the folder automatically?
#Maybe scan a staging area with special mark
a.discover_series()
a.run_all()

