import logging
import os
from pathlib import Path
import subprocess
import sys
import time
import math

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

class acquisition:
    def __init__(self, root: Path):
        """
            root (Path): Absolute path to the directory.
        """
        self.root = root
        self.series_list = []
        self.dependency_path = Path("/oak/stanford/groups/trc/BrukerBridge/dependency_root")
        script_path = Path(__file__).resolve()
        self.script_dir = script_path.parent


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
        submitted_jobnames = []
        #NOTE: increasing memory for shutil.copy(), not sure why for now
        command_base = ['sbatch', '--parsable', '--partition=owners', '--ntasks=1', '--cpus-per-task=1', '--mem=3GB']

        for path in self.series_list:
            #otherwise it's a reference, not a copy
            command = command_base[:]
            command.append(f'--time={self.get_required_time(path)}')
            #NOTE: this storage is not guaranteed throughout the entire job
            command.append(f'--tmp={self.get_required_storage_gb(path)}G')
            command.append(f'--output={str(path)}/brukerbridge.%j.log')
            command.append(f'--job-name={str(path)}_brukerbridge')
            command.append('--wrap')
            command.append(f'module load python/3.9.0; source {str(self.dependency_path)}/brukerbridge_env/bin/activate; python3 {str(self.script_dir)}/series.py {str(path)} {str(self.dependency_path)}')
            logger.info(f"submitting sbatch job using command: {command}")
            result = subprocess.run(command, capture_output=True, text=True)
            job_id = result.stdout.strip()
            submitted_jobnames.append(str(path)+'_brukerbridge')
            logger.info(f"Submitted {str(path)} processing job with ID {job_id}")

        while True:
            # Check status of submitted jobs
            status = subprocess.run(
                ['squeue', '-n', ','.join(submitted_jobnames)],
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

    def get_folder_size_gb(self, path):
        cmd = ['du', '-sB', '1', path]
        result = subprocess.run(cmd, capture_output=True, text=True)
        folder_size = int(result.stdout.split()[0])
        folder_size_gb = folder_size / 1024.0 / 1024.0 / 1024.0
        return folder_size_gb

    def get_required_time(self, path):
        folder_size_gb = self.get_folder_size_gb(path)
        # leave some room for file copy/paste and singularity/wine startup
        time_based_on_folder_size = 2 * math.ceil(10 + folder_size_gb)
        return time_based_on_folder_size

    def get_required_storage_gb(self, path):
        folder_size_gb = self.get_folder_size_gb(path)
        # peak storage = wine folder + raw file + ripped file
        required_storage_gb = math.ceil(10 + folder_size_gb * 2)
        return required_storage_gb

#=================
if len(sys.argv) > 1:
    acq_path = Path(sys.argv[1])
    if not acq_path.exists():
        logger.critical(f"Path {acq_path} is invalid, please check your input.")
        sys.exit(1)
    acq_path = acq_path.resolve()
    logger.info(f"Working on acquisition path: {acq_path}")
else:
    logger.critical("No acquisition path provided. Abort.")
    sys.exit(1)

a = acquisition(acq_path)
a.discover_series()
a.run_all()
