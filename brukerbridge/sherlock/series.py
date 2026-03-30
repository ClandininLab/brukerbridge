from xml.etree import ElementTree
from pathlib import Path
from pathlib import PureWindowsPath
import logging
import sys
import signal
import os
import shutil
import subprocess
import time
from convert import convert_to_nii
from convert import timing_decorator
import tarfile

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

class series:
    def __init__(self, path: Path, use_lscratch=False):
        #TODO: generalize
        #TODO: PV 5.5 is still in use, consider it
        self.path = path
        self.use_lscratch=use_lscratch
        self.dependency_root = Path("/scratch/groups/trc/yilin/simg")
        self.ripping_utility_root = self.dependency_root / "Utilities_root"
        self.wine_container_path = self.dependency_root / "docker-wine_latest.sif"
        self.wineprefix_path = self.dependency_root / "wineprefix.tar.gz"
        if not os.getenv('L_SCRATCH') or not os.getenv('SLURM_JOB_ID'):
          logger.critical("Failed to detect L_SCRATCH or SLURM_JOB_ID env variable. Seems not running from a Sherlock node!")
          self.scratch_path = None
          self.use_lscratch = False
        else:
          self.scratch_path = Path(os.getenv('L_SCRATCH'), os.getenv('SLURM_JOB_ID'), self.path.name)
        self.ripping_utility_path = self.get_ripping_utility_path()

    def get_xml_path(self, use_lscratch=False):
        if use_lscratch:
          path = self.scratch_path
        else:
          path = self.path
        candidates = list(path.glob("*Series*.xml"))
        valid_candidates = [
            c for c in candidates
            if "VoltageRecording" not in c.name
        ]
        if not valid_candidates:
            return None
        return valid_candidates[0]

    def get_ripping_utility_path(self):
      if not self.get_xml_path():
        return None
      acq_root = ElementTree.parse(str(self.get_xml_path())).getroot()
      if acq_root.tag != "PVScan":
        logger.critical("Failed to get Prairie View version from raw data XML, can't determine corresponding Image-block ripping utility!")
        return None
      # Example: 5.8.64.900
      util_path = self.ripping_utility_root / acq_root.attrib["version"] / "Image-Block Ripping Utility.exe"
      if not util_path.is_file():
        logger.critical("Failed to locate Image-block ripping utility with version: %s", acq_root.attrib["version"])
        return None
      return util_path

    @timing_decorator
    def prepare_file_for_ripping(self):
      if not self.use_lscratch:
        return
      logger.info("scratch area for ripping: %s", str(self.scratch_path))
      #check if storage is enough
      cmd = ['du', '-sB', '1', self.path]
      result = subprocess.run(cmd, capture_output=True, text=True)
      folder_size = int(result.stdout.split()[0])
      logger.info(f"Current folder size (GB): {folder_size / 1024.0 / 1024.0 / 1024.0}")
      cmd = ['df', '--output=avail', '-B', '1', str(self.scratch_path.parent)]
      result = subprocess.run(cmd, capture_output=True, text=True)
      scratch_free_size = int(result.stdout.strip().split('\n')[-1])
      logger.info(f"Scratch available size (GB): {scratch_free_size / 1024.0 / 1024.0 / 1024.0}")
      if folder_size > scratch_free_size:
        logger.error("Not enough scratch storage for copying raw data, abort using l_scratch")
        self.use_lscratch=False
        return
      #copy file from lustre file system to node-local file system
      logger.info("Copying %s to %s", str(self.path), str(self.scratch_path))
      shutil.copytree(self.path, self.scratch_path, dirs_exist_ok=True)
      logger.info("Copying .wine folder")
      #NOTE: needs wineprefix separation due to concurrent instances
      shutil.copy2(self.wineprefix_path, self.scratch_path.parent)
      with tarfile.open(self.scratch_path.parent / "wineprefix.tar.gz", "r:gz") as tar:
        # Extract all contents to the specified directory
        tar.extractall(path=self.scratch_path.parent)

    @timing_decorator
    def launch_and_wait_ripping(self):
      if self.use_lscratch:
        path = self.scratch_path
      else:
        path = self.path
      logger.info("Using ripping utillity: %s", self.ripping_utility_path)
      my_env = os.environ.copy()
      my_env["WINEPREFIX"] = str(self.scratch_path.parent)+'/.wine'
      #command = ['singularity', 'exec', '-B', f'{my_env["L_SCRATCH"]}:{my_env["L_SCRATCH"]},{my_env["GROUP_SCRATCH"]}:{my_env["GROUP_SCRATCH"]},/tmp:/var/lib/xkb']
      command = ['singularity', 'exec', '-B', f'{my_env["L_SCRATCH"]}:{my_env["L_SCRATCH"]},{my_env["OAK"]}:{my_env["OAK"]},/tmp:/var/lib/xkb']
      command.extend([str(self.wine_container_path), 'xvfb-run', '-a', 'wine'])
      command.extend([str(self.ripping_utility_path), '-IncludeSubFolders', '-AddRawFileWithSubFolders', 'Z:'+str(PureWindowsPath(path))])
      #command.extend(['-DoNotRipToInputDirectory', '-SetOutputDirectory', 'Z:'+str(PureWindowsPath(self.path))])
      command.extend(['-RipToInputDirectory'])
      command.extend(['-DeleteRaw', '-Convert'])
      process = subprocess.Popen(command, start_new_session=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=my_env)
      logger.info("Started ripping utility from singularity and wine virtualization")
      logger.info("command: %s", " ".join(command))
      #waiting for wine and ripping utility startup
      time.sleep(30)
      while True:
        if any(path.glob('*Filelist.txt')):
          logger.info("Filelist.txt not deleted by ripping utility yet, keep waiting.")
          time.sleep(10)
          cmd = ['find', str(path), '-mmin', '-1', '!', '-name', '*.log']
          result = subprocess.run(cmd, capture_output=True, text=True)
          if any(path.glob('*Filelist.txt')) and not result.stdout:
            logger.critical("Although Filelist.txt not deleted, during wait time there is no modification to working directory. Abort.")
            os.killpg(process.pid, signal.SIGTERM)
            cmd = ['ls', str(path)]
            result = subprocess.run(cmd, capture_output=True, text=True)
            logger.info("working dir :\n"+result.stdout)
            stdout, stderr = process.communicate()
            logger.info(f"ripping log:\n{stdout}")
            return
        else:
          logger.info("Filelist.txt are now deleted by ripping utility. End.")
          os.killpg(process.pid, signal.SIGTERM)
          return True

    @timing_decorator
    def postprocess_file_for_ripping(self):
      if not self.use_lscratch:
        return
      #TODO: log final disk usage
      logger.info("Moving conversion results from %s to %s", str(self.scratch_path), str(self.path))
      for file_path in list(self.scratch_path.glob("*.nii")) + list(self.scratch_path.glob("*.csv")):
        logger.info("Moving %s to %s", str(file_path), str(self.path))
        shutil.move(file_path, self.path)
      #TODO: enable deleting raw file as a switch
      #for file_path in list(self.path.glob('*Filelist.txt')) + list(self.path.glob('*RAWDATA*')) + list(self.path.glob('*VoltageRecording_[0-9][0-9][0-9]')):
      #  if file_path.is_file():
      #    file_path.unlink()
      #    logger.info("Deleted %s", str(file_path))

    @timing_decorator
    def cleanup_tiff(self):
      if self.use_lscratch:
        path = self.scratch_path
        logger.info("Deleting tiff file from %s", str(path))
        for file_path in path.glob("*.tif"):
          file_path.unlink()

    def process(self):
      if not self.get_xml_path() or not self.ripping_utility_path:
        self.mark_flag('failed')
        return
      self.prepare_file_for_ripping()
      if not self.launch_and_wait_ripping():
        self.mark_flag('failed')
        return
      self.mark_flag('ripped')
      logger.info("Converting tiff to nii at: %s", str(self.get_xml_path(self.use_lscratch)))
      try:
        convert_to_nii(self.get_xml_path(self.use_lscratch))
      except:
        self.mark_flag('failed')
        return
      self.postprocess_file_for_ripping()
      self.cleanup_tiff()
      self.mark_flag('converted')

    def mark_flag(self, name):
        """Create a dotfile flag to query state persistence."""
        (self.path / f".{name}").touch()

#===============================
if len(sys.argv) > 1:
    series_path = sys.argv[1]
    logger.info(f"Working on series path: {series_path}")
else:
    logger.critical("No series path provided. Abort.")
    sys.exit(1)
s = series(Path(series_path), use_lscratch=True)
s.process()
