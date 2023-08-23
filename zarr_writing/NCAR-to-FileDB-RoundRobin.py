import unittest
import xarray as xr
import numpy as np
import os
from utils import write_tools
import queue, threading, argparse, sys


array_cube_side = 2048
desired_cube_side = 512
chunk_size = 64
raw_ncar_folder_path = '/home/idies/workspace/turb/data02_02/ncar-high-rate-fixed-dt'
use_dask = True
dest_folder_name = "sabl2048b" # B is the high-rate data
write_type = "prod" # or "back" for backup

n_dask_workers = 4 # For Dask rechunking

# Kernel dies with Sciserver large jobs resources as of Aug 2023. Out of memory IMO
num_threads = 1  # For reading from FileDB
dask_local_dir = '/home/idies/workspace/turb/data02_02'


encoding={
    "velocity": dict(chunks=(chunk_size, chunk_size, chunk_size, 3), compressor=None),
    "pressure": dict(chunks=(chunk_size, chunk_size, chunk_size, 1), compressor=None),
    "temperature": dict(chunks=(chunk_size, chunk_size, chunk_size, 1), compressor=None),
    "energy": dict(chunks=(chunk_size, chunk_size, chunk_size, 1), compressor=None)
}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--timestep', type=int, required=True)
    args = parser.parse_args()
    timestep_nr = args.timestep

    folders=write_tools.list_fileDB_folders()

    # Avoiding 7-2 and 9-2 - they're too full as of May 2023
    folders.remove("/home/idies/workspace/turb/data09_02/zarr/")
    folders.remove("/home/idies/workspace/turb/data07_02/zarr/")

    for i in range(len(folders)):
        folders[i] += dest_folder_name + "_" + str(i + 1).zfill(2) + "_" + write_type + "/"


    cubes, range_list = write_tools.prepare_data(raw_ncar_folder_path + "/jhd." + str(timestep_nr).zfill(3) + ".nc")

