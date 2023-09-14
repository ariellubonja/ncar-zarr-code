from utils import write_tools
import argparse
import dask
from dask import delayed
from dask.distributed import Client


array_cube_side = 2048
desired_cube_side = 512
chunk_size = 64
raw_ncar_folder_path = '/home/idies/workspace/turb/data02_02/ncar-high-rate-fixed-dt'
use_dask = True
dest_folder_name = "sabl2048b" # B is the high-rate data
write_type = "prod" # or "back" for backup

n_dask_workers = 16 # For Dask rechunking

# Kernel dies with Sciserver large jobs resources as of Aug 2023. Out of memory IMO
# num_threads = 34  # For writing to FileDB
# dask_local_dir = '/home/idies/workspace/turb/data02_02'


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

    client = Client(n_workers=n_dask_workers)

    cubes, _ = write_tools.prepare_data(raw_ncar_folder_path + "/jhd." + str(timestep_nr).zfill(3) + ".nc")
    cubes = write_tools.flatten_3d_list(cubes)
    dests = write_tools.get_512_chunk_destinations(dest_folder_name, write_type, timestep_nr, array_cube_side)

    tasks = [delayed(write_tools.write_to_disk)(cubes[i], dests[i], encoding) for i in range(len(dests))]

    # Compute all tasks in parallel
    dask.compute(*tasks)

    client.close()
