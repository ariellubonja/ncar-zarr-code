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
    chunk_morton_mapping = write_tools.get_chunk_morton_mapping(range_list, desired_cube_side, dest_folder_name)

    cubes = write_tools.flatten_3d_list(cubes)
    flattened_node_assgn = write_tools.flatten_3d_list(write_tools.node_assignment(4))

    q = queue.Queue()


    # Populate the queue with Write to FileDB tasks
    for i in range(len(range_list)):
    #     for j in range(4):
    #         for k in range(4):
        min_coord = [a[0] for a in range_list[i]]
        max_coord = [a[1] - 1 for a in range_list[i]]
        
        morton = (write_tools.morton_pack(array_cube_side, min_coord[2], min_coord[1], min_coord[0]), write_tools.morton_pack(array_cube_side, max_coord[2], max_coord[1], max_coord[0]))
        
        chunk_name = write_tools.search_dict_by_value(chunk_morton_mapping, morton)
        
        idx = int(chunk_name[-2:].lstrip('0'))
        
        filedb_index = flattened_node_assgn[idx - 1] - 1
        
        destination = os.path.join(folders[filedb_index], dest_folder_name + str(idx).zfill(2) + "_" + str(timestep_nr).zfill(3) + ".zarr")
        
        current_array = cubes[i]
                
        q.put((current_array, destination, encoding))
    

    # Create threads and start them

    threads = []
    for _ in range(num_threads):
        t = threading.Thread(target=write_tools.write_to_disk, args=(q,))
        t.start()
        threads.append(t)

    # Wait for all tasks to be processed
    q.join()

    # Wait for all threads to finish
    for t in threads:
        t.join()
