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


def prepare_data(xr_path):
    # for timestep_nr in timestep_range:
    data_xr = xr.open_dataset(xr_path)

    # Group 3 velocity components together
    # This fails with Dask bcs. of write permission error on SciServer Job
    # Never use dask with remote location on this!!
    merged_velocity = write_tools.merge_velocities(data_xr, dask_local_dir=dask_local_dir
                                                , chunk_size_base=chunk_size, use_dask=True, n_dask_workers=n_dask_workers)


    # Unabbreviate 'e', 'p', 't' variable names
    merged_velocity = merged_velocity.rename({'e': 'energy', 't': 'temperature', 'p': 'pressure'})


    dims = [dim for dim in data_xr.dims]
    dims.reverse() # use (nnz, nny, nnx) instead of (nnx, nny, nnz)

    # Split 2048^3 into smaller 512^3 arrays
    smaller_groups, range_list = write_tools.split_zarr_group(merged_velocity, desired_cube_side, dims)


    print('Done preparing data. Starting to verify...')

    return smaller_groups, range_list


def verify_512_cube(original_512, zarr_512_path):
    print(f"Reading Zarr from {zarr_512_path}...")
    zarr_512 = xr.open_zarr(zarr_512_path)

    # Compare attributes
    assert original_512.attrs == zarr_512.attrs, f"Attribute mismatch for {zarr_512_path}."

    # Compare each variable's data
    for var in original_512.data_vars:
        assert np.array_equal(original_512[var].values, zarr_512[var].values), f"Data mismatch found for variable {var} in {zarr_512_path}."
        assert original_512[var].attrs == zarr_512[var].attrs, f"Variable attribute mismatch for variable {var} in {zarr_512_path}."


class VerifyWriteTest(unittest.TestCase):

    def setUp(self):
        timestep_nr = args.timestep
        self.queue = queue.Queue()
        
        folders=write_tools.list_fileDB_folders()

        # Avoiding 7-2 and 9-2 - they're too full as of May 2023
        folders.remove("/home/idies/workspace/turb/data09_02/zarr/")
        folders.remove("/home/idies/workspace/turb/data07_02/zarr/")

        for i in range(len(folders)):
            folders[i] += dest_folder_name + "_" + str(i + 1).zfill(2) + "_" + write_type + "/"


        cubes, range_list = prepare_data(raw_ncar_folder_path + "/jhd." + str(timestep_nr).zfill(3) + ".nc")
        
        sorted_morton_list = [] # Sorting by Morton code to be consistent with Isotropic8192

        for i in range((array_cube_side / desired_cube_side) ** 3):            
            min_coord = [a[0] for a in range_list[i]]
            max_coord = [a[1] - 1 for a in range_list[i]]
                    
            sorted_morton_list.append((write_tools.morton_pack(array_cube_side, min_coord[0], min_coord[1], min_coord[2]), write_tools.morton_pack(array_cube_side, max_coord[0], max_coord[1], max_coord[2])))
                
        sorted_morton_list = sorted(sorted_morton_list)
        # Given up in favor of Ryan's node coloring technique
        #     z_order = write_tools.morton_order_cube(cube_side=4)
        node_assignments = write_tools.node_assignment(cube_side=4)
        flattened_node_assgn = flatten_3d_list(node_assignments)

        mike_dict = {}
        for i in range(len(range_list)):            
            min_coord = [a[0] for a in range_list[i]]
            max_coord = [a[1] - 1 for a in range_list[i]]
                    
            mike_dict[dest_folder_name + str(i + 1).zfill(2)] = sorted_morton_list[i]
        
        cubes = flatten_3d_list(cubes)

        q = queue.Queue()


        # Populate the queue with tasks
        for i in range(len(range_list)):
        #     for j in range(4):
        #         for k in range(4):
            min_coord = [a[0] for a in range_list[i]]
            max_coord = [a[1] - 1 for a in range_list[i]]
            
            morton = (write_tools.morton_pack(array_cube_side, min_coord[2], min_coord[1], min_coord[0]), write_tools.morton_pack(array_cube_side, max_coord[2], max_coord[1], max_coord[0]))
            
            chunk_name = search_dict_by_value(mike_dict, morton)
            
            idx = int(chunk_name[-2:].lstrip('0'))
            
            filedb_index = flattened_node_assgn[idx - 1] - 1
            
            destination = os.path.join(folders[filedb_index], dest_folder_name + str(idx).zfill(2) + "_" + str(timestep_nr).zfill(3) + ".zarr")
            
            current_array = cubes[i]
                    
            q.put((current_array, destination))
        
        # Create threads and start them
        threads = []
        for _ in range(num_threads):
            t = threading.Thread(target=verify_write, args=(q,))
            t.start()
            threads.append(t)

        # Wait for all tasks to be processed
        q.join()

        # Wait for all threads to finish
        for t in threads:
            t.join()

    def test_chunks(self):
        while not self.queue.empty():
            original_512, zarr_512_path = self.queue.get()
            with self.subTest(msg=f"Testing chunk from {zarr_512_path}"):
                verify_512_cube(original_512, zarr_512_path)


def verify_write(q):
    while True:
        try:
            original_512, zarr_512_path = q.get(timeout=10)  # Adjust timeout as necessary

            print(f"Reading Zarr from {zarr_512_path}...")
            zarr_512 = xr.open_zarr(zarr_512_path)

            # Compare attributes
            assert original_512.attrs == zarr_512.attrs

            # Compare each variable's data
            for var in original_512.data_vars:
                assert np.array_equal(original_512[var].values, zarr_512[var].values)
                assert original_512[var].attrs == zarr_512[var].attrs  # Compare attributes of the variable
            
            print("CORRECT!")
        except queue.Empty:
            break
        except AssertionError:
            print(f"Data mismatch found for {zarr_512_path}.")
        finally:
            q.task_done()

            
def flatten_3d_list(lst_3d):
    return [element for sublist_2d in lst_3d for sublist_1d in sublist_2d for element in sublist_1d]


def search_dict_by_value(dictionary, value):
    for key, val in dictionary.items():
        if val == value:
            return key
    return None  # Value not found in the dictionary


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--timestep', type=int, required=True)
    args = parser.parse_args()
    timestep_nr = args.timestep
    
    # run the tests
    unittest.main(argv=[sys.argv[0]])