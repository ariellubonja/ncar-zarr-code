import math
import os
import queue
import re
import subprocess
import sys
from itertools import product

import dask.array as da
import numpy as np
import xarray as xr

try:
    import morton
except ImportError:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'morton-py'])
finally:
    import morton


def node_assignment(cube_side: int):
    """
    Ryan's node assignment code that solves the Color-matching problem

    :param cube_side: Length of the cube side to be assigned to nodes
    """
    def get_bounds(idx:int, mx:int):
        """Helper for requesting valid indicies around an index"""
        return max(idx-1, 0), min(idx+2, mx)

    # There are 34 availble nodes that are 1-indexed
    colors = np.arange(34) + 1
    # An array for holding how often a node has been assigned too.
    color_counts = np.zeros_like(colors)
    # An array to store the node assignment the 8192^3 data is broken into 512^3
    # sub-cubes so we need to 8192/512=16 assignments along each dimension


    nodes = np.zeros([cube_side, cube_side, cube_side], dtype=int)

    # iterate through the position indicies in `nodes` and assign available 
    # nodes greedily such that there is not a matching node within the 
    # (8 + 9 + 9) 26 cube neighborhood.
    for i, j, k in product(np.arange(cube_side), np.arange(cube_side), np.arange(cube_side)):
        if nodes[i, j, k] == 0:
            neighbor_colors = nodes[
                slice(*get_bounds(i, cube_side)),
                slice(*get_bounds(j, cube_side)),
                slice(*get_bounds(k, cube_side))
            ]
            avail_colors = np.setdiff1d(colors, neighbor_colors.flatten())
            color_idxs = avail_colors-1
            greedy_color_idx = np.argmin(color_counts[color_idxs])
            greedy_color = colors[color_idxs[greedy_color_idx]]
            color_counts[color_idxs[greedy_color_idx]] += 1
            
            nodes[i, j, k] = greedy_color
    
    return nodes


# ChatGPT
def split_zarr_group(ds, smaller_size, dims):
    """
    Takes an xarray group of arrays and splits it into smaller groups
    E.g. it takes a 2048^3 group of 6 arrays and returns 64 groups of 512^3

    Parameters:
        ds (xarray.Dataset): The Xarray group you want to split
        smaller_size (int): Size of the chunks along each dimension.
        dims (tuple): Names of the dimensions in order (dim_0, dim_1, dim_2). This can be gotten in reverse order using [dim for dim in data_xr.dims]
        
    Returns:
        list[xarray.Dataset]: A list of lists of lists of smaller Datasets.
    """

    # Calculate the number of chunks along each dimension
    num_chunks = [ds.dims[dim] // smaller_size for dim in dims]

    # I want this to be a 3D list of lists
    outer_dim = []
    
    range_list = [] # Where chunks start and end. Needed for Mike's code to find correct chunks to access

    for i in range(num_chunks[0]):
        mid_dim = []
        for j in range(num_chunks[1]):
            inner_dim = []

            for k in range(num_chunks[2]):
                # Select the chunk from each DataArray
                # These are first distributed along the last (i.e. the x)-index
                chunk = ds.isel(
                    {dims[0]: slice(i * smaller_size, (i + 1) * smaller_size), # nnz
                     dims[1]: slice(j * smaller_size, (j + 1) * smaller_size), # nny
                     dims[2]: slice(k * smaller_size, (k + 1) * smaller_size)} # nnx
                )

                inner_dim.append(chunk)
                
                a = []
                a.append([i * smaller_size, (i + 1) * smaller_size])
                a.append([j * smaller_size, (j + 1) * smaller_size])
                a.append([k * smaller_size, (k + 1) * smaller_size])
                
                range_list.append(a)

            mid_dim.append(inner_dim)

        outer_dim.append(mid_dim)

    return outer_dim, range_list


def list_fileDB_folders():
    return [f'/home/idies/workspace/turb/data{str(d).zfill(2)}_{str(f).zfill(2)}/zarr/'  for f in range(1,4) for d in range(1,13)]


def merge_velocities(transposed_ds, chunk_size_base=64):
    """
        Merge the 3 velocity components/directions - such merging exhibits faster 3-component reads. This is a Dask lazy computation
    """

    # Merge Velocities into 1
    b = da.stack([transposed_ds['u'], transposed_ds['v'], transposed_ds['w']], axis=3)
    b = b.squeeze() # It should be (2048, 2048, 2048, 3, 1) before this. Use (2048, 2048, 2048, 3)
    # Make into correct chunk sizes
    b = b.rechunk((chunk_size_base,chunk_size_base,chunk_size_base,3)) # Dask chooses (64,64,64,1)
    result = transposed_ds.drop_vars(['u', 'v', 'w'])  # Drop individual velocities

    # Add joined velocity to original group
    result['velocity'] = xr.DataArray(b, dims=('nnz', 'nny', 'nnx', 'velocity component (xyz)')) # Can't make the dim name same as scalars


    return result


def morton_pack(array_cube_side, x,y,z):
    bits = int(math.log(array_cube_side, 2))
    mortoncurve = morton.Morton(dimensions = 3, bits = bits)

    return mortoncurve.pack(x, y, z)


def get_sorted_morton_list(range_list, array_cube_side=2048):
    sorted_morton_list = [] # Sorting by Morton code to be consistent with Isotropic8192

    for i in range(len(range_list)):
        min_coord = [a[0] for a in range_list[i]]
        max_coord = [a[1] - 1 for a in range_list[i]]
                
        sorted_morton_list.append((morton_pack(array_cube_side, min_coord[0], min_coord[1], min_coord[2]), morton_pack(array_cube_side, max_coord[0], max_coord[1], max_coord[2])))
            
    sorted_morton_list = sorted(sorted_morton_list)

    return sorted_morton_list


def get_chunk_morton_mapping(range_list, dest_folder_name):
    """
    Get names of chunks e.g. sabl2048b01, sabl2048b02, etc. and their corresponding first and last point Morton codes

    :param range_list: 3D list of subarray cubes (e.g. 2048-cube is split into 512-cubes of 4x4x4). This 4x4x4 list is `range_list`
    :param dest_folder_name: Name of the destination folder (e.g. sabl2048b)
    """
    sorted_morton_list = get_sorted_morton_list(range_list)

    chunk_morton_mapping = {}
    for i in range(len(range_list)):
        chunk_morton_mapping[dest_folder_name + str(i + 1).zfill(2)] = sorted_morton_list[i]
    
    return chunk_morton_mapping


# This always fails with Kernel Died error on SciServer Jobs
# @dask.delayed
# def write_to_disk_dask(dest_groupname, current_array, encoding):
    # return write_to_disk(dest_groupname, current_array, encoding)


def flatten_3d_list(lst_3d):
    return [element for sublist_2d in lst_3d for sublist_1d in sublist_2d for element in sublist_1d]


def search_dict_by_value(dictionary, value):
    for key, val in dictionary.items():
        if val == value:
            return key
    return None  # Value not found in the dictionary


def write_to_disk(q):
    """
    Spawn threads to write Zarr cubes to disk. Do not use Dask for this as seems to cause
    worse performance than Threads

    Args:
        q (Queue): Queue of jobs
    """
    while True:
        try:
            chunk, dest_groupname, encoding = q.get(timeout=10)  # Adjust timeout as necessary

            print(f"Starting write to {dest_groupname}...")
            chunk.to_zarr(store=dest_groupname, mode="w", encoding=encoding)
            print(f"Finished writing to {dest_groupname}.")
        except queue.Empty:
            break  # Break the loop if no items are left in the queue
        # finally: # I think this is too many task_done() calls
        #     q.task_done()


def get_sharding_queue(dataset):
    """
    NetCDF data is sharded into smaller chunks, which are distributed round-robin.
    This method gathers these shards' paths and adds them to queue to compare their data with the original.
    """
    # TODO Call this using dataset Class. I don't like this hard-coding
    raw_ncar_folder_paths = [
        '/home/idies/workspace/turb/data02_02/ncar-high-rate-fixed-dt',
        '/home/idies/workspace/turb/data02_03/ncar-high-rate-fixed-dt',
    ]

    queue = []
    # TODO update with config.yaml
    array_cube_side = 2048
    dest_folder_name = "sabl2048b"
    write_type = "prod"

    timestep_nr = int(os.environ.get('TIMESTEP_NR'))
    if timestep_nr < 50:
        raw_ncar_folder_path = raw_ncar_folder_paths[0]
    else:
        raw_ncar_folder_path = raw_ncar_folder_paths[1]

    file_path = os.path.join(raw_ncar_folder_path, f"jhd.{str(timestep_nr).zfill(3)}.nc")
    cubes, _ = dataset.prepare_data(file_path)
    cubes = flatten_3d_list(cubes)

    dests = dataset.get_zarr_array_destinations(dest_folder_name, write_type, timestep_nr, array_cube_side)

    for i in range(len(dests)):
        queue.append((cubes[i], dests[i]))

    return queue


def extract_timestep_from_filename(filename):
    """
    Extracts the timestep number from a filename.

    Assumes that the filename contains a numeric part which represents the timestep.
    This numeric part may or may not be zero-padded.

    :param filename: The filename to extract the timestep from.
    :return: The extracted timestep as an integer, or None if no numeric part is found.
    """
    # Extract the basename in case a full path is provided
    basename = os.path.basename(filename)

    # Regular expression to find one or more digits
    match = re.search(r'\d+', basename)

    if match:
        return int(match.group())
    else:
        raise ValueError(f'No numeric part found in filename {filename}. This is needed to determine the timestep nr.')
