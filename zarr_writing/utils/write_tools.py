import numpy as np
import xarray as xr
import os
import dask.array as da
from dask.distributed import Client
import math, queue
from itertools import product

import subprocess
import sys

try:
    import morton
except ImportError:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'morton-py'])
finally:
    import morton


def prepare_data(xr_path, desired_cube_side=512, chunk_size=64, dask_local_dir='/home/idies/workspace/turb/data02_02', n_dask_workers=4): # use_dask=True
    """
    Prepare data for writing to FileDB. This includes:
        - Merging velocity components
        - Splitting into smaller chunks (64^3)
        - Unabbreviating variable names
        - Splitting 2048^3 arrays into 512^3 chunks
    
    Assuming we'll always use dask bcs. why not
    """

    print("Started preparing NetCDF data for verification. This will take ~20min")
    client = Client(n_workers=n_dask_workers, local_directory=dask_local_dir)
    data_xr = xr.open_dataset(xr_path)

    # Group 3 velocity components together
    # This fails with Dask bcs. of write permission error on SciServer Job
    # Never use dask with remote location on this!!
    merged_velocity = merge_velocities(data_xr, chunk_size_base=chunk_size)

    client.close()

    # Unabbreviate 'e', 'p', 't' variable names
    merged_velocity = merged_velocity.rename({'e': 'energy', 't': 'temperature', 'p': 'pressure'})

    dims = [dim for dim in data_xr.dims]
    dims.reverse() # use (nnz, nny, nnx) instead of (nnx, nny, nnz)

    # Split 2048^3 into smaller 512^3 arrays
    smaller_groups, range_list = split_zarr_group(merged_velocity, desired_cube_side, dims)

    print('Done preparing data. Starting to verify...')

    return smaller_groups, range_list


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


def merge_velocities(data_xr, chunk_size_base=64):
    """
        Merge the 3 velocity components/directions - such merging exhibits faster 3-component reads. This is a Dask lazy computation
        
        :param data_xr: the dataset (Xarray group) with 3 velocity components to merge
    """

    b = da.stack([data_xr['e']], axis=3)
    b = b.rechunk((64,64,64,1))
    data_xr['e'] = xr.DataArray(b, dims=('nnz', 'nny', 'nnx', 'extra_dim'))

    b = da.stack([data_xr['t']], axis=3)
    b = b.rechunk((64,64,64,1))
    data_xr['t'] = xr.DataArray(b, dims=('nnz', 'nny', 'nnx', 'extra_dim'))

    b = da.stack([data_xr['p']], axis=3)
    b = b.rechunk((64,64,64,1))
    data_xr['p'] = xr.DataArray(b, dims=('nnz', 'nny', 'nnx', 'extra_dim'))

    # Merge Velocities into 1
    b = da.stack([data_xr['u'], data_xr['v'], data_xr['w']], axis=3)
    # Make into correct chunk sizes
    b = b.rechunk((chunk_size_base,chunk_size_base,chunk_size_base,3))
    result = data_xr.drop_vars(['u', 'v', 'w'])  # Drop individual velocities

    # Add joined velocity to original group
    result['velocity'] = xr.DataArray(b, dims=('nnz', 'nny', 'nnx', 'velocity component (xyz)'))


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


def get_512_chunk_destinations(dest_folder_name, write_type, timestep_nr, array_cube_side=2048):
    folders = list_fileDB_folders()

    # Avoiding 7-2 and 9-2 - they're too full as of May 2023
    folders.remove("/home/idies/workspace/turb/data09_02/zarr/")
    folders.remove("/home/idies/workspace/turb/data07_02/zarr/")

    for i in range(len(folders)):
        folders[i] += dest_folder_name + "_" + str(i + 1).zfill(2) + "_" + write_type + "/"

    range_list = []  # Where chunks start and end. Needed for Mike's code to find correct chunks to access
    smaller_size = 512
    outer_dim = []

    for i in range(4):
        mid_dim = []
        for j in range(4):
            inner_dim = []

            for k in range(4):
                a = []
                a.append([i * smaller_size, (i + 1) * smaller_size])
                a.append([j * smaller_size, (j + 1) * smaller_size])
                a.append([k * smaller_size, (k + 1) * smaller_size])

                range_list.append(a)

            mid_dim.append(inner_dim)

        outer_dim.append(mid_dim)

    chunk_morton_mapping = get_chunk_morton_mapping(range_list, dest_folder_name)
    flattened_node_assgn = flatten_3d_list(node_assignment(4))

    dests = []

    for i in range(len(range_list)):
        #     for j in range(4):
        #         for k in range(4):
        min_coord = [a[0] for a in range_list[i]]
        max_coord = [a[1] - 1 for a in range_list[i]]

        morton = (morton_pack(array_cube_side, min_coord[2], min_coord[1], min_coord[0]),
                  morton_pack(array_cube_side, max_coord[2], max_coord[1], max_coord[0]))

        chunk_name = search_dict_by_value(chunk_morton_mapping, morton)

        idx = int(chunk_name[-2:].lstrip('0'))

        filedb_index = flattened_node_assgn[idx - 1] - 1

        destination = os.path.join(folders[filedb_index],
                                   dest_folder_name + str(idx).zfill(2) + "_" + str(timestep_nr).zfill(3) + ".zarr")

        dests.append(destination)

    return dests


def write_to_disk(q):
    while True:
        try:
            chunk, dest_groupname, encoding = q.get(timeout=10)  # Adjust timeout as necessary

            print(f"Starting write to {dest_groupname}...")
            chunk.to_zarr(store=dest_groupname, mode="w", encoding=encoding)
            print(f"Finished writing to {dest_groupname}.")
        except queue.Empty:
            break
        finally:
            q.task_done()