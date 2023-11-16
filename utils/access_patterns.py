import numpy as np


def sequential_8_interpolation(array, cube_shape, low=4, high=60, size=50):
    """
    Sequential access and Lagrangian 8-interpolation of array. Points picked sequentially between [low, high], forming an 8x8x8 cube
    :param array: array to be interpolated
    :param cube_shape: cube dimensions from within to pick points
    :param low: lower bound of cube e.g. 0 will be 0,0,0, meaning first chunk of array. Needs to be >= 4 for 8-interpolation
    :param high: upper bound of cube e.g. 60 will be 60,60,60, meaning bottom-right chunk of array. Needs to be <= len(array) - 4 for 8-interpolation
    :param size: number of points to pick and around which to interpolate
    """

    for index in range(low, min(high, low+size)):
        _ = array[index-4:index+4, index-4:index+4, index-4:index+4]



def create_random_indices(cube_shape, low=4, high=60, size=50):
    """
    Generate 3D random indices
    
    :param cube_shape: cube dimensions from within to pick points
    :param low: lower bound of cube e.g. 0 will be 0,0,0, meaning first chunk of array
    :param high: upper bound of cube e.g. 60 will be 60,60,60, meaning bottom-right chunk of array
    :param size: number of points to pick and around which to interpolate
    """
    
    rand_indices = np.array([np.random.randint(low=low, high=high, size=cube_shape[0]),
                              np.random.randint(low=low, high=high, size=cube_shape[1]),
                            np.random.randint(low=low, high=high, size=cube_shape[2])]).T
    
    return rand_indices
        

def index_8_interpolation(array, rand_indices):
    """
    Random Lagrangian 8-interpolation of array. Points picked uniformly at random between [low, high]
    :param array: array to be interpolated, forming an 8x8x8 cube
    :param rand_indices: list or array of indices to interpolate around in the given array
    """

    # Can't generate random indices this here bcs. it penalizes Single-variable experiments (ran 3 times vs. once for joint)

    for index in range(len(rand_indices)):
        x = rand_indices[index][0]
        y = rand_indices[index][1]
        z = rand_indices[index][2]
        _ = array[x-4:x+4, y-4:y+4, z-4:z+4]



def access_1_velocity_from_joint(xarray_arr, rand_indices):
    """
    :param xarray_arr: array to access from. Efficiently slice zarr using store = xarray.open_zarr(path), then use slice_3d = store.isel({'velocity component (xyz)': 0})
    :param rand_indices: list or array of indices to interpolate around in the given array
    """
    for row in rand_indices:
        # Define the slice
        slice_nnx = slice(row[0]-4, row[0]+4)
        slice_nny = slice(row[1]-4, row[1]+4)
        slice_nnz = slice(row[2]-4, row[2]+4)

        # Access a 3D subset of the array
        subset_3d = xarray_arr.isel(nnx=slice_nnx, nny=slice_nny, nnz=slice_nnz)

        _ = subset_3d.to_array().squeeze()