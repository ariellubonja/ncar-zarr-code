import numpy as np


def sequential_access():
    for i in range(0, 100):
        print(i)


def uniform_random_8_interpolation(array, cube_shape, low=4, high=60, size=50):
    """
    Random Lagrangian 8-interpolation of array. Points picked uniformly at random between [low, high]
    :param array: array to be interpolated
    :param cube_shape: cube dimensions from within to pick points
    :param low: lower bound of cube e.g. 0 will be 0,0,0, meaning first chunk of array
    :param high: upper bound of cube e.g. 60 will be 60,60,60, meaning bottom-right chunk of array
    :param size: number of points to pick and around which to interpolate
    """

    rand_indices = np.array([np.random.randint(low=low, high=high, size=cube_shape[0]),
                                  np.random.randint(low=low, high=high, size=cube_shape[1]),
                                np.random.randint(low=low, high=high, size=cube_shape[2])]).T
    for index in range(len(rand_indices)):
        x = rand_indices[index][0]
        y = rand_indices[index][1]
        z = rand_indices[index][2]
        array[x-4:x+4, y-4:y+4, z-4:z+4]