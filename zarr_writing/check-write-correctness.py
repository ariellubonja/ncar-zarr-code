import unittest
import xarray as xr
import numpy as np
import zarr
import dask
import os
import write_tools

class TestZarrSplit(unittest.TestCase):

    @dask.delayed
    def load_and_check(self, i, j, k, group_keys, original_groups):
        zarr_group = zarr.open(f'{i}_{j}_{k}_group.zarr', mode='r')
        
        for key in group_keys:
            xr_zarr_array = xr.DataArray(zarr_group[key])

            # Check shape of the extracted zarr cube
            assert xr_zarr_array.shape == (512, 512, 512)

            # Check values of the extracted zarr cube against the original cube
            np.testing.assert_array_equal(
                original_groups[key][512*i:512*(i+1), 512*j:512*(j+1), 512*k:512*(k+1)].compute(),
                xr_zarr_array
            )

def generate_test(timestep, path):
    def test(self):
        original_group = xr.open_dataset(path + "jhd." + str(timestep).zfill(3) + ".nc")
        # Merge velocity components to compare with Zarr
        original_group = write_tools.merge_velocities(original_group)
        original_group = original_group.rename({'e': 'energy', 't': 'temperature', 'p': 'pressure'})
        
        group_keys = list(original_group.data_vars)
        original_groups = {key: original_group[key].chunk((512, 512, 512)) for key in group_keys}
        
        folders = write_tools.list_fileDB_folders()

        tasks = []
        # Split the original array into 64 cubes (4x4x4 grid) 
        for i in range(4):
            for j in range(4):
                for k in range(4):
                    filedb_index = z_order[i][j][k] % len(folders)

                    dest_groupname = folders[filedb_index] + dest_folder_name + str(z_order[i][j][k] + 1).zfill(2) + "_" + str(timestep).zfill(2) + ".zarr"
                    
                    task = self.load_and_check(i, j, k, group_keys, original_groups)
                    tasks.append(task)
                    
        dask.compute(*tasks)

    return test

# generate test cases for timesteps 1 to 6
path = '/home/idies/workspace/turb/data02_02/ariel-6-timestep-ncar-netCDF/'
dest_folder_name = "sabl2048a"

def get_round_robined_paths(timestep):

    folders=write_tools.list_fileDB_folders()

    # Avoiding 7-2 and 9-2 - they're too full as of May 2023
    # folders.remove("/home/idies/workspace/turb/data02_02/zarr/ncar-zarr/")
    folders.remove("/home/idies/workspace/turb/data09_02/zarr/")
    folders.remove("/home/idies/workspace/turb/data07_02/zarr/") # This is already created

    for i in range(len(folders)):
        folders[i] += dest_folder_name + "_" + str(i + 1).zfill(2) + "_prod/"

    cube_root = 4

    z_order = []
    # Z-order the cube of points so they "linearize" far from each other
    for i in range(cube_root):
        a = []
        for j in range(cube_root):
            b = []
            for k in range(cube_root):
                z_position = write_tools.morton_pack(cube_root, i,j,k)
                b.append(z_position)
            a.append(b)
        z_order.append(a)

    # Calculate the number of chunks along each dimension
    num_chunks = 4

    # I want this to be a 3D list of lists
    outer_dim = []

    for i in range(num_chunks):
        mid_dim = []
        for j in range(num_chunks):
            inner_dim = []

            for k in range(num_chunks):
                inner_dim.append(str(i)+str(j)+str(k))

            mid_dim.append(inner_dim)

        outer_dim.append(mid_dim)


    folders_to_delete = []

    for i in range(num_chunks):
        for j in range(num_chunks):
            for k in range(num_chunks):
                # Put e.g. z-order=4 to filedb05_01
                filedb_index = z_order[i][j][k] % len(folders) # Loop around
                current_array = outer_dim[i][j][k]

                folders_to_delete.append(folders[filedb_index] + dest_folder_name + str(z_order[i][j][k]).zfill(2) + "_" + str(timestep).zfill(2) + ".zarr")
    
    return folders_to_delete

for i in range(0, 6):
    test_name = f'test_timestep_{i}'
    test = generate_test(i, path)
    setattr(TestZarrSplit, test_name, test)

if __name__ == '__main__':
    unittest.main()
