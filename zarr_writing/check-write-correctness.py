import unittest
import xarray as xr
import numpy as np
import zarr
import dask
import os
from utils import write_tools


original_data_path = '/home/idies/workspace/turb/data02_02/ncar-high-rate-fixed-dt/'
dest_folder_name = "sabl2048b"


class TestZarrSplit(unittest.TestCase):

    @dask.delayed
    def load_and_check(self, i, j, k, data_variables, original_groups):
        zarr_group = zarr.open(f'{i}_{j}_{k}_group.zarr', mode='r')
        
        for var in data_variables:
            xr_zarr_array = xr.DataArray(zarr_group[var])

            # Check shape of the extracted zarr cube
            assert xr_zarr_array.shape == (512, 512, 512)

            # Check values of the extracted zarr cube against the original cube
            np.testing.assert_array_equal(
                original_groups[var][512*i:512*(i+1), 512*j:512*(j+1), 512*k:512*(k+1)].compute(),
                xr_zarr_array
            )
    
    @dask.delayed
    def load_and_check(self, ncar_zarr_512_chunk_path):
        """
        Test whether NCAR data was written correctly to Zarr (i.e. whether it matches the original voxel by voxel)

        parameters:
            ncar_zarr_512_chunk (str): NCAR data in Zarr is split into 512^3 chunks. All variables and all timesteps for that 512^3 live on the same folder
                This is the path to this folder. Please give path to parent of timesteps e.g. /home/idies/workspace/turb/data01_01/zarr/sabl2048b_01_prod
        """

        first_timestep = zarr.open(os.path.join(ncar_zarr_512_chunk_path,"sabl2048b01_000.zarr"))

        # Complete this for 1 chunk, then generalize
        
        

# generate test cases for timesteps 1 to 5
def generate_test(timestep, path):
    def test(self):
        original_group = xr.open_dataset(path + "jhd." + str(timestep).zfill(3) + ".nc")
        # Merge velocity components to compare with Zarr
        original_group = write_tools.merge_velocities(original_group)
        original_group = original_group.rename({'e': 'energy', 't': 'temperature', 'p': 'pressure'})
        
        group_keys = list(original_group.data_vars)

        # TODO Do we really need to change the chunk size?
        original_groups = {key: original_group[key].chunk((512, 512, 512)) for key in group_keys}
        
        folders = write_tools.list_fileDB_folders()

        order = write_tools.node_assignment(4) # Ryan's node assignment. Should return range(1-64) for cube_side=4

        tasks = []
        # Split the original array into 64 cubes (4x4x4 grid) 
        for i in range(4):
            for j in range(4):
                for k in range(4):
                    # filedb_index = order[i][j][k] % len(folders)

                    # dest_groupname = folders[filedb_index] + dest_folder_name + str(order[i][j][k] + 1).zfill(2) + "_" + str(timestep).zfill(2) + ".zarr"
                    
                    task = self.load_and_check(i, j, k, group_keys, original_groups)
                    tasks.append(task)
                    
        dask.compute(*tasks)

    return test


for i in range(0, 6):
    test_name = f'test_timestep_{i}'
    test = generate_test(i, original_data_path)
    setattr(TestZarrSplit, test_name, test)

if __name__ == '__main__':
    unittest.main()
