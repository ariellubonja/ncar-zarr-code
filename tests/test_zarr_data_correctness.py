import unittest
import zarr
from dask.array.utils import assert_eq
import dask.array as da
import yaml
import os
from parameterized import parameterized

from src.dataset import NCAR_Dataset


config = {}
with open('tests/config.yaml', 'r') as file:
    config = yaml.safe_load(file)
dataset_name = os.environ.get('DATASET', 'NCAR-High-Rate-1')
start_timestep = int(os.environ.get('START_TIMESTEP', 0))
end_timestep = int(os.environ.get('END_TIMESTEP', 2))


# Cannot call class method using Parameterized, so have to add this fn. outside the class
def generate_data_correctness_tests():
    global config, dataset_name, start_timestep, end_timestep

    print("start_timestep: ", start_timestep)
    print("end_timestep: ", end_timestep)

    test_params = []
    dataset_config = config['datasets'][dataset_name]
    write_config = config['write_settings']
    dataset = NCAR_Dataset(
        name=dataset_config['name'],
        location_path=dataset_config['location_path'],
        desired_zarr_chunk_size=write_config['desired_zarr_chunk_length'],
        desired_zarr_array_length=write_config['desired_zarr_chunk_length'],
        prod_or_backup=write_config['prod_or_backup'],
        start_timestep=dataset_config['start_timestep'],
        end_timestep=dataset_config['end_timestep']
    )

    for timestep in range(start_timestep, end_timestep + 1):
        print("Current timestep: ", timestep)
        lazy_zarr_cubes, range_list = dataset.transform_to_zarr(timestep)
        print("len of lazy_zarr_cubes: ", len(lazy_zarr_cubes))
        destination_paths = dataset.get_zarr_array_destinations(timestep, range_list)
        print("len of destination_paths: ", len(destination_paths))
        for original_data_cube, written_zarr_cube in zip(lazy_zarr_cubes, destination_paths):
            test_params.append((original_data_cube, written_zarr_cube))

    print("Done generating tests. Len of test_params: ", len(test_params))
    return test_params


# TODO tons of duplicated code with test_zarr_attributes.py
class VerifyZarrDataCorrectness(unittest.TestCase):
    # Cannot have setUp or setupClass because they don't work with Parameterized

    @parameterized.expand(generate_data_correctness_tests())
    def test_zarr_group_data(self, original_subarray, zarr_group_path):
        '''
        Verify correctness of data contained in each zarr group against
        the original data file (NCAR NetCDF)

        Args:
            original_subarray (xarray.Dataset): (Sub)Array of original data that was written as zarr to zar_group_path
            zarr_group_path (str): Location of the sub-chunked data as a Zarr Group
        '''
        zarr_group = zarr.open_group(zarr_group_path, mode='r')
        print("Comparing original 512^3 with ", zarr_group_path)
        for var in original_subarray.data_vars:
            assert_eq(original_subarray[var].data, da.from_zarr(zarr_group[var]))
            if config['verbose']:
                print(var, " OK")

