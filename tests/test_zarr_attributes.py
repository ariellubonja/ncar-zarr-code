"""
Ariel Lubonja
2024-01-10

Checks whether the written NCAR data has the correct Zarr attributes
Tests whole folders at a time. Only need to specify DATASET env var.
"""

import unittest
import zarr
import yaml
from parameterized import parameterized
import os

from src.dataset import NCAR_Dataset


config = {}
with open('tests/config.yaml', 'r') as file:
    config = yaml.safe_load(file)
dataset_name = os.environ.get('DATASET', 'NCAR-High-Rate-1')
start_timestep = int(os.environ.get('START_TIMESTEP', 40))
end_timestep = int(os.environ.get('END_TIMESTEP', 50))
prod_or_backup = str(os.environ.get('PROD_OR_BACKUP', 'prod'))


# Cannot call class method using Parameterized, so have to add this fn. outside the class
def generate_attribute_tests():
    # Access the global configuration variable
    global config, dataset_name

    dataset_config = config['datasets'][dataset_name]
    write_config = config['write_settings']
    dataset = NCAR_Dataset(
        name=dataset_config['name'],
        location_path=dataset_config['location_path'],
        desired_zarr_chunk_size=write_config['desired_zarr_chunk_length'],
    desired_zarr_array_length=write_config['desired_zarr_array_length'],
    prod_or_backup='prod',
        start_timestep=dataset_config['start_timestep'],
        end_timestep=dataset_config['end_timestep']
    )

    test_params = []
    for dataset_name, dataset_config in config['datasets'].items():
        for timestep in range(dataset_config['start_timestep'], dataset_config['end_timestep'] + 1):
            test_params.append((dataset, timestep))

    return test_params


class VerifyNCARZarrAttributes(unittest.TestCase):
    # Cannot have setUp or setupClass because they don't work with Parameterized
    @parameterized.expand(generate_attribute_tests)
    def test_individual_timestep(self, dataset, timestep):
        _, range_list = dataset.transform_to_zarr(timestep)
        destination_paths = dataset.get_zarr_array_destinations(timestep, range_list)

        for zarr_512_path in destination_paths:
            with self.subTest(timestep=timestep):
                self.run_tests_for_single_file(zarr_512_path)

    def run_tests_for_single_file(self, zarr_512_path):
        zarr_512 = zarr.open_group(zarr_512_path, mode='r')
        self.verify_zarr_array_dims(zarr_512, zarr_512_path)
        self.verify_zarr_chunk_sizes(zarr_512, zarr_512_path)
        self.verify_zarr_compression(zarr_512, zarr_512_path)

    # TODO get Zarr group size from config.yaml
    def verify_zarr_array_dims(self, zarr_512, zarr_512_path):
        """
        Verify that the cube dimensions are as expected. Should be (512, 512, 512, 3) for velocity, (512, 512, 512, 1)
         otherwise
        """
        for var in zarr_512.array_keys():
            expected_shape = (512, 512, 512, 3) if var == "velocity" else (512, 512, 512, 1)
            self.assertEqual(zarr_512[var].shape, expected_shape)

        if config['verbose']:
            print("Cube dimension = (512, 512, 512, x),  for all variables in ", zarr_512_path)

    def verify_zarr_chunk_sizes(self, zarr_512, zarr_512_path):
        for var in zarr_512.array_keys():
            expected_chunksize = (64, 64, 64, 3) if var == "velocity" else (64, 64, 64, 1)
            self.assertEqual(zarr_512[var].chunks, expected_chunksize)

        if config['verbose']:
            print("Chunk sizes = (64, 64, 64, x),  for all variables in ", zarr_512_path)

    def verify_zarr_compression(self, zarr_512, zarr_512_path):
        for var in zarr_512.array_keys():
            self.assertIsNone(zarr_512[var].compressor)  # TODO get from config.yaml

        if config['verbose']:
            print("Compression is None for all variables in ", zarr_512_path)
