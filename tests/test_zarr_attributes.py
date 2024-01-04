import unittest
import zarr
import yaml
from parameterized import parameterized

from src.utils import write_utils
from src.dataset import NCAR_Dataset


class VerifyNCARZarrAttributes(unittest.TestCase):
    def setUpClass(cls):
        with open('tests/config.yaml', 'r') as file:
            cls.config = yaml.safe_load(file)

        # Initialize NCAR_Dataset High Rate
        cls.ncar_datasets = [
            NCAR_Dataset(
                name='sabl2048b',
                location_path=cls.config['NCAR_high_rate_paths'][0],
                desired_zarr_chunk_size=cls.config['desired_zarr_chunk_length'],
                desired_zarr_array_length=cls.config['desired_zarr_chunk_length'],
                prod_or_backup=cls.config['prod_or_backup'],
                start_timestep=0,
                end_timestep=49
            ),
            NCAR_Dataset(
                name='sabl2048b',
                location_path=cls.config['NCAR_high_rate_paths'][1],  # Split across 2 dirs
                desired_zarr_chunk_size=cls.config['desired_zarr_chunk_length'],
                desired_zarr_array_length=cls.config['desired_zarr_chunk_length'],
                prod_or_backup=cls.config['prod_or_backup'],
                start_timestep=50,
                end_timestep=99
            ),
            NCAR_Dataset(
                name='sabl2048a',
                location_path=cls.config['NCAR_low_rate_path'][0],  # List of 1 element,
                desired_zarr_chunk_size=cls.config['desired_zarr_chunk_length'],
                desired_zarr_array_length=cls.config['desired_zarr_chunk_length'],
                prod_or_backup=cls.config['prod_or_backup'],
                start_timestep=0,
                end_timestep=19
            )
        ]

    @classmethod
    def generate_test_cases(cls):
        test_cases = []
        for dataset in cls.ncar_datasets:
            # Using dataset name and timestep range as the label and parameters
            label = f"{dataset.name}_{dataset.start_timestep}_to_{dataset.end_timestep}"
            test_cases.append((label, dataset.start_timestep, dataset.end_timestep))
        return test_cases

    @parameterized.expand(generate_test_cases())
    def test_all_timesteps(self):
        for dataset in self.ncar_datasets:
            for timestep in range(dataset.start_timestep, dataset.end_timestep + 1):
                lazy_zarr_cubes = dataset.transform_to_zarr(timestep)
                destination_paths = write_utils.get_zarr_array_destinations(dataset, timestep)

                for original_512, zarr_512_path in zip(lazy_zarr_cubes, destination_paths):
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
        Verify that the cube dimensions are as expected. Should be (512, 512, 512, 3) for velocity, (512, 512, 512, 1) otherwise
        """
        for var in zarr_512.array_keys():
            expected_shape = (512, 512, 512, 3) if var == "velocity" else (512, 512, 512, 1)
            self.assertEqual(zarr_512[var].shape, expected_shape)

        print("Cube dimension = (512, 512, 512, x),  for all variables in ", zarr_512_path)

    def verify_zarr_chunk_sizes(self, zarr_512, zarr_512_path):
        for var in zarr_512.array_keys():
            expected_chunksize = (64, 64, 64, 3) if var == "velocity" else (64, 64, 64, 1)
            self.assertEqual(zarr_512[var].chunks, expected_chunksize)

        print("Chunk sizes = (64, 64, 64, x),  for all variables in ", zarr_512_path)

    def verify_zarr_compression(self, zarr_512, zarr_512_path):
        for var in zarr_512.array_keys():
            self.assertIsNone(zarr_512[var].compressor)  # TODO get from config.yaml

        print("Compression is None for all variables in ", zarr_512_path)
