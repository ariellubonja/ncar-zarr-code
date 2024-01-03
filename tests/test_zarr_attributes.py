import unittest
import zarr
import glob
import yaml

from src.utils import write_utils
from src.utils.read_utils import extract_netcdf_timestep
from src.dataset import NCAR_Dataset


# TODO Use parameterize to generate tests for each item in the queue
class VerifyZarrAttributes(unittest.TestCase):
    """
        Verify that the Zarr attributes are as expected. These include:
        - Cube dimensions. Should be (512, 512, 512, 3) for velocity, (512, 512, 512, 1) otherwise
        - Chunk sizes. Should be (64, 64, 64, 3) for velocity, (64, 64, 64, 1) otherwise
        - Compression. Should be None
    """
    def __init__(self):
        super().__init__()
        with open('tests/config.yaml', 'r') as file:
            self.config = yaml.safe_load(file)

        self.ncar_dataset = NCAR_Dataset(name=self.config['dataset_name'],
                                         location_path=self.config['original_data_paths'],
                                         desired_zarr_chunk_size=self.config['desired_zarr_chunk_length'],
                                         desired_zarr_array_length=self.config['desired_zarr_chunk_length'],
                                         prod_or_backup=self.config['prod_or_backup'])

        self.lazy_zarr_cubes = self.ncar_dataset.transform_to_zarr()
        self.destination_paths = write_utils.get_zarr_array_destinations(self.ncar_dataset)

        # Preparing a list of all timestep files to test
        self.timestep_files = []
        for folder in self.config['original_data_paths']:
            self.timestep_files.extend(glob.glob(f"{folder}/jhd.*.nc"))  # TODO fix this hard-coding


    def test_all_timesteps(self):
        for file_path in self.timestep_files:
            timestep_nr = extract_netcdf_timestep(file_path)
            with self.subTest(timestep=timestep_nr):
                self.run_tests_for_single_file()

    def run_tests_for_single_file(self):
        # TODO rename these horrible variable names
        for original_512, zarr_512_path in zip(self.lazy_zarr_cubes, self.destination_paths):
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
