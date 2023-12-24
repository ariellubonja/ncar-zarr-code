import unittest
import zarr
import glob
import os

from utils import write_tools
from utils.write_tools import flatten_3d_list

class VerifyWriteTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.array_cube_side = 2048
        cls.dest_folder_name = "sabl2048b"  # B is the high-rate data
        cls.write_type = "prod"  # or "back" for backup

        cls.raw_ncar_folder_paths = [
            '/home/idies/workspace/turb/data02_02/ncar-high-rate-fixed-dt',
            '/home/idies/workspace/turb/data02_03/ncar-high-rate-fixed-dt',
        ]
        cls.timestep_files = []

        # Preparing a list of all timestep files to test
        cls.timestep_files = []
        for folder in cls.raw_ncar_folder_paths:
            cls.timestep_files.extend(glob.glob(f"{folder}/jhd.*.nc"))

    def test_all_timesteps(self):
        for file_path in self.timestep_files:
            timestep_nr = self.extract_timestep(file_path)
            with self.subTest(timestep=timestep_nr):
                self.run_tests_for_single_file(file_path, timestep_nr)

    def extract_timestep(self, file_path):
        # Extract the filename from the full file path
        filename = os.path.basename(file_path)
        # Split the filename and extract the part with the timestep number
        timestep_part = filename.split('.')[1]
        # Convert the extracted part to an integer
        return int(timestep_part)

    def run_tests_for_single_file(self, file_path, timestep_nr):
        cubes, _ = write_tools.prepare_data(file_path)
        cubes = flatten_3d_list(cubes)

        dests = write_tools.get_512_chunk_destinations(self.dest_folder_name, self.write_type, timestep_nr, self.array_cube_side)

        for original_512, zarr_512_path in zip(cubes, dests):
            self.verify_512_cube_dimensions(zarr_512_path)
            self.verify_512_cube_chunk_sizes(zarr_512_path)
            self.verify_512_cube_compression(zarr_512_path)


    def verify_512_cube_dimensions(self, zarr_512_path):
        zarr_512 = zarr.open_group(zarr_512_path, mode='r')
        for var in zarr_512.array_keys():
            expected_shape = (512, 512, 512, 3) if var == "velocity" else (512, 512, 512, 1)
            self.assertEqual(zarr_512[var].shape, expected_shape)

        print("Cube dimension = (512, 512, 512, x),  for all variables in ", zarr_512_path)

    def verify_512_cube_chunk_sizes(self, zarr_512_path):
        zarr_512 = zarr.open_group(zarr_512_path, mode='r')
        for var in zarr_512.array_keys():
            expected_chunksize = (64, 64, 64, 3) if var == "velocity" else (64, 64, 64, 1)
            self.assertEqual(zarr_512[var].chunks, expected_chunksize)

        print("Chunk sizes = (64, 64, 64, x),  for all variables in ", zarr_512_path)

    def verify_512_cube_compression(self, zarr_512_path):
        zarr_512 = zarr.open_group(zarr_512_path, mode='r')
        for var in zarr_512.array_keys():
            self.assertIsNone(zarr_512[var].compressor)

        print("Compression is None for all variables in ", zarr_512_path)
