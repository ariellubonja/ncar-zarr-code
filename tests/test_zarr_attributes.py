import unittest
import zarr
import glob

from src.utils import write_utils
from src.utils.write_utils import flatten_3d_list
from src.utils.read_utils import extract_netcdf_timestep


# TODO Fix this, use parameterize to generate tests for each item in the queue
class VerifyZarrAttributes(unittest.TestCase):
    """
        Verify that the Zarr attributes are as expected. These include:
        - Cube dimensions. Should be (512, 512, 512, 3) for velocity, (512, 512, 512, 1) otherwise
        - Chunk sizes. Should be (64, 64, 64, 3) for velocity, (64, 64, 64, 1) otherwise
        - Compression. Should be None
    """

    @classmethod
    def setUpClass(cls):
        # Preparing a list of all timestep files to test
        cls.timestep_files = []
        for folder in write_utils.raw_ncar_folder_paths:
            cls.timestep_files.extend(glob.glob(f"{folder}/jhd.*.nc"))


    def test_all_timesteps(self):
        for file_path in self.timestep_files:
            timestep_nr = extract_netcdf_timestep(file_path)
            with self.subTest(timestep=timestep_nr):
                self.run_tests_for_single_file(file_path, timestep_nr)


    def run_tests_for_single_file(self, file_path, timestep_nr):
        # TODO Call NCARDataset class for this
        cubes, _ = write_utils.prepare_data(file_path)
        cubes = flatten_3d_list(cubes)

        dests = write_utils.get_512_chunk_destinations(self.dest_folder_name, self.write_type, timestep_nr, self.array_cube_side)

        for original_512, zarr_512_path in zip(cubes, dests):
            zarr_512 = zarr.open_group(zarr_512_path, mode='r')

            self.verify_512_cube_dimensions(zarr_512, zarr_512_path)
            self.verify_512_cube_chunk_sizes(zarr_512, zarr_512_path)
            self.verify_512_cube_compression(zarr_512, zarr_512_path)


    def verify_512_cube_dimensions(self, zarr_512, zarr_512_path):
        for var in zarr_512.array_keys():
            expected_shape = (512, 512, 512, 3) if var == "velocity" else (512, 512, 512, 1)
            self.assertEqual(zarr_512[var].shape, expected_shape)

        print("Cube dimension = (512, 512, 512, x),  for all variables in ", zarr_512_path)


    def verify_512_cube_chunk_sizes(self, zarr_512, zarr_512_path):
        for var in zarr_512.array_keys():
            expected_chunksize = (64, 64, 64, 3) if var == "velocity" else (64, 64, 64, 1)
            self.assertEqual(zarr_512[var].chunks, expected_chunksize)

        print("Chunk sizes = (64, 64, 64, x),  for all variables in ", zarr_512_path)


    def verify_512_cube_compression(self, zarr_512, zarr_512_path):
        for var in zarr_512.array_keys():
            self.assertIsNone(zarr_512[var].compressor)

        print("Compression is None for all variables in ", zarr_512_path)
