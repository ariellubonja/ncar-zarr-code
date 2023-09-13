import unittest
import zarr
from dask.array.utils import assert_eq
import dask.array as da
from utils import write_tools
from utils.write_tools import flatten_3d_list
import argparse, sys

array_cube_side = 2048
raw_ncar_folder_path = '/home/idies/workspace/turb/data02_02/ncar-high-rate-fixed-dt'
dest_folder_name = "sabl2048b"  # B is the high-rate data
write_type = "prod"  # or "back" for backup


class VerifyWriteTest(unittest.TestCase):

    def setUp(self):
        timestep_nr = args.timestep
        self.queue = []

        cubes, _ = write_tools.prepare_data(raw_ncar_folder_path + "/jhd." + str(timestep_nr).zfill(3) + ".nc")
        cubes = flatten_3d_list(cubes)

        dests = write_tools.get_512_chunk_destinations(dest_folder_name, write_type, timestep_nr, array_cube_side)

        for i in range(len(dests)):
            self.queue.append((cubes[i], dests[i]))

    def test_data_comparison(self):
        for original_512, zarr_512_path in self.queue:
            with self.subTest():
                self.verify_512_cube_data(original_512, zarr_512_path)

    def test_dimensions(self):
        for _, zarr_512_path in self.queue:
            with self.subTest(msg=f"Testing dimensions from {zarr_512_path}"):
                self.verify_512_cube_dimensions(zarr_512_path)

    def test_chunk_sizes(self):
        for _, zarr_512_path in self.queue:
            with self.subTest(msg=f"Testing chunk sizes from {zarr_512_path}"):
                self.verify_512_cube_chunk_sizes(zarr_512_path)

    def test_compression(self):
        for _, zarr_512_path in self.queue:
            with self.subTest(msg=f"Testing compression from {zarr_512_path}"):
                self.verify_512_cube_compression(zarr_512_path)

    def verify_512_cube_data(self, original_512, zarr_512_path):
        zarr_512 = zarr.open_group(zarr_512_path, mode='r')
        print("Testing data from ", zarr_512_path)
        for var in original_512.data_vars:
            print(var)
            assert_eq(original_512[var].data, da.from_zarr(zarr_512[var]))


    def verify_512_cube_dimensions(self, zarr_512_path):
        zarr_512 = zarr.open_group(zarr_512_path, mode='r')
        for var in zarr_512.array_keys():
            expected_shape = (512, 512, 512, 3) if var == "velocity" else (512, 512, 512, 1)
            self.assertEqual(zarr_512[var].shape, expected_shape)

    def verify_512_cube_chunk_sizes(self, zarr_512_path):
        zarr_512 = zarr.open_group(zarr_512_path, mode='r')
        for var in zarr_512.array_keys():
            expected_chunksize = (64, 64, 64, 3) if var == "velocity" else (64, 64, 64, 1)
            self.assertEqual(zarr_512[var].chunks, expected_chunksize)

    def verify_512_cube_compression(self, zarr_512_path):
        zarr_512 = zarr.open_group(zarr_512_path, mode='r')
        for var in zarr_512.array_keys():
            self.assertIsNone(zarr_512[var].compressor)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--timestep', type=int, required=True)
    args = parser.parse_args()

    unittest.main(argv=[sys.argv[0]])
