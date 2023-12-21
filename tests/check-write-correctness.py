import unittest
import zarr
from dask.array.utils import assert_eq
import dask.array as da
import argparse, sys, os

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from utils import write_tools
from utils.write_tools import flatten_3d_list


array_cube_side = 2048
raw_ncar_folder_path_02_02 = '/home/idies/workspace/turb/data02_02/ncar-high-rate-fixed-dt'
raw_ncar_folder_path_02_03 = '/home/idies/workspace/turb/data02_03/ncar-high-rate-fixed-dt'
dest_folder_name = "sabl2048b"  # B is the high-rate data
write_type = "prod"  # or "back" for backup


class VerifyWriteTest(unittest.TestCase):

    def setUp(self):
        timestep_nr = args.timestep
        raw_ncar_folder_path = args.ncar_path

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
            with self.subTest():
                self.verify_512_cube_dimensions(zarr_512_path)

    def test_chunk_sizes(self):
        for _, zarr_512_path in self.queue:
            with self.subTest():
                self.verify_512_cube_chunk_sizes(zarr_512_path)

    def test_compression(self):
        for _, zarr_512_path in self.queue:
            with self.subTest():
                self.verify_512_cube_compression(zarr_512_path)

    def verify_512_cube_data(self, original_512, zarr_512_path):
        zarr_512 = zarr.open_group(zarr_512_path, mode='r')
        print("Comparing original 512^3 with ", zarr_512_path)
        for var in original_512.data_vars:
            assert_eq(original_512[var].data, da.from_zarr(zarr_512[var]))
            print(var, " OK")


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


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--timestep', type=int, required=True, help='NCAR Timestep nr. to process')
    parser.add_argument('--ncar_path', type=str, required=True, choices=['data02_02', 'data02_03'], help='FileDB folder where NCAR data is located. as of Dec-2023, this is either data02_02 or data02_03')
    args = parser.parse_args()

    unittest.main(argv=[sys.argv[0]])
