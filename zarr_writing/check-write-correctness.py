import unittest
import zarr
import numpy as np
from utils import write_tools
from utils.write_tools import flatten_3d_list, search_dict_by_value
import argparse, sys
import os

array_cube_side = 2048
raw_ncar_folder_path = '/home/idies/workspace/turb/data02_02/ncar-high-rate-fixed-dt'
dest_folder_name = "sabl2048b"  # B is the high-rate data
write_type = "prod"  # or "back" for backup


class VerifyWriteTest(unittest.TestCase):

    def setUp(self):
        timestep_nr = args.timestep
        self.queue = []

        folders = write_tools.list_fileDB_folders()

        # Avoiding 7-2 and 9-2 - they're too full as of May 2023
        folders.remove("/home/idies/workspace/turb/data09_02/zarr/")
        folders.remove("/home/idies/workspace/turb/data07_02/zarr/")

        for i in range(len(folders)):
            folders[i] += dest_folder_name + "_" + str(i + 1).zfill(2) + "_" + write_type + "/"

        cubes, range_list = write_tools.prepare_data(raw_ncar_folder_path + "/jhd." + str(timestep_nr).zfill(3) + ".nc")
        cubes = flatten_3d_list(cubes)

        chunk_morton_mapping = write_tools.get_chunk_morton_mapping(range_list, dest_folder_name)
        node_assignments = write_tools.node_assignment(cube_side=4)
        flattened_node_assgn = flatten_3d_list(node_assignments)

        for i in range(len(range_list)):
            min_coord = [a[0] for a in range_list[i]]
            max_coord = [a[1] - 1 for a in range_list[i]]

            morton = (
                write_tools.morton_pack(array_cube_side, min_coord[2], min_coord[1], min_coord[0]),
                write_tools.morton_pack(array_cube_side, max_coord[2], max_coord[1], max_coord[0])
            )

            chunk_name = search_dict_by_value(chunk_morton_mapping, morton)

            idx = int(chunk_name[-2:].lstrip('0'))

            filedb_index = flattened_node_assgn[idx - 1] - 1

            destination = os.path.join(folders[filedb_index],
                                       dest_folder_name + str(idx).zfill(2) + "_" + str(timestep_nr).zfill(3) + ".zarr")

            current_array = cubes[i]

            self.queue.append((current_array, destination))

    def test_data_comparison(self):
        for original_512, zarr_512_path in self.queue:
            with self.subTest(msg=f"Testing data from {zarr_512_path}"):
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
        for var in original_512.data_vars:
            np.testing.assert_array_equal(original_512[var].values, zarr_512[var][:])

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
