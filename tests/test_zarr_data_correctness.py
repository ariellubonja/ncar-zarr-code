import unittest
import zarr
from dask.array.utils import assert_eq
import dask.array as da
import os

from utils import write_tools
from utils.write_tools import flatten_3d_list


array_cube_side = 2048
raw_ncar_folder_paths = {
    'data02_02': '/home/idies/workspace/turb/data02_02/ncar-high-rate-fixed-dt',
    'data02_03': '/home/idies/workspace/turb/data02_03/ncar-high-rate-fixed-dt',
}
dest_folder_name = "sabl2048b"  # B is the high-rate data
write_type = "prod"  # or "back" for backup

test_files = 'data02_03' # TODO figure out how to programmatically set this. Argument parsing is an antipattern in testing


class VerifyWriteTest(unittest.TestCase):

    def setUp(self):
        timestep_nr = args.timestep
        ncar_path = args.ncar_path
        raw_ncar_folder_path = raw_ncar_folder_paths.get(ncar_path)

        self.queue = []

        cubes, _ = write_tools.prepare_data(raw_ncar_folder_path + "/jhd." + str(timestep_nr).zfill(3) + ".nc")
        cubes = flatten_3d_list(cubes)

        dests = write_tools.get_512_chunk_destinations(dest_folder_name, write_type, timestep_nr, array_cube_side)

        for i in range(len(dests)):
            self.queue.append((cubes[i], dests[i]))


    def verify_512_cube_data(self, original_512, zarr_512_path):
        zarr_512 = zarr.open_group(zarr_512_path, mode='r')
        print("Comparing original 512^3 with ", zarr_512_path)
        for var in original_512.data_vars:
            assert_eq(original_512[var].data, da.from_zarr(zarr_512[var]))
            print(var, " OK")
