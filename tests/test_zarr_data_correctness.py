import unittest
import zarr
from dask.array.utils import assert_eq
import dask.array as da
import os
from parameterized import parameterized

from utils import write_tools
from utils.write_tools import flatten_3d_list


# TODO this is duplicated in test_zarr_attributes.py. Fix
raw_ncar_folder_paths = [
            '/home/idies/workspace/turb/data02_02/ncar-high-rate-fixed-dt',
            '/home/idies/workspace/turb/data02_03/ncar-high-rate-fixed-dt',
        ]


class VerifyZarrDataCorrectness(unittest.TestCase):

    array_cube_side = 2048
    dest_folder_name = "sabl2048b"  # B is the high-rate data
    write_type = "prod"  # or "back" for backup
    queue = []  # Define queue as a class variable

    @classmethod
    def setUp(cls):
        """
            NetCDF data is sharded into smaller chunks, which are distributed round-robin.
            This method gathers these shards' paths and adds them to queue to compare their data with the original.
        """

        # Read environment variables
        timestep_nr = int(os.environ.get('TIMESTEP_NR', 0))
        if timestep_nr < 50:
            raw_ncar_folder_path = raw_ncar_folder_paths[0]
        else:
            raw_ncar_folder_path = raw_ncar_folder_paths[1]

        cls.queue = []

        file_path = os.path.join(raw_ncar_folder_path, f"jhd.{str(timestep_nr).zfill(3)}.nc")
        cubes, _ = write_tools.prepare_data(file_path)
        cubes = flatten_3d_list(cubes)

        dests = write_tools.get_512_chunk_destinations(cls.dest_folder_name, cls.write_type, timestep_nr, cls.array_cube_side)

        for i in range(len(dests)):
            cls.queue.append((cubes[i], dests[i]))


    @parameterized.expand(queue)  # Use the parameterized decorator
    def test_verify_512_cube_data(self, original_512, zarr_512_path):
        zarr_512 = zarr.open_group(zarr_512_path, mode='r')
        print("Comparing original 512^3 with ", zarr_512_path)
        for var in original_512.data_vars:
            assert_eq(original_512[var].data, da.from_zarr(zarr_512[var]))
            print(var, " OK")