import unittest
import zarr
from dask.array.utils import assert_eq
import dask.array as da
import os
from parameterized import parameterized

from utils import write_tools
from utils.write_tools import flatten_3d_list

raw_ncar_folder_paths = [
    '/home/idies/workspace/turb/data02_02/ncar-high-rate-fixed-dt',
    '/home/idies/workspace/turb/data02_03/ncar-high-rate-fixed-dt',
]


def create_queue():
    """
    NetCDF data is sharded into smaller chunks, which are distributed round-robin.
    This method gathers these shards' paths and adds them to queue to compare their data with the original.
    """
    queue = []
    array_cube_side = 2048
    dest_folder_name = "sabl2048b"
    write_type = "prod"

    timestep_nr = int(os.environ.get('TIMESTEP_NR', 0))
    if timestep_nr < 50:
        raw_ncar_folder_path = raw_ncar_folder_paths[0]
    else:
        raw_ncar_folder_path = raw_ncar_folder_paths[1]

    file_path = os.path.join(raw_ncar_folder_path, f"jhd.{str(timestep_nr).zfill(3)}.nc")
    cubes, _ = write_tools.prepare_data(file_path)
    cubes = flatten_3d_list(cubes)

    dests = write_tools.get_512_chunk_destinations(dest_folder_name, write_type, timestep_nr, array_cube_side)

    for i in range(len(dests)):
        queue.append((cubes[i], dests[i]))

    return queue


class VerifyZarrDataCorrectness(unittest.TestCase):

    @parameterized.expand(create_queue())
    def test_verify_512_cube_data(self, original_512, zarr_512_path):
        zarr_512 = zarr.open_group(zarr_512_path, mode='r')
        print("Comparing original 512^3 with ", zarr_512_path)
        for var in original_512.data_vars:
            assert_eq(original_512[var].data, da.from_zarr(zarr_512[var]))
            print(var, " OK")

