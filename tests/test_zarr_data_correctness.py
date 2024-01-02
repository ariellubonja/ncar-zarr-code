import unittest
import zarr
from dask.array.utils import assert_eq
import dask.array as da
from parameterized import parameterized

from src.utils.write_utils import get_sharding_queue

# TODO Read path from config file

class VerifyZarrDataCorrectness(unittest.TestCase):

    @parameterized.expand(get_sharding_queue())
    def test_verify_512_cube_data(self, original_512, zarr_512_path):
        zarr_512 = zarr.open_group(zarr_512_path, mode='r')
        print("Comparing original 512^3 with ", zarr_512_path)
        for var in original_512.data_vars:
            assert_eq(original_512[var].data, da.from_zarr(zarr_512[var]))
            print(var, " OK")

