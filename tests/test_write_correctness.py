import pytest
import zarr
from dask.array.utils import assert_eq
import dask.array as da
import os
import sys

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from utils import write_tools
from utils.write_tools import flatten_3d_list

array_cube_side = 2048
raw_ncar_folder_path_02 = '/home/idies/workspace/turb/data02_02/ncar-high-rate-fixed-dt'
raw_ncar_folder_path_03 = '/home/idies/workspace/turb/data02_03/ncar-high-rate-fixed-dt'
# raw_ncar_folder_path = '/sciserver/filedb02-03/turb/ncar-high-rate-fixed-dt/'
dest_folder_name = "sabl2048b"  # B is the high-rate data
write_type = "prod"  # or "back" for backup

def setup_data(timestep_nr):
    cubes, _ = write_tools.prepare_data(raw_ncar_folder_path + "/jhd." + str(timestep_nr).zfill(3) + ".nc")
    cubes = flatten_3d_list(cubes)
    dests = write_tools.get_512_chunk_destinations(dest_folder_name, write_type, timestep_nr, array_cube_side)
    return [(cubes[i], dests[i]) for i in range(len(dests))]

def verify_512_cube_data(original_512, zarr_512_path):
    zarr_512 = zarr.open_group(zarr_512_path, mode='r')
    for var in original_512.data_vars:
        assert_eq(original_512[var].data, da.from_zarr(zarr_512[var]))

def verify_512_cube_dimensions(zarr_512_path):
    zarr_512 = zarr.open_group(zarr_512_path, mode='r')
    for var in zarr_512.array_keys():
        expected_shape = (512, 512, 512, 3) if var == "velocity" else (512, 512, 512, 1)
        assert zarr_512[var].shape == expected_shape

def verify_512_cube_chunk_sizes(zarr_512_path):
    zarr_512 = zarr.open_group(zarr_512_path, mode='r')
    for var in zarr_512.array_keys():
        expected_chunksize = (64, 64, 64, 3) if var == "velocity" else (64, 64, 64, 1)
        assert zarr_512[var].chunks == expected_chunksize

def verify_512_cube_compression(zarr_512_path):
    zarr_512 = zarr.open_group(zarr_512_path, mode='r')
    for var in zarr_512.array_keys():
        assert zarr_512[var].compressor is None

@pytest.fixture(scope="module")
def data_queue(request):
    timestep_nr = request.config.getoption("--timestep")
    return setup_data(timestep_nr)

class TestVerifyWrite:

    @pytest.fixture(autouse=True)
    def setup(self, request):
        timestep_nr = request.config.getoption("--timestep")
        self.data_queue = setup_data(timestep_nr)

    def test_data_comparison(self):
        for original_512, zarr_512_path in self.data_queue:
            verify_512_cube_data(original_512, zarr_512_path)

    def test_dimensions(self):
        for _, zarr_512_path in self.data_queue:
            verify_512_cube_dimensions(zarr_512_path)

    def test_chunk_sizes(self):
        for _, zarr_512_path in self.data_queue:
            verify_512_cube_chunk_sizes(zarr_512_path)

    def test_compression(self):
        for _, zarr_512_path in self.data_queue:
            verify_512_cube_compression(zarr_512_path)

# Helper function to set up data
def setup_data(timestep_nr):
    if timestep_nr < 49:
        raw_ncar_folder_path = raw_ncar_folder_path_02
    else:
        raw_ncar_folder_path = raw_ncar_folder_path_03

    cubes, _ = write_tools.prepare_data(raw_ncar_folder_path + "/jhd." + str(timestep_nr).zfill(3) + ".nc")
    cubes = flatten_3d_list(cubes)
    dests = write_tools.get_512_chunk_destinations(dest_folder_name, write_type, timestep_nr, array_cube_side)
    return [(cubes[i], dests[i]) for i in range(len(dests))]