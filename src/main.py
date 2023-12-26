# Author: Ariel Lubonja <ariellubonja@live.com>
# Date: 26-Dec-2023

from dataset import Dataset
import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--timestep', type=int, required=True)
    parser.add_argument('-p', '--path', type=str,
                    help='path to where the NCAR .netcdf files are located', required=True)
    parser.add_argument('-zc', '--zarr_chunk_size', type=int,
                        help='Zarr chunk size (int)', default=64)
    parser.add_argument('--desired_cube_side', type=int, default=512, help='The desired side length of the 3D data cube')
    parser.add_argument('--zarr_encoding', type=bool, default=True,
                        help='kwargs, including Compression to be passed to xarray.to_zarr(). Not implemented yet, '
                             'please edit main.py')
    parser.add_argument('--distribution', type=str, choices=['prod', 'back'], default='prod', help='Whether distribution should be "prod" for production or "back" for backup')


    # TODO Do some checking
    args = parser.parse_args()
    TIMESTEP = args.timestep
    LOCATION_PATH = args.path
    ZARR_CHUNK_SIDE = args.zarr_chunk_size
    desired_cube_side = args.desired_cube_side
    PROD_OR_BACKUP = args.distribution

    ENCODING = {
        "velocity": dict(chunks=(ZARR_CHUNK_SIDE, ZARR_CHUNK_SIDE, ZARR_CHUNK_SIDE, 3), compressor=None),
        "pressure": dict(chunks=(ZARR_CHUNK_SIDE, ZARR_CHUNK_SIDE, ZARR_CHUNK_SIDE, 1), compressor=None),
        "temperature": dict(chunks=(ZARR_CHUNK_SIDE, ZARR_CHUNK_SIDE, ZARR_CHUNK_SIDE, 1), compressor=None),
        "energy": dict(chunks=(ZARR_CHUNK_SIDE, ZARR_CHUNK_SIDE, ZARR_CHUNK_SIDE, 1), compressor=None)
    }

    ncar_dataset = Dataset(name='sabl2048b',
                           location_path=LOCATION_PATH,
                           zarr_chunk_size=ZARR_CHUNK_SIDE,
                           desired_cube_side=desired_cube_side,
                           encoding=ENCODING,
                           timestep=TIMESTEP)

    lazy_zarr_cubes = ncar_dataset.transform_to_zarr()
    ncar_dataset.distribute_to_filedb(lazy_zarr_cubes, PROD_OR_BACKUP)
