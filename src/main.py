# Author: Ariel Lubonja <ariellubonja@live.com>
# Date: 26-Dec-2023

from src.dataset import NCAR_Dataset
import argparse
import yaml

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('-n', '--name', type=str,
                        choices=['sabl2048a', 'sabl2048b'],  # Update this as more datasets are added
                        help='Name of the dataset e.g. sabl2048b. Folders are created using this', required=True)
    parser.add_argument('-p', '--paths', type=list, nargs='+',
                        help='path to where each dataset file is located. Please specify 1 file and not whole '
                             'directories. If no path provided, the default path'
                             'from config.yaml is used. Deprecated. Modify config.yaml instead.',
                        required=False)

    parser.add_argument('--distribution', type=str, choices=['prod', 'back'], required=True,
                        help='Whether distribution should be "prod" for production or "back" for backup')
    parser.add_argument('-zc', '--zarr_chunk_size', type=int,
                        help='Zarr chunk size (int)', default=64)
    parser.add_argument('--desired_cube_side', type=int, default=512,
                        help='The desired side length of the 3D data cube')
    parser.add_argument('-st', '--start_timestep', type=int, required=True,
                        help='Timestep to start processing from. Due to SciServer job time limitations, not all '
                             'timesteps can be processed at once.')
    parser.add_argument('-et', '--end_timestep', type=int, required=True,
                        help='Timestep to end processing at (inclusive). See -st for more info')

    # TODO Do some checking
    args = parser.parse_args()
    DATASET_NAME = args.name
    LOCATION_PATHS = args.paths
    ZARR_CHUNK_SIDE = args.zarr_chunk_size
    desired_cube_side = args.desired_cube_side
    PROD_OR_BACKUP = args.distribution
    start_timestep = args.start_timestep
    end_timestep = args.end_timestep

    with open('/Users/ariellubonja/prog/zarrify-across-network/config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    if LOCATION_PATHS is None:
        if DATASET_NAME not in config['datasets']:
            raise ValueError("DATASET_NAME not found in config.yaml")
        LOCATION_PATHS = config['datasets'][DATASET_NAME]['location_paths']

    ncar_dataset = NCAR_Dataset(name=DATASET_NAME,
                                location_path=LOCATION_PATHS,
                                desired_zarr_chunk_size=ZARR_CHUNK_SIDE,
                                desired_zarr_array_length=desired_cube_side,
                                prod_or_backup=PROD_OR_BACKUP,
                                start_timestep=start_timestep,
                                end_timestep=end_timestep)

    if PROD_OR_BACKUP == 'prod':
        ncar_dataset.distribute_to_filedb()
    else:
        ncar_dataset.create_backup_copy()
