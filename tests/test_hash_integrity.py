"""
Ariel Lubonja
2024-01-10

Check the integrity of the original NCAR data by comparing the
SHA-256 hash of the files with the expected hash contained in hash.txt
Tests whole folders at a time. Only need to specify DATASET env var.
"""

import unittest
import subprocess
import os
from parameterized import parameterized
import yaml

from src.dataset import NCAR_Dataset


config = {}
with open('config.yaml', 'r') as file:
# with open('/Users/ariellubonja/prog/zarrify-across-network/config.yaml', 'r') as file:
    config = yaml.safe_load(file)
dataset_name = os.environ.get('DATASET')
start_timestep = int(os.environ.get('START_TIMESTEP', -1))
end_timestep = int(os.environ.get('END_TIMESTEP', -1))


def get_sha256(full_file_path):
    """
    Return the SHA-256 hash and filename in the format 'hash filename'.
    """
    cmd = ["sha256sum", full_file_path]
    result = subprocess.run(cmd, stdout=subprocess.PIPE)
    hash_value = result.stdout.decode().strip().split()[0]
    return hash_value


def generate_hash_tests():
    """
    Load all expected hashes from 'hash.txt' files in the directories specified in 'config.yaml'.
    """
    # Load YAML configuration
    global config, dataset_name, start_timestep, end_timestep

    dataset_config = config['datasets'][dataset_name]

    if start_timestep == -1 or end_timestep == -1:
        start_timestep = dataset_config['start_timestep']
        end_timestep = dataset_config['end_timestep']

    write_config = config['write_settings']

    # config['datasets'].keys() # dataset_names

    dataset = NCAR_Dataset(
        name=dataset_name,
        location_paths=dataset_config['location_paths'],
        desired_zarr_chunk_size=write_config['desired_zarr_chunk_length'],
        desired_zarr_array_length=write_config['desired_zarr_array_length'],
        write_mode='prod',
        start_timestep=start_timestep,
        end_timestep=end_timestep
    )

    dataset_path = None
    if dataset_name == "sabl2048b": # high rate data is split into 2 folders
        if start_timestep < 50 and end_timestep < 50:
            dataset_path = dataset.location_paths[0]  # First 50 timesteps are in 1 folder
        elif start_timestep >= 50 and end_timestep > 50:
            dataset_path = dataset.location_paths[1]
        else:
            raise Exception("Please run hash tests for sabl2048b separately on timesteps 0-49 and 50-104, since they're stored on different folders")
    elif dataset_name == "sabl2048a":
        dataset_path = dataset.location_paths[0]  # Only 1 folder, not a list

    all_expected_hashes = []
    # Iterate over each data path and load the corresponding hash entries
    # for data_path in dataset.location_path:
    hash_file_path = os.path.join(dataset_path, 'hash.txt')
    if os.path.exists(hash_file_path):
        with open(hash_file_path, 'r') as hash_file:  # Make sure file isn't too big
            lines = hash_file.readlines()  # Read all lines into a list
            selected_lines = lines[start_timestep:end_timestep + 1]  # Slice the list for the desired range

            all_expected_hashes = []
            for line in selected_lines:
                temp_line = line.split('  ')
                temp_line[1] = os.path.join(dataset_path, temp_line[1].replace('\n', ''))
                all_expected_hashes.append(tuple(temp_line))
    else:
        raise FileNotFoundError(f"hash.txt file expected but not found at {hash_file_path}")

    return all_expected_hashes


class TestFileHashes(unittest.TestCase):
    @parameterized.expand(generate_hash_tests())
    def test_file_hash(self, true_hash, full_file_path):
        """
        Test that the hash of the file at full_file_path matches the expected hash, passed as argument to this function.
        True hash is read from hash.txt in each directory where the data is stored.
        Args:
            true_hash: str
            full_file_path: str
        """

        computed_hash = get_sha256(full_file_path)
        self.assertEqual(computed_hash, true_hash,
                         f"Hash mismatch for {full_file_path}")

