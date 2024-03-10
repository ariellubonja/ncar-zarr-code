"""
Ariel Lubonja
2024-03-10

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
with open('/Users/ariellubonja/prog/zarrify-across-network/tests/config.yaml', 'r') as file:
    config = yaml.safe_load(file)
dataset_name = os.environ.get('DATASET', 'NCAR-High-Rate-1')


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
    global config, dataset_name

    test_params = []
    dataset_config = config['datasets'][dataset_name]
    write_config = config['write_settings']
    dataset = NCAR_Dataset(
        name=dataset_config['name'],
        location_path=dataset_config['location_path'],
        desired_zarr_chunk_size=write_config['desired_zarr_chunk_length'],
        desired_zarr_array_length=write_config['desired_zarr_array_length'],
        prod_or_backup='prod',
        start_timestep=dataset_config['start_timestep'],
        end_timestep=dataset_config['end_timestep']
    )

    all_expected_hashes = []
    # Iterate over each data path and load the corresponding hash entries
    # for data_path in dataset.location_path:
    hash_file_path = os.path.join(dataset.location_path, 'hash.txt')
    if os.path.exists(hash_file_path):
        with open(hash_file_path, 'r') as hash_file:
            for line in hash_file:
                temp_line = line.split('  ')
                temp_line[1] = os.path.join(dataset.location_path, temp_line[1].replace('\n', ''))
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

