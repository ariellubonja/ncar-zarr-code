import unittest
import subprocess
import os
from parameterized import parameterized
import yaml


def get_sha256(raw_ncar_folder_path, filename):
    """
    Return the SHA-256 hash and filename in the format 'hash filename'.
    """
    full_path = os.path.join(raw_ncar_folder_path, filename)
    cmd = ["sha256sum", full_path]
    result = subprocess.run(cmd, stdout=subprocess.PIPE)
    hash_value = result.stdout.decode().strip().split()[0]
    return f"{hash_value}  {filename}"


def load_all_expected_hashes():
    """
    Load all expected hashes from 'hash.txt' files in the directories specified in 'config.yaml'.
    """
    # Load YAML configuration
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    all_expected_hashes = []
    # Iterate over each data path and load the corresponding hash entries
    for data_path in config['data_paths']:
        hash_file_path = os.path.join(data_path, 'hash.txt')
        if os.path.exists(hash_file_path):
            with open(hash_file_path, 'r') as hash_file:
                for line in hash_file:
                    all_expected_hashes.append((line.strip(), data_path))
        else:
            raise FileNotFoundError(f"hash.txt file expected but not found at {hash_file_path}")

    return all_expected_hashes


class TestFileHashes(unittest.TestCase):
    @parameterized.expand(load_all_expected_hashes())
    def test_file_hash(self, hash_entry):
        expected_hash_entry, data_path = hash_entry
        expected_hash, expected_filename = expected_hash_entry.split('  ', 1)
        computed_entry = get_sha256(data_path, expected_filename)
        self.assertEqual(computed_entry, expected_hash_entry,
                         f"Hash mismatch for {expected_filename}")
