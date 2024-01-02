import unittest
import subprocess
import os
from parameterized import parameterized
import yaml


def get_sha256(full_file_path):
    """
    Return the SHA-256 hash and filename in the format 'hash filename'.
    """
    cmd = ["sha256sum", full_file_path]
    result = subprocess.run(cmd, stdout=subprocess.PIPE)
    hash_value = result.stdout.decode().strip().split()[0]
    return hash_value


def load_all_expected_hashes():
    """
    Load all expected hashes from 'hash.txt' files in the directories specified in 'config.yaml'.
    """
    # Load YAML configuration
    with open('tests/config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    all_expected_hashes = []
    # Iterate over each data path and load the corresponding hash entries
    for data_path in config['original_data_paths']:
        hash_file_path = os.path.join(data_path, 'hash.txt')
        if os.path.exists(hash_file_path):
            with open(hash_file_path, 'r') as hash_file:
                for line in hash_file:
                    temp_line = line.split('  ')
                    temp_line[1] = os.path.join(data_path, temp_line[1].replace('\n', ''))
                    all_expected_hashes.append(tuple(temp_line))
        else:
            raise FileNotFoundError(f"hash.txt file expected but not found at {hash_file_path}")

    return all_expected_hashes


class TestFileHashes(unittest.TestCase):
    @parameterized.expand(load_all_expected_hashes())
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

