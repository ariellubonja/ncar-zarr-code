import subprocess
import os

raw_ncar_folder_path = '/home/idies/workspace/turb/data02_02/ncar-high-rate-fixed-dt'


def get_sha256(filename):
    """
    Return the SHA-256 hash and filename in the format 'hash filename'.
    """
    full_path = os.path.join(raw_ncar_folder_path, filename)
    cmd = ["sha256sum", full_path]
    result = subprocess.run(cmd, stdout=subprocess.PIPE)
    # Only get the hash, and append the original filename
    hash_value = result.stdout.decode().strip().split()[0]
    return f"{hash_value}  {filename}"


def main():
    # Hardcoded list of SHA-256 hashes with filenames
    hardcoded_hashes = [
        'c29de1bc2538b1ce019b8d239d3b427bfbae60f581dde879b533d523e23a671f  jhd.000.nc',
        'effdea07f11b39a76540c5b4fd831fe8c210d6296319aa744a263cfef20d7d20  jhd.001.nc',
        '3c94e889142653f9f0dfa089a839c1fa867d9086317e523633fb210c7a0a4ba7  jhd.002.nc',
        '76b53cc31d3ecaff8fd392df23a124974b890da0517ded19295e37c63a2a04fa  jhd.003.nc',
        '7e27327860364369762f2653763c73055615f217d76045f21c3b6e7d04ab228c  jhd.004.nc',
    ]

    for entry in hardcoded_hashes:
        expected_hash, expected_filename = entry.split('  ', 1)
        computed_entry = get_sha256(expected_filename)

        if computed_entry == entry:
            print(f"{expected_filename}: PASSED")
        else:
            print(f"{expected_filename}: FAILED (Expected: {entry}, Got: {computed_entry})")


if __name__ == "__main__":
    main()

