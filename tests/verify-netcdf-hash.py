import subprocess
import os
from concurrent.futures import ProcessPoolExecutor
import argparse


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


def check_file(entry):
    expected_hash, expected_filename = entry.split('  ', 1)
    computed_entry = get_sha256(expected_filename)

    if computed_entry == entry:
        return (expected_filename, "PASSED", None, None)
    else:
        return (expected_filename, "FAILED", entry, computed_entry)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Process specific files.')
    parser.add_argument('integers', metavar='N', type=int, nargs='+', 
                        help='an integer for the file number or two integers for a range of files')
    return parser.parse_args()


def main():
    args = parse_args()
    if len(args.integers) == 1:
        file_indices = [args.integers[0]]
    elif len(args.integers) == 2:
        file_indices = list(range(args.integers[0], args.integers[1] + 1))
    else:
        print("Please provide either one file index or two indices for a range.")
        return
    
    # Hardcoded list of SHA-256 hashes with filenames
    hardcoded_hashes = [
'fe08d2182c2a504491cb7eafc3daff35fb43c9358553ec5c073ed71ae0d54a04  jhd.000.nc',
'59d67feaacb17510e1d63b78a1beefed4d9b17362ad28cc41c65ba0a8d88fbfe  jhd.001.nc',
'44dbc074b62bd606320e6bb4b8bea1c4b2f55ed35ae0525f3866eea371a5b301  jhd.002.nc',
'1a12fb27168b3054c68f8fb575cf22b6ae7277a08278e023951391cbbd9204cd  jhd.003.nc',
'a062dac74138d1a385c7599c39ee40dcd53ac6553d2480b2d99c2a1989446563  jhd.004.nc',
'162e409e9a1a4f4ec8289a2a89a7449fe0c75b0463ece6b61a044deef0f37890  jhd.005.nc',
'8eedab12fa6c50c58d6a628aa3175b88ed1ad731a4d5b2145fb221cee4098c6e  jhd.006.nc',
'c040abc183f779893f557cbea1f4fce2466b8c8a4ecb5f6d64eb04a3d3d72af3  jhd.007.nc',
'317f492a2674f50e31b1c671328924f30c9bb6919d8c7fe4601d67aab0645484  jhd.008.nc',
'76c90e01b41d79764d81e5d24b200793a00f687394ba6e933f48763150c33c11  jhd.009.nc',
'87a579525c739b1a256b9fd826fa3970c99d412162f57e13c1fc15eea4de9c7f  jhd.010.nc',
'88f326be71ee3e0d124aae1b40f8d9a33c7ebf486e240afec3335e548dd45a5d  jhd.011.nc',
'd90d7f2220045b3004b5bed9904754445af1d684b0c9e97295767da19b3f18c6  jhd.012.nc',
'60fddce3227898b9b8231803b4b223a3c84353a33a29d021f7377522707a38a3  jhd.013.nc',
'95c58692b78878910e16f9388008844e15f8adce556ea304c16959823a0388ad  jhd.014.nc',
'fce2d9425d761506a2f619988bd6c83fd1aaa8def61fc1fd067e65da01df5bdb  jhd.015.nc',
'f2480a7c6005d8283538ebaed5e65b0e1751a4fc97d96998d50acddee61bae78  jhd.016.nc',
'17c4e1c1688eb2445e4315a79d6754d1b026111727ddba5e1fd7ac3e8a2634fd  jhd.017.nc',
'd704dfac9bf35b07178462cea0b5414253cb8b3ffa1a98809a90902de2abec22  jhd.018.nc',
'8aae9cd03e3bcb5ffb4f1481a8a69538648cd5efb4f3e68d04f3139bf754fb87  jhd.019.nc',
'01b53e96916b526a74b255c489a6a9a394ef4f903b4845f022ff9d768ed9f657  jhd.020.nc',
'f1756f4caecf53ffd85e3609b0cf4c11916108d9f5d30fdb594306033600457a  jhd.021.nc',
'0846e9caa4b2fc4d3a1fc573c6e31c4ba16b1242cb19028300b2c7b5ce30bb7a  jhd.022.nc',
'20485e555109a540a6c6e31d65eb6a298c1bf1f410e4d19a12f85a8ac0da9f4f  jhd.023.nc',
'125ca444579517259e137b769dc6278ca89b19ef90173d58fb528b9f04ce7e5c  jhd.024.nc',
'33b1a6709052093bf3925e29dcd8af86c25f223cc1a4493d7b1aa95636d53809  jhd.025.nc',
'db4106d904ebea26c8c73c382f42a28ae098cec2194f4abb434977abd1242561  jhd.026.nc',
'107e4bca19c293c405245614e31415d96a90ddb701ae91204630b6a59fcad60e  jhd.027.nc',
'9fdb419f3fcf6148086fb9538477a8c55f66b0845754a528f01b3eede92c29e0  jhd.028.nc',
'b4fc75e496730c968b7d13e23b15831f2fe653bdb77c1068cfc3e17b8b1759b0  jhd.029.nc',
'5a3e40dfe8fe409cde610eed70f1f215da066500294ce5c473c32b90ba4588d8  jhd.030.nc',
'af43f79eb4d39c3a22e2eb2ed77140f48b615351f6ce21f2ce6ed9229bddaaf5  jhd.031.nc',
'd05a9fc6c11f5fa1c2db6e0a3b688043ab074902d9a7b0c74d9fb1d1ff03adde  jhd.032.nc',
'9b48a84b352b63cced2c77073b40ab8993b215aeff6e8036125cf1fa4a34946c  jhd.033.nc',
'e4990c8f17537a1beda1b0fccd38d891c67c9713057d65a1bb49cef6cd2be84f  jhd.034.nc',
'e3d914bd3f4334c4fd6ce4034c3ef49cfd620bdfa6b25021a596da4ed8c4c59d  jhd.035.nc',
'4cc94e6193cbafb899ca721b1b0e6045e6a6b13b3b7428d917be77fd1e497e3f  jhd.036.nc',
'c0a477f615c4d2535546cc3f83c73e3ff4112aee7c52af098f57dc57442b9289  jhd.037.nc',
'c8bdd9734a495061393409850a341b2fd21561a00f9a3eb0b281340135f335fa  jhd.038.nc',
'c237aea94a9dc91b8d6bb60b4a0ece1f9ed6c39272155846ac047246096c73ad  jhd.039.nc',
'926390cbacde19ce8f57a23f08e914abedaf67bc1a1d40c0a05b31d21e5bc6fa  jhd.040.nc',
'804f3ec0ce23c5ec4b0e6e7cf4c23e39592d03b4ae0a576dbc33a8d2b79be3ec  jhd.041.nc',
'c5f7ef852c544c5bc87243ed99a80ffb0a0b9dc5ac530d666fb58cf8d65b68c7  jhd.042.nc',
'd2456278df96fb9ae65195aedafc6cd3bedb9e13ff6158a8f792d22027516c0c  jhd.043.nc',
'041d57256041ea000b00670a8ba5ef8c6ae8575e7fa3e657a4a18617f7abbe8d  jhd.044.nc',
'b9e737dc74fad239fb0f856115ab01bba9b1a080d148e3bb3365e730626bdbc8  jhd.045.nc',
'1427c5900a3736aeee43532db8e635f31c09fdf287927c08ae1a974c02b74527  jhd.046.nc',
'120cef29ad8ffb09fbf4e763d0f5c6fb80027faa8181d9f94d9533d7592160e8  jhd.047.nc',
'b8ab895a9e82e80e387cb9ea11bcd5978763557c4316c0a67c8d2c61a950cd6a  jhd.048.nc',
'e85f60ada6d0f17529798cdb5eb2d6e8dd482a3617efc19556785a60cbad44f5  jhd.049.nc'
    ]

    selected_hashes = [hardcoded_hashes[i] for i in file_indices if i < len(hardcoded_hashes)]

    with ProcessPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(check_file, selected_hashes))


    for filename, status, expected, computed in results:
        if status == "PASSED":
            print(f"{filename}: PASSED")
        else:
            print(f"{filename}: FAILED (Expected: {expected}, Got: {computed})")



if __name__ == "__main__":
    main()

