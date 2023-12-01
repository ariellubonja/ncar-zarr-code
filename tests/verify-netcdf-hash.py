import subprocess
import os
from concurrent.futures import ProcessPoolExecutor
import argparse


def get_sha256(filename):
    """
    Return the SHA-256 hash and filename in the format 'hash filename'.
    """
    raw_ncar_folder_path = get_ncar_folder_address()
    
    full_path = os.path.join(raw_ncar_folder_path, filename)
    # print("NCAR passed argument path: ", raw_ncar_folder_path)

    cmd = ["sha256sum", full_path]
    result = subprocess.run(cmd, stdout=subprocess.PIPE)
    # Only get the hash, and append the original filename
    hash_value = result.stdout.decode().strip().split()[0]
    return f"{hash_value}  {filename}"


def get_ncar_folder_address():
    args = parse_args()
    raw_ncar_folder_path = args.path
    return raw_ncar_folder_path


def check_file(entry):
    expected_hash, expected_filename = entry.split('  ', 1)
    # print("Parsed filename: ", expected_filename)
    computed_entry = get_sha256(expected_filename)

    if computed_entry == entry:
        return (expected_filename, "PASSED", None, None)
    else:
        return (expected_filename, "FAILED", entry, computed_entry)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Process specific files.')

    # Existing argument for file numbers or range
    parser.add_argument('integers', metavar='N', type=int, nargs='+', 
                        help='an integer for the file number or two integers for a range of files')

    # Optional argument for the file path
    parser.add_argument('-p', '--path', type=str, 
                        help='path to where the NCAR .netcdf files are located')

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
'e85f60ada6d0f17529798cdb5eb2d6e8dd482a3617efc19556785a60cbad44f5  jhd.049.nc',
'82f28dd04b62410a4e6bed9d2179b08267e6aeecbd9104335bed1de210cf132b  jhd.050.nc',
'556bac84c2d171cba836deafc4af421967c764a7d6972d2fffc1ae12c9cc475f  jhd.051.nc',
'56bd3d297260ac7c94df707f39c967efc1d87564ebeb75406453db1ed28b6894  jhd.052.nc',
'62a79a1e6ad28df90eaaea4305a324b1bc4d8bce0088471a7eaba814c38d19ad  jhd.053.nc',
'00a1f63eaa10df31e3b41ce870908c4bf385a8f1e18b5665b101330bb04b4725  jhd.054.nc',
'a2b36a3e5c430c21c849526f368321df7a29b1fb0327c0b0d66bbbc8ed0e9813  jhd.055.nc',
'b15826e79dfb319a1e9708a901fbedf9a6c32d05a4a148ab22b42eac49b51ab9  jhd.056.nc',
'16a4f208964292d0cf649fd6ac6e341f5ddab2030c1c4b9e5137df4ba7abeadb  jhd.057.nc',
'898abf3db72bf05cdd1551f9d848c5af91eb1f5de6066d7db7060996a681638d  jhd.058.nc',
'378ffe2aa77527a45c054fe4c2b15b919d005a9a39abac9cd37340ca61cf62b8  jhd.059.nc',
'650a24ffa8cba7a5797a78a41923989011c3acd3395d27d223f5b6f1c3e23f6b  jhd.060.nc',
'57286dca9cc72b01c2cfb354585af12185efe2457957af6cde4a0bdea0878703  jhd.061.nc',
'6f3c1179e66be68aa4a9f139fdf8cd83fc297dd6ec76e8eb1d6ce2f4aab7592b  jhd.062.nc',
'8fd432b736523e58598951354278b96e9ebc81ced5e3d734ef381b1ceba170d5  jhd.063.nc',
'b54a6b524a2ec879599a044f832f96d41f450267e1a683100d120d5d5629b689  jhd.064.nc',
'5185e823265e6a17a1e4d79e978149ded8f18e94bd62e86edb6a8803960932e7  jhd.065.nc',
'502ce99e3f129ca1d515dac0ee9b7f2ee5195bf301340f70473efc7c7bdd0901  jhd.066.nc',
'f8a105734441176956f4de759932216fc45c6906ab361313398ea8c8629d0c3d  jhd.067.nc',
'676f56da1b8c082f58f838505562959a0931988825df72730c3797cdd504f7a9  jhd.068.nc',
'23ef86b7e49a4e4ddac7379099c8d61167b3acc3ee82c25661a42fb28cf0ff64  jhd.069.nc',
'478d973bca0764f0de3c29b9e45e955560a534d73f5e50c55a7935294ce35b12  jhd.070.nc',
'2948776700d3c8d2ca17b8ee03057ce04568432536ee389f8096e8536b2082c6  jhd.071.nc',
'8518624943ae6f8b375120d3e7897a60a633f9bfc2e2e9f4142a615b44556e99  jhd.072.nc',
'17f5e2b48c2d9013c460710b983c95312115f529760e16db5afae40efcdac419  jhd.073.nc',
'734a644c3ec9c575908285083d01d093862d7161f14d8be2a3161ab8c41c9b38  jhd.074.nc',
'844ad0fb2afb6b8fd5e2f8f3b961031bcc6b0d788354c5677a35273fee5933b8  jhd.075.nc',
'a46c217dab3b9619cf8cbba2b8f56540ffe7c4d52ebec5f356ef31b2cd29b913  jhd.076.nc',
'a87ba56c070b9af369cc69ea153330162c9922c4e48c997335f5be197a57c36a  jhd.077.nc',
'3d2dfe52dbee6a26d1310e11739dad1f39ea7b1a28a22650e12c1991f959b93c  jhd.078.nc',
'5a9d9735c99c954f9a1c1871a85f8a8baa2d660cdb8828a5a7e9a1e32c4fea11  jhd.079.nc',
'a8410477527aecd23ded73098b01908b1931326c0565c39d7f64c243649ea283  jhd.080.nc',
'7d5aeebaec916eb60b4cbe78f294808a455da069cfcc5f3020ac427448fd3392  jhd.081.nc',
'602770398a71def644121643b4cea95fb356c71fbcb4757fb0d4988a24b73869  jhd.082.nc',
'9c8b999cd0a3c7078fe628adc267878bd5f366cbb4343d24764dd0f091eb5abe  jhd.083.nc',
'ddc3d2a08620ac2a96d6efb45a1c26b7ce0d450f10d040a4739c9b35b88262f8  jhd.084.nc',
'c3c533cd28ef32c7b2942d697dffd677086bc9066af997b7bd25c4d7f7a172b4  jhd.085.nc',
'a01178046ffb6306faa3be925d2307913c154c9e7762ad9d8e949dc8d03b4cbd  jhd.086.nc',
'1941f761344febee09989a9a82b93bc04a662ab734c7de421140a585fc1115d5  jhd.087.nc',
'06246ec2693e08d72f7b66f0f6a83ce63c50cb97fe10ad82a866d34b06130407  jhd.088.nc',
'51af6c9c1d0677b07940e0b263486ebb09a240c9d68ab5be807c7cddc436533d  jhd.089.nc',
'b15403798a1443c90f02606d0ce2eca5e51d7587a95af21adcf130d6d424c8a1  jhd.090.nc',
'ebad604362267da62df5da2fde6915f49b91b6ba728f6bc2ebd158d01bda005e  jhd.091.nc',
'8aa9305de89b7a50ed6661e6c056b85bad3bc99fa7dcf331899697a7b80fbddd  jhd.092.nc',
'0285200b9d44a5c51979c579feaf44bf662271c2ee4d4a142ce7619b44e13522  jhd.093.nc',
'bf5ebf4b7e76ded188845ac1aced025a1e8e126e324ecc0d954c5efe413cc2d9  jhd.094.nc',
'325acc57abe935caf5ea896323b3ac710144244b77137fcc4a43f1f59a14b5d7  jhd.095.nc',
'aa093c6d4225c802919313eae6c120a8666c58574c995391d4218082d7edb288  jhd.096.nc',
'8276e153d02ecbde058fe751c16b0731ef9ec028e186bf292cbd1b086f1f7468  jhd.097.nc',
'f926439343afd82e12939850c52e0f981c018566177d47ad19d7f821da9b7fd7  jhd.098.nc',
'8daf348f951559ac5de55a3c2daa08550e2bad657e24960b170004bfd2b938dc  jhd.099.nc',
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

