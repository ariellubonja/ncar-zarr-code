import subprocess
import os
from concurrent.futures import ProcessPoolExecutor


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


def main():
    # Hardcoded list of SHA-256 hashes with filenames
    hardcoded_hashes = [
        'c29de1bc2538b1ce019b8d239d3b427bfbae60f581dde879b533d523e23a671f  jhd.000.nc',
        'effdea07f11b39a76540c5b4fd831fe8c210d6296319aa744a263cfef20d7d20  jhd.001.nc',
        '3c94e889142653f9f0dfa089a839c1fa867d9086317e523633fb210c7a0a4ba7  jhd.002.nc',
        '76b53cc31d3ecaff8fd392df23a124974b890da0517ded19295e37c63a2a04fa  jhd.003.nc',
        '7e27327860364369762f2653763c73055615f217d76045f21c3b6e7d04ab228c  jhd.004.nc',
        '1a4cc54b47af25caea43320de18eed9ad2be0df69939be5a0504d5ae824d7189  jhd.005.nc',
        '5f6b4555e46c8613481f3d807717adc9943c63ca1c5b158326fb5846edc2295b  jhd.006.nc',
        '4222febd48a43044c74fc0cc93597d562d6b24088a44b53f344e61eebbe9d854  jhd.007.nc',
        'e3e941cf4fb6ba6fc66a549f1be19fe5f4f06fc0edcdfb437384588f43efb46f  jhd.008.nc',
        'a17e8616b2dc5ac30e48943c772334e93a13b5f900a52131f9997d4e2df38083  jhd.009.nc',
        'defd2e4181acd1912db662c21b4755305db1e67cf040d781fd2dca217f9e2e9e  jhd.010.nc',
        '8cb58de726a9fba661a2b632cfba6acb1dc83c30d801721e36a2f8106d22fa19  jhd.011.nc',
        'c18742a6022ec53e6e5ead4f1b20d018e192169dfe478605095b37d39888fbee  jhd.012.nc',
        'b5cbfe8b38d30b1250cd1ad330a8c0d3e3b129661e003dceea5629c32c7fb142  jhd.013.nc',
        '1a8d0c195cd1ed8dd288eb6b7916e865adec81db2cf20a7654fe888b76368fe3  jhd.014.nc',
        '1978f6d2c884a5aeff084b93eab8f98a75e3cbc0d1f38f9a4cdd3afcd83fba0e  jhd.015.nc',
        '1bb4c10e9e47d9d069e4c8ad2756b3c47460397410c5067e50d8a77fa25417ed  jhd.016.nc',
        'a6430c2f8263e8bf923c3cdb060262352598da06601582957fd2bc048bcae8e7  jhd.017.nc',
        'e23f15109f897dd0beebe93fec45005d18d938bcb16ccbec7da1de0b915187c9  jhd.018.nc',
        '1fccb0f303ef38bddff0b6cf2cd1dbe94a2c562f25bff614eff8f5a264b40d13  jhd.019.nc',
        'b889455d0ae4a77afc36b4198b7ac31bbe2285e39b7d38cabad73d63e237bb6f  jhd.020.nc',
        '03eaf1a3e3362de37c298cd2ffcf93d0813e0889365506b0825a55e16c755f74  jhd.021.nc',
        'e09de12c3c242f4f0fa2425b572a320dcbca56ea4e17631ae913d29dcace30fd  jhd.022.nc',
        'd39e0d5d45a3441635e0094166aa07ac004d06a608d94aacbed34f803991d2fb  jhd.023.nc',
        '2c83a2a1032fe07d029c963d46788b7d1cabe4b7c88aa2a7ce9eb97065e2b17c  jhd.024.nc',
        'd770b625889427cef56cc9b67686f26e6b5c71d025612e05f158925a00dedd8e  jhd.025.nc',
        'a92ca30bca4458cd2724bcaa32ce6ea877eccabe0c88bb74a19265b308e84a97  jhd.026.nc',
        '3f58f96c4c6611b2e866ac5295974d93d5a0ef46869cd7a0e88179a2b8a8bee6  jhd.027.nc',
        '45d0cc3d347fbf1419473ef5fb8fb63ef8b94ed8c55ab7fba00f7d935ba2d19f  jhd.028.nc',
        '3bfd5308b503ff50e3fa09872dd46096162341c72fcf55af604c175652fd4318  jhd.029.nc',
        'cdb0df86c1217eda8c78948dbbd0b624577272324635f7964f844d32d741db33  jhd.030.nc',
        '6404bbaa1821a11648d624d8767f256000ab0403200d15ebcc4257ec06d0af80  jhd.031.nc',
        'a38aaf89ef1cb44695c0a4531f599797c2048a5ff814d30f854ceac2280b4070  jhd.032.nc',
        'e28d2f80c8b1683715649127672692f6f8d015279f9ccec9546fc69e6c3fefe2  jhd.033.nc',
        'c9a561da463b369e19223444d0f7b8269c1f3d5d7d159eb519b41b602a15f533  jhd.034.nc',
        '9f14522922a33d0ee5da40a3888d931b7138c7daf7d6dd3b53820057a3b97dd3  jhd.035.nc',
        '1f2265533551e6000be316a8b0c7e07895ab50452dd955b9b21277929caad3fc  jhd.036.nc',
        'b21be59124e3526e4a0e440d91e25758084ef5357aa96d09f9abad8f5f2a461e  jhd.037.nc',
        '479256f3d536400682a8f482154585c59e0d3ec0ec75f83a901d45e8b243be29  jhd.038.nc',
        '3dc006556c3189fb30c328ac4feb43ec3649d4b6b23be6dd97e3ceffab737b4b  jhd.039.nc',
        'fa77f64243889f2716b9aa9ce983ca8c6bb72c8f43fae3fc5249171d2118842a  jhd.040.nc',
        '71d47ef47dc0d39742ee02399547869636810445c0a5fc11157a23db71843c79  jhd.041.nc',
        '9cca34736fdee66423ba21d5fdac8ee6ea45efcf69c18e1791825006386d3e39  jhd.042.nc',
        '8a53bc03aed340588e3d2821388886405bfae8f771cb4c99f47c1d8979d4be56  jhd.043.nc',
        'de221711bb3b85f44db862d6cd0f842f61ba36e8b3064906527eeede36e4d22a  jhd.044.nc',
    ]

    with ProcessPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(check_file, hardcoded_hashes))

    for filename, status, expected, computed in results:
        if status == "PASSED":
            print(f"{filename}: PASSED")
        else:
            print(f"{filename}: FAILED (Expected: {expected}, Got: {computed})")



if __name__ == "__main__":
    main()

