#!/bin/bash
# Ariel Apr 2024
# ChatGPT original template
# Using bash because Python directory operations are slower

BASE_DIR="/Volumes/backup-hdd/ncar"

# filedb07_02 and 09_02 are at capacity and therefore not included in the backup
declare -a directories=(
    "data01_01" "data02_01" "data03_01" "data04_01" "data05_01" "data06_01" "data07_01" "data08_01" "data09_01" "data10_01" "data11_01" "data12_01"
    "data01_02" "data02_02" "data03_02" "data04_02" "data05_02" "data06_02" "data08_02" "data10_02" "data11_02" "data12_02"
    "data01_03" "data02_03" "data03_03" "data04_03" "data05_03" "data06_03" "data07_03" "data08_03" "data09_03" "data10_03" "data11_03" "data12_03"
)

# Get the number of directories (excluding the two full ones)
num_directories=34

# Loop through each directory
for (( i=0; i<num_directories; i++ )); do
    # Determine the source and target directories
    source_index=$i
    target_index=$(( (i + 1) % num_directories ))

    source_dir="${directories[$source_index]}"
    target_dir="${directories[$target_index]}"

    # Directory paths
    source_path="$BASE_DIR/$source_dir/zarr"
    target_path="$BASE_DIR/$target_dir/zarr"

    # Copy files from source to target
    # Assuming the structure and naming are consistent and all files need to be copied
    echo "Copying from $source_path to $target_path..."
    cp -r "$source_path"/* "$target_path"/
done

echo "Copy operation completed."
