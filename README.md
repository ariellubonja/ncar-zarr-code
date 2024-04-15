# Speed Up Scientific Data Analysis with Efficient Data Storage

**Welcome to our data management tool!** This tool is crafted to help you manage and analyze large scientific datasets more effectively. Using this, you can convert your datasets into a format called Zarr and then distribute them across multiple storage disks. This helps in speeding up the process of reading data, making it a great fit for scientific research in areas like climate studies and astronomy.

### How Does It Work?
- **Converts to [Zarr](https://zarr.readthedocs.io/en/stable/) Format**: The tool changes your datasets into Zarr format, known for its efficiency and versatility.
- **Distributes Data Across Disks**: It spreads out the data across different disks, which allows for faster parallel access to the data.
- **Customizable**: Originally designed for [Johns Hopkins' FileDB Database](https://turbulence.pha.jhu.edu), this tool can be easily adapted for different storage setups.

### Key Features
- **Balanced Disk Usage**: It writes data in a round-robin manner to balance the load on each disk.
- **Smart Data Separation**: Ensures neighboring data chunks are not on the same disk by solving the [Map Coloring](https://en.wikipedia.org/wiki/Four_color_theorem) problem, maximizing parallel read efficiency.
- **Optimized Data Order**: Uses Morton/z-ordering to optimize the way data is lined up and accessed.
- **Supports Zarr Encoding and Compression**: Enhances data storage efficiency.

- **Dual Copy Writing**: Creates both primary and backup copies of data for added safety.


### Easy Installation
Get started quickly: git clone https://github.com/ariellubonja/zarrify-across-network.

### Adapting the Script to Distribute Your Own Files

- **Changing Input Paths**: Input data folder is specified using the `--path` argument in `main.py`. The path should contain Xarray-compatible data

#### Changing Output Paths for Distributed Files

1. Have single Xarray-compatible files which contain your large Scientific data
1. Have multiple hard disks on which you want to distribute. These should be in a list of paths (i.e. list of strings)
2. `src/utils/write_utils.py:list_fileDB_folders()` should be changed to return your disk paths
3. `src/dataset.py:get_zarr_array_destinations()` determines the destination paths for the Zarr files in an ordered list. Change this to match your desired directory structure

#### Changing the Disk Node Assignment Schema

This code is located in `src/utils/write_utils.py:node_assignment()`

The function takes in the number of nodes and the number of batches and returns a list of lists, where each sublist contains the node assignments for a batch. The current implementation uses the [Node/Map Coloring algorithm](https://en.wikipedia.org/wiki/Graph_coloring#Node_coloring) to assign nodes to batches. You can modify this function to implement your own node assignment schema.


### Writing New Data to FileDB

Run `main.py` as follows to distribute the data across the FileDB nodes:


```
cd /home/idies/workspace/Storage/ariel4/persistent/zarrify-across-network

../zarr-py3.11/bin/python -m src.main --write_mode prod -n sabl2048b -st 48 -et 49
```


Command-Line Arguments:


[//]: # (- --timestep: The timestep number for the NCAR data &#40;required&#41;.)
- -p or --path: Path to the location of the data file (required). Specify individual filenames for each timestep, not the directory.
- -zc or --zarr_chunk_size: Zarr chunk size. Defaults to 64.
- --desired_cube_side: Desired side length of the 3D data cube. Defaults to 512.
- --write_mode: Type of writes - "prod" for production or "back" for backup, "delete_back" to delete backups.
[//]: # (- --zarr_encoding: Boolean flag to enable custom Zarr encoding. Currently not implemented. Defaults to True.)

##### A Few Things to Note

- Input must be [Xarray-compatible](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html)

- Use SciServer Large Job for writing, since it takes ~2-3h to write one 
NCAR timestep. Limit to 3-4 timesteps for job, as 16h+ jobs will get canceled.
Remember `start_timestep` and `end_timestep` are inclusive.

- The script currently does not implement a custom Zarr encoding choice. It currently uses `Compression=None`. Please edit `dataset.py` to modify encoding parameters.

- The script reads the Timestep from the input file name 

- Optimal Zarr chunk size has been found to be $64^3$ by Mike Schnaubern and Ryan Hausen. This is the default chunk size.

- I've found that using Dask's local dir (`dask_local_dir='/home/idies/workspace/turb/data02_02', n_dask_workers=4)` is slower than not.

- The repo includes a mode to delete backup directories (. This is useful for cleaning up after a failed write. To use, run `main.py` with the `--delete` flag. Please use cautiously.

[//]: # (### Customizing Destination Layout and Assignment Schema)


[//]: # (If you need to adapt the destination layout for Zarr files or change the node assignment schema in this repository, you can do so by editing specific functions within `utils/write_utils.py`. Below are guidelines on where and how to make these changes:)


### Workflow Overview

1. Data Transformation: The script reads the specified NetCDF files and transforms them into Zarr format.
2. Data Distribution: The Zarr data is then distributed across FileDB nodes. The distribution process utilizes Ryan Hausen's node_assignment algorithm for efficient data placement.
3. Environment Configuration: For large datasets, consider configuring your environment to handle high memory and storage requirements.

## Testing

Running Zarr Data Correctness Tests
The VerifyZarrDataCorrectness test suite in tests/test_zarr_data_correctness.py is designed to verify the correctness of Zarr data conversion for specific datasets and timesteps. To run these tests, you need to set environment variables for specifying the dataset and timestep range.

Setting Environment Variables

Before running the tests, you need to set the following environment variables:

DATASET: The name of the dataset you want to test. For example, "NCAR-High-Rate-1".
START_TIMESTEP: The starting timestep number for testing.
END_TIMESTEP: The ending timestep number for testing.
You can set these variables in your terminal as follows:

For Linux/Mac:
```
export DATASET="NCAR-High-Rate-1"
export START_TIMESTEP=0
export END_TIMESTEP=2
```

For Windows:
```
set DATASET=NCAR-High-Rate-1
set START_TIMESTEP=0
set END_TIMESTEP=2
```
### Running the Tests

#### Data Correctness Test

- Check whether the written data matches the original by manually 
comparing all arrays

Use `pytest-xdist` (needs to be `pip install`-ed) as follows:

```
# Change to NCAR-High-Rate-1, NCAR-High-Rate-2 or NCAR-Low-Rate
# See config.yaml for the list of available datasets
export DATASET="NCAR-Low-Rate"
export WRITE_MODE=prod
cd /home/idies/workspace/Storage/ariel4/persistent/zarrify-across-network

# Done this way to prevent pytest from spawning too many threads and 
#   running out of memory 
for timestep in {1..10}  # Change as desired
do
    # Set START_TIMESTEP and END_TIMESTEP for the current iteration
    export START_TIMESTEP=$timestep
    export END_TIMESTEP=$timestep

    # Run the pytest in parallel with 34 threads
    ../zarr-py3.11/bin/python -m pytest -n 34 tests/test_zarr_data_correctness.py
done
```

#### Hash Integrity Test

- Check whether the hash of the original data matches the given hash.txt

This test case checks all timesteps in the given dataset folder, so no 
need to specify `start_timestep` and `end_timestep`.

```
# Change to NCAR-High-Rate-1, NCAR-High-Rate-2 or NCAR-Low-Rate
# See config.yaml for the list of available datasets
export DATASET="NCAR-High-Rate-2"
cd /home/idies/workspace/Storage/ariel4/persistent/zarrify-across-network

../zarr-py3.11/bin/python -m pytest -n 5 tests/test_hash_integrity.py
```


#### Zarr Attributes Test

- Check whether the Zarr attributes (compression, encoding, etc.) are as desired
for all NCAR timesteps. Only need to specify whether `prod` or `back` copy

In SciServer, can run this as a Small job, since it's very quick

```
export PROD_OR_BACKUP=prod

cd /home/idies/workspace/Storage/ariel4/persistent/zarrify-across-network

# Run the pytest in parallel with 34 threads
../zarr-py3.11/bin/python -m pytest -n 34 tests/test_zarr_attributes.py
```