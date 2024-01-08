# Reduce Data Read Time by Disk-Parallel Access

This data management tool is designed to efficiently handle and distribute large scientific datasets. This package transforms datasets into the Zarr format and smartly distributes them across multiple disks, enabling faster parallel data access. It's an ideal solution for scientists and data analysts working with extensive datasets, such as in climate research or astronomy.

This package is designed to transform any Xarray-compatible scientific datasets into [Zarr format](https://zarr.readthedocs.io/en/stable/) and distribute these Zarr files to a network of disks, _**to maximize Parallel Read speed by using multiple disks**_. The code is written to target [Johns Hopkins' FileDB Database](https://turbulence.pha.jhu.edu/datasets.aspx), but is written to be easily adaptible to your own use case

The script handles:

- Writing to disks in round-robin fashion, to load-balance disk usage
- separate neighboring data subarrays, so that no two neighboring data chunks end up on the same disk. This is done so that for any contiguous data access, parallel read is maximized.
- Morton/z-ordering of chunks, to ensure near-optimal linearization of data chunks
- Zarr encoding and Compression
- writing both production and backup copies. By default, backup copies are shifted by one, to minimize data loss overlap in case of disk failure

### Script Installation
Simply use `git clone https://github.com/ariellubonja/zarrify-across-network` to download this repository.

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


### Running the Script

Run `main.py` with the necessary arguments to initiate the process of converting and distributing data. The script requires the timestep, path to the input Xarray-compatible files, and other optional parameters.


Command-Line Arguments:


[//]: # (- --timestep: The timestep number for the NCAR data &#40;required&#41;.)
- -p or --path: Path to the location of the data file (required). Specify individual filenames for each timestep, not the directory.
- -zc or --zarr_chunk_size: Zarr chunk size. Defaults to 64.
- --desired_cube_side: Desired side length of the 3D data cube. Defaults to 512.
- --distribution: Type of distribution - "prod" for production or "back" for backup. Defaults to "prod".
[//]: # (- --zarr_encoding: Boolean flag to enable custom Zarr encoding. Currently not implemented. Defaults to True.)


Example Command:

> python src/main.py -p /path/to/ncar/netcdf/files/jhd.000.nc -n sabl2048a


##### Notes

- Input must be [Xarray-compatible](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html)

- The script currently does not implement a custom Zarr encoding choice. It currently uses `Compression=None`. Please edit `dataset.py` to modify encoding parameters.

- The script reads the Timestep from the input file name 

- Optimal Zarr chunk size has been found to be 64^3 by Mike Schnaubern and Ryan Hausen. This is the default chunk size.

- I've found that using Dask's local dir (`dask_local_dir='/home/idies/workspace/turb/data02_02', n_dask_workers=4)` is slower than not.

[//]: # (### Customizing Destination Layout and Assignment Schema)


[//]: # (If you need to adapt the destination layout for Zarr files or change the node assignment schema in this repository, you can do so by editing specific functions within `utils/write_utils.py`. Below are guidelines on where and how to make these changes:)

#### Need Further Assistance?
If you encounter difficulties or have specific questions about customizing the code for your use case, please feel free to open an issue in the repository. We're here to help and would be happy to assist you in adapting the tool to your specific needs.


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

After setting the environment variables, run the tests using the unittest module:

> python -m unittest tests/test_zarr_data_correctness.py

The tests will automatically read the environment variables and run the data correctness checks for the specified dataset and timesteps.
