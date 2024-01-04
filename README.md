# Dataset Distribution to Zarr and FileDB

This package is designed to transform datasets into [Zarr format](https://zarr.readthedocs.io/en/stable/) and distribute them on [Johns Hopkins' FileDB](https://turbulence.pha.jhu.edu/datasets.aspx) disks. It handles data chunking, Zarr encoding and Compression, and writing both production and backup distribution types.

### Script Usage
Run `main.py` with the necessary arguments to initiate the process of converting and distributing data. The script requires the timestep, path to the NCAR .netcdf files, and other optional parameters.


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

## If You Want to Reuse Script for Your Own Data and Network of Disks

[//]: # (### Customizing Destination Layout and Assignment Schema)


[//]: # (If you need to adapt the destination layout for Zarr files or change the node assignment schema in this repository, you can do so by editing specific functions within `utils/write_utils.py`. Below are guidelines on where and how to make these changes:)

#### Changing Destination Paths for Distributed Files


To customize the dispersion of Zarr files across your network of disks:

- Edit the Destination Path Logic: `utils/write_utils.py:get_512_chunk_destinations()` determines the destination paths for the Zarr files in an ordered list. Change this to match your desired directory structure

#### Adapting the Disk Node Assignment Schema

This code is located in `utils/write_utils.py:node_assignment()`

The function takes in the number of nodes and the number of batches and returns a list of lists, where each sublist contains the node assignments for a batch. The current implementation uses the [Node/Map Coloring algorithm](https://en.wikipedia.org/wiki/Graph_coloring#Node_coloring) to assign nodes to batches. You can modify this function to implement your own node assignment schema.

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

bash
Copy code
export DATASET="NCAR-High-Rate-1"
export START_TIMESTEP=0
export END_TIMESTEP=2
For Windows:

cmd
Copy code
set DATASET=NCAR-High-Rate-1
set START_TIMESTEP=0
set END_TIMESTEP=2
Running the Tests

After setting the environment variables, run the tests using the unittest module:

bash
Copy code
python -m unittest tests/test_zarr_data_correctness.py
The tests will automatically read the environment variables and run the data correctness checks for the specified dataset and timesteps.


### Jupyter Notebooks Description

The repository contains code in notebooks. Some of them are used for testing and some for visualization. There is also deprecated code. Here is a brief description of the notebooks:

1. chunk-partial-decompress-NOshard - various read speed rests using Zarr chunking but Not Sharding

2. zarr_sharding - same but with Sharding

3. compression-tradeoff-1-var - Testing how much compression affects read speed, using only 1 variable from NCAR data, as opposed to the usual 6

4. round-robin - spread Zarr data round robin across fileDB nodes

5. NCAR to Zarr - convert NCAR netCDF into zarr with (64,64,64) chunk size

6. visualize-NCAR-animation - plot some 2D slices of NCAR data and create a video animation. Used to check sanity data 
