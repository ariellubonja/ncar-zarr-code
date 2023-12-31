# Dataset Distribution to Zarr and FileDB

This package is designed to transform datasets into [Zarr format](https://zarr.readthedocs.io/en/stable/) and distribute them on [Johns Hopkins' FileDB](https://turbulence.pha.jhu.edu/datasets.aspx) disks. It handles data chunking, Zarr encoding and Compression, and writing both production and backup distribution types.

### Script Usage
Run `main.py` with the necessary arguments to initiate the process of converting and distributing data. The script requires the timestep, path to the NCAR .netcdf files, and other optional parameters.


Command-Line Arguments:


[//]: # (- --timestep: The timestep number for the NCAR data &#40;required&#41;.)
- -p or --path: Path to the location of the data file (required). Specify individual filenames for each timestep, not the directory.
- -zc or --zarr_chunk_size: Zarr chunk size. Defaults to 64.
- --desired_cube_side: Desired side length of the 3D data cube. Defaults to 512.
- --zarr_encoding: Boolean flag to enable custom Zarr encoding. Currently not implemented. Defaults to True.
- --distribution: Type of distribution - "prod" for production or "back" for backup. Defaults to "prod".


Example Command:

> python src/main.py -p /path/to/ncar/netcdf/files/jhd.000.nc -n sabl2048a


##### Notes

- Input must be [Xarray-compatible](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html)

- The script currently does not implement custom Zarr encoding (`--zarr_encoding` flag). Please edit `main.py` to modify encoding parameters.

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

### Zarr Data Correctness Test

`VerifyZarrDataCorrectness` is designed to verify the correctness of the conversion of NetCDF data into Zarr files. This test manually compares the data matrices. See `test_zarr_attributes.py` for Zarr metadata and attribute tests.


#### Running the Test
Directly run the test by navigating to the root directory of the project and run the test using the following command:

```
TIMESTEP_NR=<your_timestep_nr> python -m unittest path/to/VerifyZarrDataCorrectness.py
```

#### Setting the Environment Variable

If you'd like, you can set the Environment Variable for the current terminal session.

Before running the test, set the `TIMESTEP_NR` environment variable to the desired timestep number. This can be done as follows:

On Linux/Mac:
```
export TIMESTEP_NR=<your_timestep_number>
```
On Windows:

```
set TIMESTEP_NR=<your_timestep_number>
```

Replace `<your_timestep_number>` with the actual timestep number you want to test.


### Jupyter Notebooks Description

The repository contains code in notebooks. Some of them are used for testing and some for visualization. There is also deprecated code. Here is a brief description of the notebooks:

1. chunk-partial-decompress-NOshard - various read speed rests using Zarr chunking but Not Sharding

2. zarr_sharding - same but with Sharding

3. compression-tradeoff-1-var - Testing how much compression affects read speed, using only 1 variable from NCAR data, as opposed to the usual 6

4. round-robin - spread Zarr data round robin across fileDB nodes

5. NCAR to Zarr - convert NCAR netCDF into zarr with (64,64,64) chunk size

6. visualize-NCAR-animation - plot some 2D slices of NCAR data and create a video animation. Used to check sanity data 
