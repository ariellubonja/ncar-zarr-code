# Dataset Distribution to Zarr and FileDB

This package is designed to transform datasets into [Zarr format](https://zarr.readthedocs.io/en/stable/) and distribute them on [Johns Hopkins' FileDB](https://turbulence.pha.jhu.edu/datasets.aspx) disks. It handles data chunking, Zarr encoding and Compression, and writing both production and backup distribution types.

### Script Usage
Run `main.py` with the necessary arguments to initiate the process of converting and distributing data. The script requires the timestep, path to the NCAR .netcdf files, and other optional parameters.


Command-Line Arguments:


- --timestep: The timestep number for the NCAR data (required).
- -p or --path: Path to the location of the NCAR .netcdf files (required).
- -zc or --zarr_chunk_size: Zarr chunk size. Defaults to 64.
- --desired_cube_side: Desired side length of the 3D data cube. Defaults to 512.
- --zarr_encoding: Boolean flag to enable custom Zarr encoding. Currently not implemented. Defaults to True.
- --distribution: Type of distribution - "prod" for production or "back" for backup. Defaults to "prod".


Example Command:

> python main.py --timestep 10 -p /path/to/ncar/netcdf/files --zarr_chunk_size 64 --desired_cube_side 512 --distribution prod


Notes:

The script currently does not implement custom Zarr encoding (`--zarr_encoding` flag). Please edit `main.py` to modify encoding parameters.

Optimal Zarr chunk size has been found to be 64^3 by Mike Schnaubern and Ryan Hausen. This is the default chunk size.


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
