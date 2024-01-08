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
