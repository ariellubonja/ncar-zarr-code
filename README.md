# TODO Update


notebooks:

1. chunk-partial-decompress-NOshard - various read speed rests using Zarr chunking but Not Sharding

2. zarr_sharding - same but with Sharding

3. compression-tradeoff-1-var - Testing how much compression affects read speed, using only 1 variable from NCAR data, as opposed to the usual 6

4. round-robin - spread Zarr data round robin across fileDB nodes

5. NCAR to Zarr - convert NCAR netCDF into zarr with (64,64,64) chunk size

6. visualize-NCAR-animation - plot some 2D slices of NCAR data and create a video animation. Used to check sanity data 


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
