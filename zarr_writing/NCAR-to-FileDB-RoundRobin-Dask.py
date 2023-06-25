import xarray as xr
import os
import write_tools
import dask

# Parameters
desired_cube_side = 512
chunk_size = 64
raw_ncar_folder_path = '/home/idies/workspace/turb/data02_02/ariel-6-timestep-ncar-netCDF'
use_dask = True # Has issues on SciServer Compute
dest_folder_name = "ncardb"


@dask.delayed
def write_to_disk(dest_group_name, current_array, chunk_size):
    encoding = {variable_name: {'compressor': None, 'chunks': (chunk_size, chunk_size, chunk_size)} for variable_name in current_array.variables}
    current_array.to_zarr(store=dest_group_name,
        mode="w",
        encoding = encoding)


if __name__ == "__main__":
    folders=write_tools.list_fileDB_folders()

    # Avoiding 7-2 and 9-2 - they're too full as of May 2023
    folders.remove("/home/idies/workspace/turb/data09_02/zarr/")
    folders.remove("/home/idies/workspace/turb/data07_02/zarr/")


    # Convert netCDF and Round-Robin over FileDB in Z-order
    timestep_nr = 0 # Add this to file name

    # contention on data_xr = xr.open_dataset(file_name) bcs all source data live on data02_02
    for file_name in os.listdir(raw_ncar_folder_path):
        data_xr = xr.open_dataset(file_name)
        merged_velocity = write_tools.merge_velocities(data_xr, chunk_size_base=chunk_size, use_dask=use_dask)

        dims = [dim for dim in data_xr.dims]
        smaller_groups = write_tools.split_zarr_group(merged_velocity, desired_cube_side, dims)
        z_order = write_tools.morton_order_cubes(smaller_groups)

        tasks = []
        for i in range(len(smaller_groups)):
            for j in range(len(smaller_groups[i])):
                for k in range(len(smaller_groups[i][j])):
                    filedb_index = z_order[i][j][k] % len(folders) # Loop around FileDB nodes
                    current_array = smaller_groups[i][j][k]
                    tasks.append(write_to_disk(folders[filedb_index] + dest_folder_name + str(i) + str(j) + str(k) + "_" + str(timestep_nr) + ".zarr", current_array, chunk_size))

        # And this is where the computation happens
        dask.compute(*tasks)

        timestep_nr += 1
