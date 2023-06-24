import numpy as np
import xarray as xr
import dask.array as da
from dask.distributed import Client


# ChatGPT
def split_zarr_group(ds, smaller_size, dims):
    """
    Takes an xarray group of arrays and splits it into smaller groups
    E.g. it takes a 2048^3 group of 6 arrays and returns 64 groups of 512^3

    Parameters:
        ds (xarray.Dataset): The Xarray group you want to split
        smaller_size (int): Size of the chunks along each dimension.
        dims (tuple): Names of the dimensions in order (dim_0, dim_1, dim_2). This can be gotten in reverse order using [dim for dim in data_xr.dims]
        
    Returns:
        list[xarray.Dataset]: A list of smaller Datasets.
    """

    # Calculate the number of chunks along each dimension
    num_chunks = [ds.dims[dim] // smaller_size for dim in dims]

    chunks = []

    if len(dims) == 3:
        for i in range(num_chunks[0]):
            for j in range(num_chunks[1]):
                for k in range(num_chunks[2]):
                    # Select the chunk from each DataArray
                    chunk = ds.isel(
                        {dims[0]: slice(i * smaller_size, (i + 1) * smaller_size),
                         dims[1]: slice(j * smaller_size, (j + 1) * smaller_size),
                         dims[2]: slice(k * smaller_size, (k + 1) * smaller_size)}
                    )
                    chunks.append(chunk)
    # Grouped Velocity is 4D
    elif len(dims) == 4:
        for i in range(num_chunks[0]):
            for j in range(num_chunks[1]):
                for k in range(num_chunks[2]):
                    for l in range(num_chunks[3]):
                        # Select the chunk from each DataArray
                        chunk = ds.isel(
                            {dims[0]: slice(i * smaller_size, (i + 1) * smaller_size),
                             dims[1]: slice(j * smaller_size, (j + 1) * smaller_size),
                             dims[2]: slice(k * smaller_size, (k + 1) * smaller_size),
                             dims[3]: slice(l * smaller_size, (l + 1) * smaller_size)}
                        )
                        chunks.append(chunk)

    return chunks



def list_fileDB_folders():
    return [f'/home/idies/workspace/turb/data{str(d).zfill(2)}_{str(f).zfill(2)}/zarr/'  for f in range(1,4) for d in range(1,13)]



def merge_velocities(data_xr):
    """
        Merge the 3 velocity components/directions - such merging exhibits faster 3-component reads. This is a Dask lazy computation
        
        :param data_xr: the dataset (Xarray group) with 3 velocity components to merge
    """
    client = Client()

    
    b = da.stack([data_xr['u'], data_xr['v'], data_xr['w']], axis=3)

    result = b.rechunk((chunk_size_base,chunk_size_base,chunk_size_base,3))
    
    client.close()

    return result