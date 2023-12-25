"""
    Collection of functions for reading or loading original NetCDF or Zarr arrays
"""
import os


def extract_netcdf_timestep(file_path: str) -> int:
    """
        Extracts the timestep nr. from the raw NetCDF file path

        Args:
            file_path (str): The path to the NetCDF file.

        Returns:
            int: The extracted timestep number as an integer.
    """
    filename = os.path.basename(file_path)
    # Split the filename and extract the part with the timestep number
    timestep_part = filename.split('.')[1]

    return int(timestep_part)
