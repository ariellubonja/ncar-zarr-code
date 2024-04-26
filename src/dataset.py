# Author: Ariel Lubonja <ariellubonja@live.com>
# Date: 26-Dec-2023


from abc import ABC, abstractmethod
import queue
import threading
from .utils import write_utils
import xarray as xr
import dask
import glob
import os
import re
import warnings

import shutil


class Dataset(ABC):
    """
    A class meant to represent a Dataset to be distributed into Johns Hopkins' FileDB.

    ...

    Attributes
    ----------
    name : str
        The name of the dataset
    location_paths : list(str)
        Path to the directories containing Xarray-compatible files to be Zarrified and distributed to FileDB. One file
         in the directory should belong to one timestep of the data
    desired_zarr_chunk_size : int
        The chunk size to be used when writing to Zarr
    desired_zarr_array_length : int
        The desired side length of the 3D data cube represented by each Zarr Group

    ...

    Methods
    -------
    _get_data_cube_side():
        Gets the side length of the 3D data cube (private method)
    transform_to_zarr():
        Transforms the dataset to Zarr format (must be implemented by subclasses)
    distribute_to_filedb(PROD_OR_BACKUP='prod', USE_DASK=False, NUM_THREADS=34):
        Distributes the dataset to FileDB using Ryan Hausen's node_assignment() node coloring alg.
    """

    def __init__(self, name, location_paths, desired_zarr_chunk_size, desired_zarr_array_length, write_mode,
                 start_timestep, end_timestep):
        self.name = name
        self.location_paths = location_paths  # List of paths
        self.desired_zarr_chunk_size = desired_zarr_chunk_size
        self.desired_zarr_array_length = desired_zarr_array_length
        self.write_mode = write_mode
        self.original_array_length = None  # Must be populated by subclass method
        self.start_timestep = start_timestep
        self.end_timestep = end_timestep
        # TODO add something that reads end_timestep from folder instead of having to be specified

        # TODO Generalize this. It's hard-coded for NCAR
        self.encoding = {
            "velocity": dict(chunks=(desired_zarr_chunk_size, desired_zarr_chunk_size, desired_zarr_chunk_size, 3),
                             compressor=None),
            "pressure": dict(chunks=(desired_zarr_chunk_size, desired_zarr_chunk_size, desired_zarr_chunk_size, 1),
                             compressor=None),
            "temperature": dict(chunks=(desired_zarr_chunk_size, desired_zarr_chunk_size, desired_zarr_chunk_size, 1),
                                compressor=None),
            "energy": dict(chunks=(desired_zarr_chunk_size, desired_zarr_chunk_size, desired_zarr_chunk_size, 1),
                           compressor=None)}

    def _get_data_cube_side(self, data_xarray):
        raise NotImplementedError('TODO Implement reading the length of the 3D cube side from path')

    def get_zarr_array_destinations(self, timestep: int, range_list: list):
        """
        Destinations of all Zarr arrays pertaining to how they are distributed on FileDB, according to Node Coloring
        Args:
            timestep (int): timestep of the dataset to return paths for
            range_list: Where zarr subarrays start and end. Needed for Mike's code to find correct chunks to access

        Returns:
            list (str): List of destination paths of Zarr arrays
        """
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def transform_to_zarr(self, file_path):
        """
        Function that converts the dataset to Zarr format. Dataset-specific and therefore must be implemented by
        subclasses
        """
        raise NotImplementedError("Subclasses must implement this method")

    def distribute_to_filedb(self, NUM_THREADS=34):
        '''
        Write the production copy of the dataset to FileDB using Ryan
        Hausen's node_assignment() node coloring alg.

        Args:
            NUM_THREADS (int): Number of threads to use when writing to disk. Currently 34 to match nr. of disks on
                FileDB
        '''
        # Note that this multithreading works over multiple timesteps. 2nd
        #   timestep will start before 1st is finished
        for timestep in range(self.start_timestep, self.end_timestep + 1):
            lazy_zarr_cubes, range_list = self.transform_to_zarr(timestep)

            q = queue.Queue()

            # stores = [KVStore(zarr.DirectoryStore(path)) for path in dest_groupname]

            dests, _ = self.get_zarr_array_destinations(timestep, range_list)

            # Populate the queue with Write to FileDB tasks
            for i in range(len(dests)):
                q.put((lazy_zarr_cubes[i], dests[i], self.encoding))

            threads = []  # Create threads and start them
            for _ in range(NUM_THREADS):
                t = threading.Thread(target=write_utils.write_to_disk, args=(q,))
                t.start()
                threads.append(t)

            q.join()  # Wait for all tasks to be processed

            for t in threads:  # Wait for all threads to finish
                t.join()


    def create_backup_copy(self, NUM_THREADS=34):
        '''
        Write the backup copy of the dataset to FileDB, and shift
        the nodes by one, so prod and backup copies live on different
        disks. Make sure `prod` data is correct by running the tests/
        before running this function! Modified from ChatGPT

        Args:
            NUM_THREADS (int): Number of threads to use when writing to
            disk. Currently 34 to match nr. of disks on FileDB
        '''

        warnings.warn("Make sure the production dataset is "
                      "correct! This function simply copies the production "
                      "dataset and offsets it by one disk! Errors in `prod` will be"
                      "propagated. Make sure to run all tests before creating "
                      "this backup copy!", Warning)

        def worker(q):
            """Thread worker function to copy directories and overwrite existing ones."""
            while True:
                try:
                    src_path, dest_path = q.get_nowait()
                    print(f"Copying {src_path} to {dest_path}")
                    # Check if the destination path exists and remove it if it does
                    if os.path.exists(dest_path):
                        shutil.rmtree(dest_path)
                    shutil.copytree(src_path, dest_path, dirs_exist_ok=True)
                    q.task_done()
                except queue.Empty:
                    break
                except Exception as e:
                    print(f"Error copying {src_path} to {dest_path}: {e}")
                    q.task_done()

        filedb_folders = write_utils.list_fileDB_folders()
        q = queue.Queue()

        for i in range(len(filedb_folders)):
            current_dir = filedb_folders[i]
            next_dir = filedb_folders[(i + 1) % len(filedb_folders)]  # Wrap around to the first directory

            # Scan current directory for folders matching 'dataset_name_xx_prod'
            for folder in os.listdir(current_dir):
                if self.name in folder and folder.endswith('_prod'):
                    src_path = os.path.join(current_dir, folder)
                    # Replace '_prod' with '_back' in folder name
                    dest_folder = folder.replace('_prod', '_back')
                    dest_path = os.path.join(next_dir, dest_folder)
                    q.put((src_path, dest_path))

        threads = []
        for _ in range(NUM_THREADS):
            t = threading.Thread(target=worker, args=(q,))
            t.start()
            threads.append(t)

        q.join()  # Block until all tasks are done

        for t in threads:
            t.join()  # Make sure all threads have finished

        print("Backup creation completed.")


    def delete_backup_directories(self, NUM_THREADS=34):
        """
        Deletes directories that match 'sabl2048a_xx_back' in parallel using threading.
        Modified from ChatGPT

        Args:
            filedb_folders (list of str): List of folders where directories are to be deleted.
            num_threads (int): Number of threads to use for parallel deletion.
        """
        filedb_folders = write_utils.list_fileDB_folders()

        def directory_deleter(q):
            """ Thread worker function to delete directories. """
            while not q.empty():
                try:
                    path = q.get_nowait()
                    print(f"Deleting directory: {path}")
                    shutil.rmtree(path)
                    q.task_done()
                except queue.Empty:
                    break
                except Exception as e:
                    print(f"Failed to delete {path}: {e}")
                    q.task_done()

        to_delete = []
        q = queue.Queue()

        # Collect all directories to delete
        for folder in filedb_folders:
            if os.path.exists(folder):
                for subfolder in os.listdir(folder):
                    if self.name in subfolder and subfolder.endswith('_back'):
                        full_path = os.path.join(folder, subfolder)
                        to_delete.append(full_path)

        # Display directories to be deleted and ask for user confirmation
        if not to_delete:
            print("No directories to delete.")
            return

        print("Directories to be deleted:")
        for path in to_delete:
            print(path)

        # Request user confirmation
        response = input("\nAre you sure you want to delete these directories? This cannot be undone. Y/N: ")
        if response.lower() == 'y':
            for path in to_delete:
                q.put(path)

            threads = []
            # Start threads
            for _ in range(NUM_THREADS):
                t = threading.Thread(target=directory_deleter, args=(q,))
                t.start()
                threads.append(t)

            q.join()  # Wait for the queue to be empty

            for t in threads:  # Ensure all threads have finished
                t.join()

            print("Deletion completed.")
        else:
            print("Deletion aborted by user.")


class NCAR_Dataset(Dataset):
    """
        National Center for Atmospheric Research (NCAR) 2048^3 dataset.

        This class implements transform_to_zarr(). Please see Dataset superclass for more details
    """

    def __init__(self, name, location_paths, desired_zarr_chunk_size, desired_zarr_array_length, write_mode,
                 start_timestep, end_timestep):
        super().__init__(name, location_paths, desired_zarr_chunk_size, desired_zarr_array_length, write_mode,
                         start_timestep, end_timestep)

        self.file_extension = '.nc'
        self.NCAR_files = []
        for path in self.location_paths:
            self.NCAR_files += glob.glob(os.path.join(path, f'*{self.file_extension}'))
        self.original_array_length = 2048

    def transform_to_zarr(self, timestep: int) -> tuple[list, list]:
        """
        Read and lazily transform the NetCDF data of NCAR to Zarr. This makes data ready for distributing to FileDB.
        """
        cubes, range_list = self._prepare_NCAR_NetCDF(timestep)
        cubes = write_utils.flatten_3d_list(cubes)

        return cubes, range_list

    def _prepare_NCAR_NetCDF(self, timestep: int):
        """
        Prepare data for writing to FileDB. This includes:
            - Merging velocity components
            - Splitting into smaller chunks (64^3 or desired_zarr_chunk_size^3)
            - Unabbreviating variable names
            - Splitting 2048^3 arrays into 512^3 chunks (original_array_length -> desired_zarr_array_length)

        This function deals with the intricaties of the NCAR dataset. It is not meant to be used for other datasets.
        """
        # TODO The variable names are hard-coded

        def select_file(file_list, timestep):
            for full_path in file_list:
                # Extract the filename from the full path
                filename = os.path.basename(full_path)

                # Extract the number from the filename using a more specific regular expression
                match = re.search(r'jhd\.(\d+)\.nc', filename)
                if match:
                    file_timestep = int(match.group(1))
                    if file_timestep == timestep:
                        return full_path
            # If no file is found, raise an exception
            raise FileNotFoundError(f"No file found for timestep {timestep}")
        
        # Open the dataset using xarray
        data_xr = xr.open_dataset(select_file(self.NCAR_files,timestep),
                                  chunks={'nnz': self.desired_zarr_chunk_size, 'nny': self.desired_zarr_chunk_size,
                                          'nnx': self.desired_zarr_chunk_size})

        assert isinstance(data_xr['e'].data, dask.array.core.Array)

        self.array_cube_side = self._get_data_cube_side(data_xr)

        # Add an extra dimension to the data to match isotropic8192 Drop is there to drop the Coordinates object
        # that is created - this creates a separate folder when to_zarr() is called
        expanded_ds = data_xr.expand_dims({'extra_dim': [1]}).drop_vars('extra_dim')
        # The above adds the extra dimension to the start. Fix that - put it in the back
        transposed_ds = expanded_ds.transpose('nnz', 'nny', 'nnx', 'extra_dim')

        # Group 3 velocity components together
        # Never use dask with remote network location on this!!
        merged_velocity = write_utils.merge_velocities(transposed_ds, chunk_size_base=self.desired_zarr_chunk_size)

        # TODO this is also hard-coded
        merged_velocity = merged_velocity.rename({'e': 'energy', 't': 'temperature', 'p': 'pressure'})

        dims = [dim for dim in data_xr.dims]
        dims.reverse()  # use (nnz, nny, nnx) instead of (nnx, nny, nnz)

        # Split 2048^3 into smaller 512^3 arrays
        smaller_groups, range_list = write_utils.split_zarr_group(merged_velocity, self.desired_zarr_array_length, dims)

        return smaller_groups, range_list

    def _get_data_cube_side(self, data_xarray: xr.Dataset) -> int:
        """
        Gets the side length of one 3D cube for the NCAR dataset (private method)

        Args:
            data_xarray (xarray.Dataset): The xarray Dataset object containing the data

        Returns:
            int: The side length of the array cube. For example, if each Variable in the dataset is 2048^3, then this
                function returns 2048
        """
        return data_xarray['e'].data.shape[0]

    def get_zarr_array_destinations(self, timestep: int, range_list: list):
        """
        Destinations of all Zarr arrays pertaining to how they are distributed on FileDB, according to Node Coloring
        Args:
            timestep (int): timestep of the dataset to process
            range_list (list): Where chunks start and end. Needed for Mike's code to find correct chunks to access

        Returns:
            list[str]: List of destination paths of Zarr arrays
        """
        folders = write_utils.list_fileDB_folders()

        for i in range(len(folders)):
            if self.write_mode == "prod":
                idx = i
            elif self.write_mode == "back":  # Shift back by 1
                idx = i - 1
            
            folders[idx] += self.name + "_" + str(idx + 1).zfill(2) + "_" + self.write_mode + "/"

        if self.write_mode == "back":
            # Shift list of FileDB folders by 1
            first_element = folders.pop(0)
            folders.append(first_element)

        chunk_morton_mapping = write_utils.get_chunk_morton_mapping(range_list, self.name)
        # print("chunk_morton_mapping name: ", chunk_morton_mapping)
        flattened_node_assgn = write_utils.flatten_3d_list(write_utils.node_assignment(4))

        dests = []

        for i in range(len(range_list)):
            min_coord = [a[0] for a in range_list[i]]
            max_coord = [a[1] - 1 for a in range_list[i]]

            morton = (write_utils.morton_pack(self.original_array_length, min_coord[0], min_coord[1], min_coord[2]),
                      write_utils.morton_pack(self.original_array_length, max_coord[0], max_coord[1], max_coord[2]))

            chunk_name = write_utils.search_dict_by_value(chunk_morton_mapping, morton)
            # print("Chunk name: ", chunk_name)
            # raise Exception

            idx = int(chunk_name[-2:].lstrip('0'))

            filedb_index = flattened_node_assgn[idx - 1] - 1

            destination = os.path.join(folders[filedb_index],
                                       self.name + str(idx).zfill(2) + "_" + str(timestep).zfill(3) + ".zarr")

            dests.append(destination)

        return dests, chunk_morton_mapping
