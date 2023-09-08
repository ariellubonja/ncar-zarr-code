# Ariel's Edit from Ryan Hausen's code 17-Jun-2023, 7:35PM

import logging
import os
from itertools import filterfalse, product, repeat, starmap
from time import time
from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor

import click
import multiprocessing as mp
import numpy as np
import regex as re
import xarray as xr
import zarr
from tqdm import tqdm

import giverny.turbulence_toolkit as tt
import giverny.turbulence_gizmos.basic_gizmos as bg
import giverny.isotropic_cube as dataset

GivernyIndex = Tuple[int, int]

from SciServer import Authentication

# redirect verbose output from functions
#https://stackoverflow.com/a/54955536
from contextlib import contextmanager
import sys
@contextmanager
def stdout_redirector():
    class MyStream:
        def write(self, msg):
            pass
        def flush(self):
            pass
    old_stdout = sys.stdout
    sys.stdout = MyStream()
    try:
        yield
    finally:
        sys.stdout = old_stdout
        

def retrieve_data(
    idxs:Tuple[GivernyIndex, GivernyIndex, GivernyIndex],
    dataset_title:str = 'isotropic8192', 
    output_path:str = './turbulence_output', 
    variable:str = "pressure", 
    timepoint:int = 1, 
) -> Tuple[np.ndarray, str]:
    xs, ys, zs = idxs
    cube = dataset.iso_cube(dataset_title = dataset_title, output_path=output_path)
    with stdout_redirector():
        axes_ranges = bg.assemble_axis_data([xs, ys, zs])
        strides = bg.assemble_axis_data([1, 1, 1])
        cutout_data, fname = tt.getCutout(cube, variable, timepoint, axes_ranges, strides)

    return cutout_data, fname

        
def convert_idx(
    xs_ys_zs:Tuple[GivernyIndex, GivernyIndex, GivernyIndex]
) -> Tuple[int, int, int]:
    return list(map(
        lambda idx: idx[0]//512,
        xs_ys_zs
    ))
    
    
node_pat = re.compile(r"data\d\d_\d\d")
cube_pat = re.compile(r"iso8192db_\d\d")
def convert_path_to_zarr(
    folder_name_idxs:List[str], 
    node_assignment:str, 
    path:str
) -> str:
    # folders are one indexed
    folder_idx = folder_name_idxs.index(node_assignment) + 1
    folder_name = f"iso8192db_{folder_idx:02}_prod"
    return re.sub(
        node_pat, 
        lambda s: node_assignment + "/zarr", 
        re.sub(
            cube_pat,
            lambda s: folder_name,
            path
        )
    ).replace(
        "_vel",
        ""
    ).replace(
        ".bin", 
        ".zarr"
    )
    
        
def convert_prod_path_to_backup_path(folder_name_idxs:List[str], path:str) -> str:
    prod_node = re.search(
        node_pat, 
        path
    )[0]
    backup_node_idx = (folder_name_idxs.index(prod_node) + 1) % len(folder_name_idxs)
    backup_node = folder_name_idxs[backup_node_idx]
    
    return re.sub(
        node_pat,
        backup_node,
        path,
    ).replace("_prod", "_back")

# https://stackoverflow.com/a/11233293/2691018
formatter = logging.Formatter('%(asctime)s %(message)s')
def setup_logger(name, log_file, level=logging.INFO):
    """To setup as many loggers as you want"""

    handler = logging.FileHandler(log_file)        
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


def extract_and_convert_idxs(
    timepoint:int,
    folder_name_idxs: List[str],
    node_assignment:str, 
    xs_ys_zs:Tuple[GivernyIndex, GivernyIndex, GivernyIndex],
) -> str:

    logger_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "logs",
        f"{'.'.join(map(str, xs_ys_zs))}.child.log"
    )

    logger = setup_logger(
        f"distribute_zarr_job_{str(xs_ys_zs)}",
        logger_file_path,
    )
    
    if os.path.exists(logger_file_path):
        with open(logger_file_path, "r") as f:
            if "Completed job" in f.read():
                logger.info("Job already completed.")
                return
    
    variable = "velocity"
    output_path = f"/home/idies/workspace/turb/data01_01/zarr/tmp_ryan/turbulence2-read_box_data/turbulence_output/turb_out_{variable}"
    dataset_title = "isotropic8192"
    total_start = time()
    
    logger.info(f"Retrieving velocity at t={timepoint}...")
    
    try:
        start = time()
        velocity_cube, location = retrieve_data(
            xs_ys_zs,
            dataset_title,
            output_path,
            variable,
            timepoint,
        )
    except Exception as e:
        logger.error(str(e))
        raise e
        return f"Velocity failed = {str(e)}"
        
    else:
        logger.info(f"velocity retrieved {time()-start} seconds")
    
    
    logger.info(f"Retrieving pressure at t={timepoint}...")
    start = time()
    variable = "pressure"
    output_path = f"./turbulence_output/turb_out_{variable}"
    
    try:
        pressure_cube, _ = retrieve_data(
            xs_ys_zs,
            dataset_title,
            output_path,
            variable,
            timepoint,
        )
    except Exception as e:
        logger.error(str(e))
        return f"Pressure failed = {str(e)}"
    else:
        logger.info(f"pressure retrieved {time() - start} seconds")

    
    logger.info("Building xarray dataset...")
    start = time()
    xr_dataset = xr.Dataset(
       data_vars = {
           "pressure": (
               ("x", "y", "z", "p"), 
               pressure_cube.data.astype(np.float32)
           ),
           "velocity": (
               ("x", "y", "z", "v"),
               velocity_cube.data.astype(np.float32)
           ),
       },
       coords = {
            "z" : np.arange(512),
            "y" : np.arange(512),
            "x" : np.arange(512),
        }
    )
    del velocity_cube
    del pressure_cube
    logger.info(f"built {time() - start}")
    
    def save_store(path) -> str:
        store = zarr.storage.FSStore(
            path, 
            modestr="w", 
            compression="None",
        )
        
        try:
            xr_dataset.to_zarr(
                store=store,
                mode="w",
                encoding={
                    "velocity": dict(chunks=(64, 64, 64, 3), compressor=None),
                    "pressure": dict(chunks=(64, 64, 64, 1), compressor=None),
                }
            )
        except Exception as e:
            logger.info(f"{type(e)}, {str(e)}")
            return f"Saving to {path} failed = {type(e)}, {str(e)}"
    
        return "Success!"
    
    prod_path = convert_path_to_zarr(folder_name_idxs, node_assignment, location)
    back_path = convert_prod_path_to_backup_path(folder_name_idxs, prod_path) 
    
    logger.info("Saving data...")
    start = time()
    with ThreadPoolExecutor(2) as p:
        results = list(p.map(save_store, [prod_path, back_path]))
    logger.info(f"Saving data complete {time()-start} seconds")
    logger.info(f"Completed job in {time() - total_start} seconds")
    
    failed = list(filter(lambda r: r!="Success!", results))
    if len(failed)>1:
        return " ".join(failed)
    else:
        logger.info(f"Completed job in {time() - total_start} seconds")
        return "Success!" 
        

def get_range(start:int, end:int) -> List[GivernyIndex]:
    ends = list(reversed(range(end, start-1, -512)))
    starts = list(map(lambda x: x-511, ends))
    return list(zip(starts, ends))


@click.command()
@click.option("--xs", type=(int,int), required=True)
@click.option("--ys", type=(int,int), required=True)
@click.option("--zs", type=(int,int), required=True)
@click.option("--time_step", type=int, required=True)
@click.option("--n_procs", type=int, default=4)
def convert_binary(
    xs:GivernyIndex,
    ys:GivernyIndex,
    zs:GivernyIndex,
    time_step:int,
    n_procs:int,
) -> None:
     # each cube pressure+velocity is ~1.5GB

    # These are the names of the filedb nodes 
    assignable_nodes = list(map(
        lambda xy: "data{:02}_{:02}".format(*xy),
        filterfalse(
            lambda xy: (xy[0]==9 and xy[1]==2) or (xy[0]==7 and xy[1]==2),
            product(range(1, 13), range(1, 4)),
        )
    ))

    # These are the filedb nodes sorted by the ordering used for the binary files
    folder_name_idxs = sorted(
        assignable_nodes, 
        key=lambda s: (s[-2:], s[4:6])
    )
    
    node_assignments = np.load("node_assignment.npy") - 1
    
    def transform_idxs_to_job(
        xs_ys_zs:Tuple[GivernyIndex, GivernyIndex, GivernyIndex]
    ) -> Tuple[str, Tuple[GivernyIndex, GivernyIndex, GivernyIndex]]:
        x, y, z = convert_idx(xs_ys_zs)
        node_assignment = assignable_nodes[node_assignments[x, y, z]]
        return (time_step, folder_name_idxs, node_assignment, xs_ys_zs)
    
    parent_log_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "logs",
        f"{'.'.join(map(str, [xs, ys, zs]))}.parent.log"
    )
    print("logging to", parent_log_dir)
    logger = setup_logger(
        "distribute_zarr_job_all",
        parent_log_dir,
    )
    
    x_ranges = get_range(*xs)
    y_ranges = get_range(*ys)
    z_ranges = get_range(*zs)
    
    idxs = list(product(x_ranges, y_ranges, z_ranges))
    
    logger.info(f"Working on {idxs}")
    
    assignments_idxs = list(map(transform_idxs_to_job, idxs))
        
    # logger.info(f"Jobs {assignments_idxs}")

    logger.info("Starting jobs...")
    
    start = time()
    
    try:
        n_jobs = len(assignments_idxs)
        if n_jobs > 1 and n_procs > 1:
            logger.info("working in parallel")
            with mp.Pool(min(n_procs, n_jobs), ) as p:
                for i, msg in enumerate(p.starmap(extract_and_convert_idxs, assignments_idxs, chunksize=1)):
                    logger.info(f"Completed cube {i}: {msg}")
        else:
            logger.info("working in serial")
            for i, msg in enumerate(starmap(extract_and_convert_idxs, assignments_idxs)):
                logger.info(f"Completed cube {i}: {msg}")
                
    except Exception as e:
        logger.error(str(type(e)) + " " + str(e))
        raise e
    else:
        logger.info(f"Completed work {time() - start}")
    
    
if __name__ == '__main__':
    convert_binary()