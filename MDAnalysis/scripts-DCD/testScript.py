#!/usr/bin/env python
from __future__ import print_function, division
import numpy as np
import MDAnalysis as mda
from MDAnalysis import Universe, Writer
from MDAnalysis.analysis.align import rotation_matrix
from MDAnalysis.lib.qcprot import CalcRMSDRotationalMatrix
import dask
from dask.distributed import Client, performance_report
from dask.delayed import delayed
import time
import multiprocessing
from shutil import copyfile
import pickle
import sys
import warnings
from Bio import BiopythonDeprecationWarning
from proxystore.connectors.file import FileConnector
from proxystore.store import Store, get_or_create_store
import os
import argparse
import statistics

warnings.simplefilter('ignore', BiopythonDeprecationWarning)
warnings.filterwarnings('ignore', category=DeprecationWarning, module='MDAnalysis.coordinates.DCD')

def pickle_size(obj):
    return sys.getsizeof(pickle.dumps(obj))

def rmsd(mobile, xref0):
    xmobile0 = mobile.positions - mobile.center_of_mass()
    return CalcRMSDRotationalMatrix(xref0.T.astype(np.float64), xmobile0.T.astype(np.float64), mobile.n_atoms, None, None)

def block_rmsd(ag, ref0, start=None, stop=None, step=None, store_config=None):
    start_block_rmsd = time.time()
    
    u = ag.universe
    xref0 = ref0.positions - ref0.center_of_mass()

    clone = mda.Universe(u._topology, u.trajectory.filename, **u.kwargs)
    g = clone.atoms[ag.indices]
    assert u != clone

    bsize = stop - start
    results = np.zeros([bsize, 2])
    t_comp = np.zeros(bsize)

    start1 = time.time()
    for iframe, ts in enumerate(clone.trajectory[start:stop:step]):
        start2 = time.time()
        results[iframe, :] = ts.time, rmsd(g, xref0)
        t_comp[iframe] = time.time() - start2

    t_all_frame = time.time() - start1
    t_comp_final = np.mean(t_comp)

    u_size = pickle_size(u)
    xref0_size = pickle_size(xref0)
    g_size = pickle_size(g)
    results_size = pickle_size(results)

    if store_config:
        store = get_or_create_store(store_config)
        results = store.proxy(results)

    end_block_rmsd = time.time()
    block_rmsd_time = end_block_rmsd - start_block_rmsd

    return results, t_comp_final, t_all_frame, block_rmsd_time, u_size, xref0_size, g_size, results_size

def com_parallel_dask(ag, n_blocks, client, store=None):
    ref0 = ag.universe.select_atoms("protein")
    bsize = int(np.ceil(ag.universe.trajectory.n_frames / float(n_blocks)))

    blocks = []
    agsize = pickle_size(ag)
    ref0size = pickle_size(ref0)

    for iblock in range(n_blocks):
        start, stop, step = iblock * bsize, (iblock + 1) * bsize, 1
        if store:
            agprox = store.proxy(ag)
            ref0prox = store.proxy(ref0)
            store_config = store.config()
            out = delayed(block_rmsd)(agprox, ref0prox, start=start, stop=stop, step=step, store_config=store_config)
        else:
            out = delayed(block_rmsd)(ag, ref0, start=start, stop=stop, step=step)
        blocks.append(out)

    blockssize = pickle_size(blocks)

    start_client_compute = time.time()
    output = client.compute(blocks, sync=True)
    end_client_compute = time.time()
    client_compute_time = end_client_compute - start_client_compute

    start_calculations = time.time()
    results = np.vstack([out[0] for out in output])
    t_comp_avg = np.mean([out[1] for out in output])
    t_comp_max = np.max([out[1] for out in output])
    t_all_frame_avg = np.mean([out[2] for out in output])
    t_all_frame_max = np.max([out[2] for out in output])
    pickle_size_result = pickle_size(results)
    block_rmsd_times = [out[3] for out in output]
    u_sizes = [out[4] for out in output]
    xref0_sizes = [out[5] for out in output]
    g_sizes = [out[6] for out in output]
    results_sizes = [out[7] for out in output]
    end_calculations = time.time()
    calculations_time = end_calculations - start_calculations

    return results, t_comp_avg, t_comp_max, t_all_frame_avg, t_all_frame_max, pickle_size_result, blockssize, block_rmsd_times, client_compute_time, calculations_time, agsize, ref0size, u_sizes, xref0_sizes, g_sizes, results_sizes

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run MDAnalysis benchmarks with Dask and ProxyStore.')
    parser.add_argument('--traj_size', type=int, default=300, help='Total size of the trajectory.')
    parser.add_argument('--block_size', type=int, default=multiprocessing.cpu_count(), help='Size of each block.')
    parser.add_argument('--iterations', type=int, default=1, help='Number of iterations.')
    parser.add_argument('--num_traj_sizes', type=int, default=3, help='Number of trajectory sizes.')
    parser.add_argument('--proxystore', action='store_true', help='Enable ProxyStore usage.')

    args = parser.parse_args()

    traj_size = args.traj_size
    block_size = args.block_size
    iterations = args.iterations
    num_traj_sizes = args.num_traj_sizes
    use_proxystore = args.proxystore

    traj_sizes = [(i + 1) * traj_size // num_traj_sizes for i in range(num_traj_sizes)]

    PSF, DCD1 = ["adk4AKE.psf", "1ake_007-nowater-core-dt240ps.dcd"]

    num_cores = multiprocessing.cpu_count()
    client = Client(n_workers=num_cores)
    dask.config.set(scheduler='distributed')

    store = None
    if use_proxystore:
        store = Store(
            name='dask',
            connector=FileConnector('/tmp/proxystore-cache'),
            populate_target=True,
            register=True,
        )

    all_runtimes = []
    all_pickle_sizes = {
        'u_sizes': [],
        'xref0_sizes': [],
        'g_sizes': [],
        'results_sizes': []
    }
    all_block_rmsd_times = []

    with open('data_PS.txt', 'w') as file:
        with performance_report(filename="report_PS.html"):
            for k in traj_sizes:
                # Creating the universe for doing benchmark
                start_time_u1 = time.time()
                u1 = mda.Universe(PSF, DCD1)
                end_time_u1 = time.time()

                longDCD = 'newtraj.dcd'

                # Creating big trajectory sizes from initial trajectory (DCD file)
                start_time_writer = time.time()
                with mda.Writer(longDCD, u1.atoms.n_atoms) as W:
                    for i in range(k):
                        for ts in u1.trajectory:
                            W.write(u1)
                end_time_writer = time.time()

                # Creating the universe before the for loop
                start_time_u2_before = time.time()
                u = mda.Universe(PSF, longDCD)
                end_time_u2_before = time.time()

                # Doing benchmarks
                ii = 2
                for j in range(iterations):  # number of iterations
                    start_time_u2 = time.time()
                    longDCD1 = 'newtraj{}.dcd'.format(ii)
                    copyfile(longDCD, longDCD1)
                    u = mda.Universe(PSF, longDCD1)
                    end_time_u2 = time.time()

                    start_time_select_atoms = time.time()
                    mobile = u.select_atoms("(resid 1:29 or resid 60:121 or resid 160:214) and name CA")
                    end_time_select_atoms = time.time()

                    # Time the execution of com_parallel_dask
                    start_time_com_parallel_dask = time.time()
                    results_proxy, t_comp_avg, t_comp_max, t_all_frame_avg, t_all_frame_max, pickle_size_result, blocks_size, block_rmsd_times, client_compute_time, calculations_time, agsize, ref0size, u_sizes, xref0_sizes, g_sizes, results_sizes = com_parallel_dask(mobile, block_size, client, store)
                    end_time_com_parallel_dask = time.time()
                    total_com_parallel_dask_time = end_time_com_parallel_dask - start_time_com_parallel_dask

                    # Track statistics
                    runtimes = {
                        'create_universe_u1': end_time_u1 - start_time_u1,
                        'create_universe_u_before_loop': end_time_u2_before - start_time_u2_before,
                        'create_universe_u': end_time_u2 - start_time_u2,
                        'select_atoms': end_time_select_atoms - start_time_select_atoms,
                        'write_trajectory': end_time_writer - start_time_writer,
                        'com_parallel_dask_total': total_com_parallel_dask_time,
                        'client_compute_time': client_compute_time,
                        'calculations_time': calculations_time,
                    }

                    pickle_sizes = {
                        'ag': agsize,
                        'ref0': ref0size,
                        'blocks_size': blocks_size,
                    }

                    # Write results to file
                    start_time_file_write = time.time()
                    file.write(f"XTC{k} {block_size} {j} {t_comp_avg} {t_comp_max} {t_all_frame_avg} {t_all_frame_max} {total_com_parallel_dask_time} {pickle_size_result} {blocks_size} {runtimes} {pickle_sizes}\n")
                    file.flush()
                    os.remove(longDCD1)
                    end_time_file_write = time.time()

                    runtimes['file_write'] = end_time_file_write - start_time_file_write

                    # Aggregate results for statistical calculations
                    all_runtimes.append(runtimes)
                    all_pickle_sizes['u_sizes'].extend(u_sizes)
                    all_pickle_sizes['xref0_sizes'].extend(xref0_sizes)
                    all_pickle_sizes['g_sizes'].extend(g_sizes)
                    all_pickle_sizes['results_sizes'].extend(results_sizes)
                    all_block_rmsd_times.extend(block_rmsd_times)
                    ii += 1

    # Calculate statistics using numpy
    runtime_stats = {key: {
        'min': np.min([run[key] for run in all_runtimes]),
        'max': np.max([run[key] for run in all_runtimes]),
        'mean': np.mean([run[key] for run in all_runtimes]),
        'stdev': np.std([run[key] for run in all_runtimes], ddof=1)
    } for key in all_runtimes[0]}

    pickle_size_stats = {key: {
        'min': np.min(all_pickle_sizes[key]),
        'max': np.max(all_pickle_sizes[key]),
        'mean': np.mean(all_pickle_sizes[key]),
        'stdev': np.std(all_pickle_sizes[key], ddof=1)
    } for key in all_pickle_sizes}

    block_rmsd_stats = {
        'min': np.min(all_block_rmsd_times),
        'max': np.max(all_block_rmsd_times),
        'mean': np.mean(all_block_rmsd_times),
        'stdev': np.std(all_block_rmsd_times, ddof=1)
    }

    with open('stats.txt', 'w') as stats_file:
        stats_file.write(f'Runtime statistics: {runtime_stats}\n')
        stats_file.write(f'Pickle size statistics: {pickle_size_stats}\n')
        stats_file.write(f'Block RMSD statistics: {block_rmsd_stats}\n')

    client.close()
