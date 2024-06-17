#!/usr/bin/env python
from __future__ import print_function, division
import numpy as np
import MDAnalysis as mda
from MDAnalysis import Universe, Writer
from MDAnalysis.analysis import rms
from dask.distributed import Client
from dask import delayed
import time
from shutil import copyfile
import os
import argparse

def calculate_rmsd(mobile, ref0):
    """Calculate RMSD using MDAnalysis' RMSD class."""
    rmsd_calc = rms.RMSD(mobile, ref0)
    rmsd_calc.run()
    return rmsd_calc.results.rmsd[:, 2]  # returning the RMSD values

def block_rmsd(index, topology, trajectory, ref0_selection, start=None, stop=None, step=None):
    clone = mda.Universe(topology, trajectory)
    g = clone.atoms[index]
    ref0 = clone.select_atoms(ref0_selection)  # Ensure ref0 is selected from the cloned universe

    bsize = stop - start
    results = np.zeros([bsize, 2])
    t_comp = np.zeros(bsize)

    start1 = time.time()
    for iframe, ts in enumerate(clone.trajectory[start:stop:step]):
        start2 = time.time()
        results[iframe, :] = ts.time, calculate_rmsd(g, ref0)[iframe]
        t_comp[iframe] = time.time() - start2

    t_all_frame = time.time() - start1
    t_comp_final = np.mean(t_comp)

    return results, t_comp_final, t_all_frame

def com_parallel_dask(ag, n_blocks):
    topology, trajectory = ag.universe.filename, ag.universe.trajectory.filename
    ref0_selection = "(resid 1:29 or resid 60:121 or resid 160:214) and name CA"  # Use the same selection for ref0
    bsize = int(np.ceil(ag.universe.trajectory.n_frames / float(n_blocks)))
    print("Setting up {} blocks with {} frames each".format(n_blocks, bsize))

    blocks = []
    t_comp = []
    t_all_frame = []
    for iblock in range(n_blocks):
        start, stop, step = iblock * bsize, (iblock + 1) * bsize, 1
        print("Dask setting up block trajectory[{}:{}]".format(start, stop))

        out = delayed(block_rmsd, pure=True)(ag.indices, topology, trajectory, ref0_selection, start=start, stop=stop, step=step)
        blocks.append(out[0])
        t_comp.append(out[1])
        t_all_frame.append(out[2])

    total = delayed(np.vstack)(blocks)
    t_comp_avg = delayed(np.sum)(t_comp) / n_blocks
    t_comp_max = delayed(np.max)(t_comp)
    t_all_frame_avg = delayed(np.sum)(t_all_frame) / n_blocks
    t_all_frame_max = delayed(np.max)(t_all_frame)

    return total, t_comp_avg, t_comp_max, t_all_frame_avg, t_all_frame_max

def main(n_workers):
    # Initialize Dask client
    client = Client(n_workers=n_workers)
    print(f"Dask client initialized with {n_workers} workers.")

    PSF = "adk4AKE.psf"
    DCD1 = "1ake_007-nowater-core-dt240ps.dcd"

    with open('data.txt', mode='w') as file:
        traj_size = [50, 150, 300]
        for k in traj_size:  # We have 3 trajectory sizes
            # Creating the universe for doing benchmark
            u1 = mda.Universe(PSF, DCD1)
            longDCD = 'newtraj.dcd'

            # Creating big trajectory sizes from initial trajectory (DCD file)
            with mda.Writer(longDCD, u1.atoms.n_atoms) as W:
                for i in range(k):
                    for ts in u1.trajectory:
                        W.write(u1)
            u = mda.Universe(PSF, longDCD)

            # Doing benchmarks
            ii = 2
            block_size = [1, 2, 4, 6, 8, 10, 12]
            for i in block_size:  # Changing blocks
                for j in range(1, 6):  # Changing files (5 files per block size)
                    longDCD1 = 'newtraj{}.dcd'.format(ii)
                    copyfile(longDCD, longDCD1)
                    u = mda.Universe(PSF, longDCD1)
                    print(u)
                    print("Frames in trajectory {} for traj_size {}".format(u.trajectory.n_frames, k))
                    mobile = u.select_atoms("(resid 1:29 or resid 60:121 or resid 160:214) and name CA")

                    total = com_parallel_dask(mobile, i)
                    total = delayed(total)
                    start = time.time()
                    output = total.compute()
                    tot_time = time.time() - start
                    file.write(
                        "DCD {} {} {} {} {} {} {} {}\n".format(k, i, j, output[1], output[2], output[3], output[4],
                                                               tot_time))
                    file.flush()
                    os.remove('newtraj{}.dcd'.format(ii))
                    ii += 1

    # Close the Dask client
    client.close()
    print("Dask client closed.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run MDAnalysis benchmark with Dask locally.')
    parser.add_argument('--n_workers', type=int, default=4, help='Number of Dask workers')
    args = parser.parse_args()

    main(args.n_workers)
