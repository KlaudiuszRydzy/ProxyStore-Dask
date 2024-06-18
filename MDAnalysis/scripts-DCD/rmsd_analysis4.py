#!/usr/bin/env python
from __future__ import print_function, division
import numpy as np
import MDAnalysis as mda
from MDAnalysis import Universe, Writer
import dask
from dask.distributed import Client
from dask.delayed import delayed
from dask import multiprocessing
from dask.multiprocessing import get
from MDAnalysis.analysis.align import rotation_matrix
from MDAnalysis.lib.qcprot import CalcRMSDRotationalMatrix
import time
from shutil import copyfile
import glob, os

def rmsd(mobile, xref0):
    xmobile0 = mobile.positions - mobile.center_of_mass()
    return CalcRMSDRotationalMatrix(xref0.T.astype(np.float64), xmobile0.T.astype(np.float64), mobile.n_atoms, None, None)

def block_rmsd(ag, ref0, start=None, stop=None, step=None):
    u = ag.universe
    xref0 = ref0.positions - ref0.center_of_mass()

    clone = mda.Universe(u._topology, u.trajectory.filename, **u.kwargs)
    g = clone.atoms[ag.indices]
    assert u != clone

    print("block_rmsd", start, stop, step)
    print(len(clone.trajectory))
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

    return results, t_comp_final, t_all_frame

def com_parallel_dask(ag, n_blocks, client):
    ref0 = ag.universe.select_atoms("protein")

    bsize = int(np.ceil(ag.universe.trajectory.n_frames / float(n_blocks)))
    print("setting up {} blocks with {} frames each".format(n_blocks, bsize))

    blocks = []
    for iblock in range(n_blocks):
        start, stop, step = iblock * bsize, (iblock + 1) * bsize, 1
        print("dask setting up block trajectory[{}:{}]".format(start, stop))

        out = delayed(block_rmsd)(ag, ref0, start=start, stop=stop, step=step)
        blocks.append(out)

    output = client.compute(blocks, sync=True)
    results = np.vstack([out[0] for out in output])
    t_comp_avg = np.mean([out[1] for out in output])
    t_comp_max = np.max([out[1] for out in output])
    t_all_frame_avg = np.mean([out[2] for out in output])
    t_all_frame_max = np.max([out[2] for out in output])

    return results, t_comp_avg, t_comp_max, t_all_frame_avg, t_all_frame_max

if __name__ == "__main__":
    print(mda.__version__)
    PSF, DCD1 = ["adk4AKE.psf", "1ake_007-nowater-core-dt240ps.dcd"]

    num_cores = multiprocessing.cpu_count()
    client = Client(n_workers=num_cores)
    dask.config.set(scheduler='distributed')

    with open('data.txt', mode='w') as file:
        traj_size = [25, 50, 100]  # Smaller trajectory sizes for testing
        for k in traj_size:  # we have 3 trajectory sizes
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
            block_size = [1, 2, 4]  # Optional: Smaller block sizes for testing
            for i in block_size:  # changing blocks
                for j in range(1, 3):  # fewer iterations per block size for testing
                    longDCD1 = 'newtraj{}.dcd'.format(ii)
                    copyfile(longDCD, longDCD1)
                    u = mda.Universe(PSF, longDCD1)
                    print(u)
                    print("frames in trajectory {} for traj_size {}".format(u.trajectory.n_frames, k))
                    print(len(u.trajectory))
                    mobile = u.select_atoms("(resid 1:29 or resid 60:121 or resid 160:214) and name CA")

                    start = time.time()
                    results, t_comp_avg, t_comp_max, t_all_frame_avg, t_all_frame_max = com_parallel_dask(mobile, i, client)
                    tot_time = time.time() - start

                    file.write("XTC{} {} {} {} {} {} {} {}\n".format(k, i, j, t_comp_avg, t_comp_max, t_all_frame_avg, t_all_frame_max, tot_time))
                    file.flush()
                    os.remove('newtraj{}.dcd'.format(ii))
                    ii = ii + 1
    client.close()
