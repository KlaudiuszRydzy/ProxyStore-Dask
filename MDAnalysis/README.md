# ProxyStore-Dask Benchmark

## Overview

This repository benchmarks the performance of MDAnalysis applications using Dask and ProxyStore.

## Setup

### Conda

```bash
conda env create -f ProxyStore-Dask/MDAnalysis/environment.yml
conda activate mdanalysis
```
# Example benchmark run
python MDAnalysis/scripts-DCD/RMSD-dask-dcd.py --n_workers 4 --threads_per_worker 2 --data_size 1000 --chunk_size 10

--n_workers: Number of Dask workers.

--threads_per_worker: Number of threads per Dask worker.

--data_size: Number of frames to use from the dataset.

--chunk_size: Size of chunks for parallel processing.

Results will be saved in a data.txt file.

The format of this will be:

XTC<trajectory_size> <block_size> <t_comp_avg> <t_comp_max> t_all_frame_avg> <t_all_frame_ma> <tot_time>

# For example, the line:

XTC50 1 1 4.590783735320367e-05 4.590783735320367e-05 28.090301513671875 28.090301513671875 29.29596996307373

# can be broken down as:

XTC50 1: Trajectory size 50 and block size 1.

1: First iteration.

4.590783735320367e-05: Average computation time per frame.

4.590783735320367e-05: Maximum computation time per frame.

28.090301513671875: Average total time for all frames.

28.090301513671875: Maximum total time for all frames.

29.29596996307373: Total computation time.
