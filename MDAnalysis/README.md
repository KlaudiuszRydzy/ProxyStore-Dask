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
```bash
python master_script.py --traj_size 300 --block_size 4 --iterations 2 --num_traj_sizes 3
```

Command-Line Arguments:

--traj_size: Specifies the total size of the trajectory. Default is 300.

--block_size: Specifies the size of each block. Default is the number of CPU cores.

--iterations: Specifies the number of iterations. Default is 1.

--num_traj_sizes: Specifies the number of trajectory sizes. Default is 3.


This will run a script that creates two graphs that compare the runtimes of a ProxyStore implementation, and a non-ProxyStore implementation.
