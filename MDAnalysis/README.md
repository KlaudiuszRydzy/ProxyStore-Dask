# ProxyStore-Dask Benchmark

## Overview

This repository benchmarks the performance of MDAnalysis applications using Dask and ProxyStore.

## Setup

### Conda

```bash
conda env create -f MDAnalysis/environment.yml
conda activate mdanalysis
```
# Example benchmark run
python MDAnalysis/benchmark/benchmark_script.py --n_workers 4 --threads_per_worker 2 --data_size 1000 --chunk_size 10

--n_workers: Number of Dask workers.

--threads_per_worker: Number of threads per Dask worker.

--data_size: Number of frames to use from the dataset.

--chunk_size: Size of chunks for parallel processing.

Results will be saved in the MDAnalysis/results directory.
