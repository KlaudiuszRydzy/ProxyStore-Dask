import numpy as np
import pandas as pd
import polars as pl
import time
from proxystore.serialize import serialize, deserialize
import sys
import json

def generate_data(size):
    py_list = [0] * (size // 8)
    np_array = np.random.rand(size // 8)
    pd_df = pd.DataFrame(np.random.rand(size // 8, 1))
    pl_df = pl.DataFrame(pd_df)
    return py_list, np_array, pd_df, pl_df

def benchmark():
    sizes = [1024, 10*1024, 100*1024, 1024*1024, 10*1024*1024, 100*1024*1024, 1024*1024*1024]
    repetitions = 20
    results = []

    for size in sizes:
        print(f"\nBenchmarking size: {size} bytes")
        data_objs = generate_data(size)

        for obj_name, obj in zip(['Python List', 'NumPy Array', 'Pandas DataFrame', 'Polars DataFrame'], data_objs):
            ser_times = []
            des_times = []

            for _ in range(repetitions):
                start_time = time.time()
                serialized_data = serialize(obj)
                ser_times.append(time.time() - start_time)

                start_time = time.time()
                deserialized_data = deserialize(serialized_data)
                des_times.append(time.time() - start_time)

            avg_ser_time = sum(ser_times) / repetitions
            avg_des_time = sum(des_times) / repetitions
            std_ser_time = np.std(ser_times)
            std_des_time = np.std(des_times)
            results.append({
                'type': obj_name,
                'size': size,
                'avg_ser_time': avg_ser_time,
                'std_ser_time': std_ser_time,
                'avg_des_time': avg_des_time,
                'std_des_time': std_des_time,
            })

    with open('benchmark_results.json', 'w') as f:
        json.dump(results, f, indent=4)

if __name__ == "__main__":
    benchmark()
