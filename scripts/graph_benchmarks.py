import json
import numpy as np
import matplotlib.pyplot as plt


# Function to load JSON data from a file
def load_data(filename):
    with open(filename, 'r') as f:
        return json.load(f)


# Function to calculate Gbps and std deviation
def calculate_gbps(data):
    results = {}
    for entry in data:
        dtype = entry['type']
        size_bytes = entry['size']
        size_bits = size_bytes * 8

        avg_ser_time = entry['avg_ser_time']
        std_ser_time = entry['std_ser_time']
        avg_des_time = entry['avg_des_time']
        std_des_time = entry['std_des_time']

        gbps_ser = size_bits / avg_ser_time / 1e9
        gbps_des = size_bits / avg_des_time / 1e9

        std_gbps_ser = gbps_ser - (size_bits / (avg_ser_time + std_ser_time) / 1e9)
        std_gbps_des = gbps_des - (size_bits / (avg_des_time + std_des_time) / 1e9)

        if dtype not in results:
            results[dtype] = {
                'sizes': [],
                'gbps_ser': [],
                'gbps_des': [],
                'std_ser': [],
                'std_des': []
            }

        results[dtype]['sizes'].append(size_bytes)
        results[dtype]['gbps_ser'].append(gbps_ser)
        results[dtype]['gbps_des'].append(gbps_des)
        results[dtype]['std_ser'].append(std_gbps_ser)
        results[dtype]['std_des'].append(std_gbps_des)

    return results


# Load the data from JSON files
dask_data = load_data('benchmark_results.json')
ps_data = load_data('benchmark_results_PS.json')

# Calculate Gbps and std deviation for both datasets
dask_results = calculate_gbps(dask_data)
ps_results = calculate_gbps(ps_data)

# Define data sizes and types for subplots
data_sizes = ["1KB", "10KB", "100KB", "1MB", "10MB", "100MB", "1GB"]
data_types = list(dask_results.keys())

# Plot the data in a 2x3 grid
fig, axs = plt.subplots(2, 3, figsize=(12, 8))

# Iterate over the first 5 data types and plot the graphs
for i, dtype in enumerate(data_types):
    ax = axs[i // 3, i % 3]

    dask_sizes = dask_results[dtype]['sizes']
    ps_sizes = ps_results[dtype]['sizes']

    dask_gbps_ser = dask_results[dtype]['gbps_ser']
    dask_gbps_des = dask_results[dtype]['gbps_des']
    dask_std_ser = dask_results[dtype]['std_ser']
    dask_std_des = dask_results[dtype]['std_des']

    ps_gbps_ser = ps_results[dtype]['gbps_ser']
    ps_gbps_des = ps_results[dtype]['gbps_des']
    ps_std_ser = ps_results[dtype]['std_ser']
    ps_std_des = ps_results[dtype]['std_des']

    # Plot Dask data
    ax.errorbar(dask_sizes, dask_gbps_ser, yerr=dask_std_ser, fmt='-', color='orange', label='Dask Serialization',
                capsize=5)
    ax.errorbar(dask_sizes, dask_gbps_des, yerr=dask_std_des, fmt='--', color='orange', label='Dask Deserialization',
                capsize=5)

    # Plot ProxyStore data
    ax.errorbar(ps_sizes, ps_gbps_ser, yerr=ps_std_ser, fmt='-', color='purple', label='PS Serialization', capsize=5)
    ax.errorbar(ps_sizes, ps_gbps_des, yerr=ps_std_des, fmt='--', color='purple', label='PS Deserialization', capsize=5)

    ax.set_xscale('log')
    ax.set_yscale('log')  # Set y-axis to log scale
    ax.set_xticks([1024, 10240, 102400, 1048576, 10485760, 104857600, 1073741824])
    ax.set_xticklabels(data_sizes)
    ax.set_ylabel('Gigabits per Second')
    ax.set_title(f'{dtype}')

# Hide the last subplot (bottom right)
axs[1, 2].axis('off')

# Set common labels
fig.suptitle('Serialization and Deserialization Performance Comparison', fontsize=16)
plt.xlabel('Data Size')

# Add legend to the first subplot
axs[0, 0].legend(loc='upper right')

# Adjust layout and show the plot
plt.tight_layout(rect=[0, 0.03, 1, 0.95])
plt.show()
# plt.savefig('serialization_deserialization_comparison.png')

