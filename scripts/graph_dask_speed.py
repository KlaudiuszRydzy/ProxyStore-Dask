import json
import numpy as np
import matplotlib.pyplot as plt

# Function to load JSON data from a file
def load_data(filename):
    with open(filename, 'r') as f:
        return json.load(f)

# Function to extract serialization times
def extract_serialization_times(data):
    results = {'NumPy Array': {'sizes': [], 'ser_times': [], 'std_ser': []},
               'Python List': {'sizes': [], 'ser_times': [], 'std_ser': []}}

    for entry in data:
        dtype = entry['type']
        if dtype not in results:
            continue

        size_bytes = entry['size']
        avg_ser_time = entry['avg_ser_time']
        std_ser_time = entry['std_ser_time']

        results[dtype]['sizes'].append(size_bytes)
        results[dtype]['ser_times'].append(avg_ser_time)
        results[dtype]['std_ser'].append(std_ser_time)

    return results

# Load the data from the JSON file
dask_data = load_data('benchmark_results.json')

# Extract serialization times
dask_results = extract_serialization_times(dask_data)

# Define data sizes and create the plot
data_sizes = ["1KB", "10KB", "100KB", "1MB", "10MB", "100MB", "1GB"]

fig, ax = plt.subplots(figsize=(8, 6))

# Plotting the serialization times for NumPy Array and Python List
for dtype, color in zip(['NumPy Array', 'Python List'], ['blue', 'orange']):
    sizes = dask_results[dtype]['sizes']
    ser_times = dask_results[dtype]['ser_times']
    std_ser = dask_results[dtype]['std_ser']

    ax.errorbar(sizes, ser_times, yerr=std_ser, fmt='-', color=color, label=f'{dtype} Serialization', capsize=5)

ax.set_xscale('log')
ax.set_yscale('log')
ax.set_xticks([1024, 10240, 102400, 1048576, 10485760, 104857600, 1073741824])
ax.set_xticklabels(data_sizes)
ax.set_ylabel('Average Run Time (s)')
ax.set_xlabel('Data Size')
ax.set_title('Serialization Performance for NumPy Array and Python List')

# Add legend
ax.legend(loc='upper left')

# Show the plot
plt.tight_layout()
plt.show()
# plt.savefig('serialization_comparison.png')
