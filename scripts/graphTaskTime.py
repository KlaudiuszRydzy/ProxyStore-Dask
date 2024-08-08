import re 
import os
import json
import matplotlib.pyplot as plt
import numpy as np

# Function to extract runtimes from log files
def extract_runtimes(directory):
    runtimes = []
    for subdir in os.listdir(directory):
        path = os.path.join(directory, subdir, 'log.txt')
        if os.path.isfile(path):
            with open(path, 'r') as file:
                content = file.read()
                match = re.search(r'runtime=(\d+)', content)
                if match:
                    runtimes.append(int(match.group(1)))
    return runtimes

# Function to extract task times from tasks.jsonl files
def extract_task_times(directory):
    local_train_diffs = []
    for subdir in os.listdir(directory):
        path = os.path.join(directory, subdir, 'tasks.jsonl')
        if os.path.isfile(path):
            with open(path, 'r') as file:
                for line in file:
                    task = json.loads(line)
                    function_name = task.get("function_name")
                    submit_time = task.get("submit_time")
                    task_start_time = task.get("execution", {}).get("task_start_time")
                    if function_name == "local_train" and submit_time and task_start_time:
                        diff = task_start_time - submit_time
                        local_train_diffs.append(diff)
    return local_train_diffs

# Directories containing the log files
dask_dir = 'fedlearnDask'
ps_dir = 'fedlearnPS'

# Extract runtimes from directories
dask_runtimes = extract_runtimes(dask_dir)
ps_runtimes = extract_runtimes(ps_dir)

# Extract task times from directories
dask_local_train_diffs = extract_task_times(dask_dir)
ps_local_train_diffs = extract_task_times(ps_dir)

# Calculate average and standard deviation of runtimes
avg_dask_runtime = np.mean(dask_runtimes) if dask_runtimes else 0
std_dask_runtime = np.std(dask_runtimes) if dask_runtimes else 0

avg_ps_runtime = np.mean(ps_runtimes) if ps_runtimes else 0
std_ps_runtime = np.std(ps_runtimes) if ps_runtimes else 0

# Calculate average and standard deviation of local train task times
avg_dask_local_train = np.mean(dask_local_train_diffs) if dask_local_train_diffs else 0
std_dask_local_train = np.std(dask_local_train_diffs) if dask_local_train_diffs else 0

avg_ps_local_train = np.mean(ps_local_train_diffs) if ps_local_train_diffs else 0
std_ps_local_train = np.std(ps_local_train_diffs) if ps_local_train_diffs else 0

# Plotting average runtimes with error bars
labels_runtime = ['Dask\nwithout\nProxyStore', 'Dask\nwith\nProxyStore']
averages_runtime = [avg_dask_runtime, avg_ps_runtime]
std_devs_runtime = [std_dask_runtime, std_ps_runtime]
colors_runtime = ['orange', 'purple']

plt.figure(figsize=(7, 4))
plt.barh(labels_runtime, averages_runtime, color=colors_runtime, xerr=std_devs_runtime, capsize=5)
plt.xlabel('Average Runtime (s)')
plt.title('Federated Learning ProxyStore vs. no ProxyStore Comparison')
plt.xlim(500, max(averages_runtime) + 10)  # Set x-axis limit starting from 300

# Save the plot as a file
plt.savefig('FedLearnComparison.png')

# Plotting average local train task times with error bars
labels_task = ['Dask\nw/o PS\nLocal Train', 'Dask\nw/ PS\nLocal Train']
averages_task = [avg_dask_local_train, avg_ps_local_train]
std_devs_task = [std_dask_local_train, std_ps_local_train]
colors_task = ['orange', 'purple']

plt.figure(figsize=(7, 4))
plt.barh(labels_task, averages_task, color=colors_task, xerr=std_devs_task, capsize=5)
plt.xlabel('Average Task Time Difference (s)')
plt.title('Local Train Task Time Comparison: ProxyStore vs. no ProxyStore')

# Save the plot as a file
plt.savefig('LocalTrainTaskTimeComparison.png')

# Show the plots
plt.show()

