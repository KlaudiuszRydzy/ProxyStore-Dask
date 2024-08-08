import re
import matplotlib.pyplot as plt

# Function to parse the log file
def parse_log(file_path):
    input_sizes = []
    avg_times = []
    std_devs = []
    
    input_size_pattern = re.compile(r"Starting run config .* input_size_bytes=(\d+)")
    avg_time_pattern = re.compile(r"Average run time: (\d+\.\d+) ± (\d+\.\d+)s")
    
    with open(file_path, 'r') as file:
        for line in file:
            input_size_match = input_size_pattern.search(line)
            avg_time_match = avg_time_pattern.search(line)
            
            if input_size_match:
                input_size = int(input_size_match.group(1))
                input_sizes.append(input_size)
            
            if avg_time_match:
                avg_time = float(avg_time_match.group(1))
                std_dev = float(avg_time_match.group(2))
                avg_times.append(avg_time)
                std_devs.append(std_dev)
    
    return input_sizes, avg_times, std_devs

if __name__ == "__main__":
    # Parse the log files
    input_sizes_dask, avg_times_dask, std_devs_dask = parse_log('log.txt')
    input_sizes_numpy, avg_times_numpy, std_devs_numpy = parse_log('lognumpy.txt')

    # Convert input sizes to a more readable format (e.g., MB)
    input_sizes_mb_dask = [size / (1024 * 1024) for size in input_sizes_dask]
    input_sizes_mb_numpy = [size / (1024 * 1024) for size in input_sizes_numpy]

    # Create the plot
    fig, ax = plt.subplots()

    # Plotting the Dask data
    ax.errorbar(input_sizes_mb_dask, avg_times_dask, yerr=std_devs_dask, label='Dask no-op Task Roundtrip time in seconds', color='orange', linestyle='-')
    ax.fill_between(input_sizes_mb_dask, 
                    [avg - std for avg, std in zip(avg_times_dask, std_devs_dask)], 
                    [avg + std for avg, std in zip(avg_times_dask, std_devs_dask)], 
                    color='orange', alpha=0.1)

    # Plotting the NumPy data
    ax.errorbar(input_sizes_mb_numpy, avg_times_numpy, yerr=std_devs_numpy, label='NumPy Roundtrip time in seconds', color='blue', linestyle='-')
    ax.fill_between(input_sizes_mb_numpy, 
                    [avg - std for avg, std in zip(avg_times_numpy, std_devs_numpy)], 
                    [avg + std for avg, std in zip(avg_times_numpy, std_devs_numpy)], 
                    color='blue', alpha=0.1)

    # Set logarithmic scale for the x-axis
    ax.set_xscale('log')

    # Add labels and title
    ax.set_xlabel('Input Size (MB)')
    ax.set_ylabel('Average Run Time (s)')
    ax.set_title('Dask No-Op Task Roundtrip Time vs. NumPy Roundtrip Time')

    # Customize the x-ticks
    labels = ["1 KB", "10 KB", "100 KB", "1 MB", "10 MB", "100 MB", "1 GB"]
    sizes = [1 * 1024, 10 * 1024, 100 * 1024, 1 * 1024**2, 10 * 1024**2, 100 * 1024**2, 1 * 1024**3]
    ax.set_xticks(sizes)
    ax.set_xticklabels(labels)

    # Add legend
    ax.legend()

    # Save the plot as a PNG file
    plt.savefig('dask_vs_numpy_roundtrip_time.png')

