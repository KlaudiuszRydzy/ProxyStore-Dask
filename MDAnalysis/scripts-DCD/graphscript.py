import os
import re
import matplotlib.pyplot as plt

def parse_stats_file(filepath):
    with open(filepath, 'r') as file:
        content = file.read()

    finegrained_input_diff = []
    finegrained_output_diff = []
    iteration_input_diffs = re.findall(r'Fine-grained input time difference: ([\d\.]+)', content)
    iteration_output_diffs = re.findall(r'Fine-grained output time difference: ([\d\.]+)', content)

    for diff in iteration_input_diffs:
        finegrained_input_diff.append(float(diff))
    for diff in iteration_output_diffs:
        finegrained_output_diff.append(float(diff))

    return {
        'input_mean': sum(finegrained_input_diff) / len(finegrained_input_diff),
        'input_stdev': (sum([(x - sum(finegrained_input_diff) / len(finegrained_input_diff))**2 for x in finegrained_input_diff]) / len(finegrained_input_diff))**0.5,
        'output_mean': sum(finegrained_output_diff) / len(finegrained_output_diff),
        'output_stdev': (sum([(x - sum(finegrained_output_diff) / len(finegrained_output_diff))**2 for x in finegrained_output_diff]) / len(finegrained_output_diff))**0.5
    }

def plot_graph(block_sizes, proxystore_stats, non_proxystore_stats, metric, ylabel, filename):
    metric_mean = f'{metric}_mean'
    metric_stdev = f'{metric}_stdev'

    plt.figure(figsize=(10, 6))
    plt.errorbar(block_sizes, [proxystore_stats[bs][metric_mean] for bs in block_sizes], 
                 yerr=[proxystore_stats[bs][metric_stdev] for bs in block_sizes], 
                 label='ProxyStore', color='purple', linestyle='None', marker='^')
    plt.errorbar(block_sizes, [non_proxystore_stats[bs][metric_mean] for bs in block_sizes], 
                 yerr=[non_proxystore_stats[bs][metric_stdev] for bs in block_sizes], 
                 label='Non-ProxyStore', color='orange', linestyle='None', marker='^')

    plt.xlabel('Block Size')
    plt.ylabel(ylabel)
    plt.title(f'{ylabel} vs Block Size')
    plt.legend()
    plt.grid(True)
    plt.savefig(filename)
    plt.close()

def main():
    block_sizes = [1, 8, 16]
    proxystore_stats = {}
    non_proxystore_stats = {}

    for block_size in block_sizes:
        proxystore_filepath = f'results_proxystore/stats_PS_{block_size}.txt'
        non_proxystore_filepath = f'results_non_proxystore/stats_{block_size}.txt'

        proxystore_stats[block_size] = parse_stats_file(proxystore_filepath)
        non_proxystore_stats[block_size] = parse_stats_file(non_proxystore_filepath)

    if not os.path.exists('graphs'):
        os.makedirs('graphs')

    plot_graph(block_sizes, proxystore_stats, non_proxystore_stats, 'input', 'Fine-grained Input Time Difference (s)', 'graphs/finegrained_input_time_difference.png')
    plot_graph(block_sizes, proxystore_stats, non_proxystore_stats, 'output', 'Fine-grained Output Time Difference (s)', 'graphs/finegrained_output_time_difference.png')

if __name__ == "__main__":
    main()
