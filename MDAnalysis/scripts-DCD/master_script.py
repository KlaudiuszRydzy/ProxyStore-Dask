#!/usr/bin/env python
import argparse
import subprocess
import pandas as pd
import matplotlib.pyplot as plt
import os
import multiprocessing

def run_script(script, args):
    command = [
        'python', script,
        '--traj_size', str(args.traj_size),
        '--block_size', str(args.block_size),
        '--iterations', str(args.iterations),
        '--num_traj_sizes', str(args.num_traj_sizes)
    ]
    subprocess.run(command)
    # Delete newtraj.dcd files
    for f in os.listdir('.'):
        if f.startswith('newtraj') and f.endswith('.dcd'):
            os.remove(f)

def read_data(file_path):
    columns = ["traj_size", "block_size", "iteration", "t_comp_avg", "t_comp_max", "t_all_frame_avg", "t_all_frame_max", "tot_time", "pickle_size_result", "block_input_size"]
    data = pd.read_csv(file_path, sep=" ", header=None, names=columns)
    return data

def plot_graphs(data_non_ps, data_ps, output_path):
    # Plot completion time vs input pickle size
    plt.figure(figsize=(10, 6))
    for traj_size in data_non_ps['traj_size'].unique():
        subset_non_ps = data_non_ps[data_non_ps['traj_size'] == traj_size]
        subset_ps = data_ps[data_ps['traj_size'] == traj_size]
        plt.plot(subset_non_ps['block_input_size'], subset_non_ps['tot_time'], label=f'Non-PS Traj size: {traj_size}', marker='o')
        plt.plot(subset_ps['block_input_size'], subset_ps['tot_time'], label=f'PS Traj size: {traj_size}', marker='x')
    
    plt.xlabel('Input Pickle Size (bytes)')
    plt.ylabel('Total Completion Time (seconds)')
    plt.title('Total Completion Time vs Input Pickle Size')
    plt.legend()
    plt.grid(True)
    plt.savefig(os.path.join(output_path, "completion_time_vs_input_pickle_size.png"))
    plt.close()

    # Plot completion time vs output pickle size
    plt.figure(figsize=(10, 6))
    for block_size in data_non_ps['block_size'].unique():
        subset_non_ps = data_non_ps[data_non_ps['block_size'] == block_size]
        subset_ps = data_ps[data_ps['block_size'] == block_size]
        plt.plot(subset_non_ps['pickle_size_result'], subset_non_ps['tot_time'], label=f'Non-PS Block size: {block_size}', marker='o')
        plt.plot(subset_ps['pickle_size_result'], subset_ps['tot_time'], label=f'PS Block size: {block_size}', marker='x')
    
    plt.xlabel('Output Pickle Size (bytes)')
    plt.ylabel('Total Completion Time (seconds)')
    plt.title('Total Completion Time vs Output Pickle Size')
    plt.legend()
    plt.grid(True)
    plt.savefig(os.path.join(output_path, "completion_time_vs_output_pickle_size.png"))
    plt.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run MDAnalysis benchmarks with and without ProxyStore and generate comparison graphs.')
    parser.add_argument('--traj_size', type=int, default=300, help='Total size of the trajectory.')
    parser.add_argument('--block_size', type=int, default=multiprocessing.cpu_count(), help='Size of each block.')
    parser.add_argument('--iterations', type=int, default=1, help='Number of iterations.')
    parser.add_argument('--num_traj_sizes', type=int, default=3, help='Number of trajectory sizes.')

    args = parser.parse_args()

    # Run non-ProxyStore script
    run_script('rmsd_choose_PS.py', args)

    # Run ProxyStore script
    run_script('rmsd_choose.py', args)

    # Read data from both output files
    data_non_ps = read_data('data.txt')
    data_ps = read_data('data_PS.txt')

    # Plot and save graphs
    plot_graphs(data_non_ps, data_ps, '.')

