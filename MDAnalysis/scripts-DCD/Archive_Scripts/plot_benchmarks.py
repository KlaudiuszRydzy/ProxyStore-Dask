#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import os

def read_data(file_path):
    columns = ["traj_size", "block_size", "iteration", "t_comp_avg", "t_comp_max", "t_all_frame_avg", "t_all_frame_max", "tot_time", "pickle_size_result", "block_input_size"]
    data = pd.read_csv(file_path, sep=" ", header=None, names=columns)
    
    return data

def plot_completion_time_vs_input_pickle_size(data, output_path):
    plt.figure(figsize=(10, 6))
    for traj_size in data['traj_size'].unique():
        subset = data[data['traj_size'] == traj_size]
        plt.plot(subset['block_input_size'], subset['tot_time'], label=f'Trajectory size: {traj_size}', marker='o')
    
    plt.xlabel('Input Pickle Size (bytes)')
    plt.ylabel('Total Completion Time (seconds)')
    plt.title('Total Completion Time vs Input Pickle Size')
    plt.legend()
    plt.grid(True)
    plt.savefig(os.path.join(output_path, "completion_time_vs_input_pickle_size.png"))
    plt.close()

def plot_completion_time_vs_output_pickle_size(data, output_path):
    plt.figure(figsize=(10, 6))
    for block_size in data['block_size'].unique():
        subset = data[data['block_size'] == block_size]
        plt.plot(subset['pickle_size_result'], subset['tot_time'], label=f'Block size: {block_size}', marker='o')
    
    plt.xlabel('Output Pickle Size (bytes)')
    plt.ylabel('Total Completion Time (seconds)')
    plt.title('Total Completion Time vs Output Pickle Size')
    plt.legend()
    plt.grid(True)
    plt.savefig(os.path.join(output_path, "completion_time_vs_output_pickle_size.png"))
    plt.close()

if __name__ == "__main__":
    file_path = 'data.txt'  # Path to your data file
    output_path = '.'  # Current directory
    data = read_data(file_path)
    
    plot_completion_time_vs_input_pickle_size(data, output_path)
    plot_completion_time_vs_output_pickle_size(data, output_path)
