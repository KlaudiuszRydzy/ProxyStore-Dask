import argparse
import subprocess
import os
import shutil

def run_script(traj_size, block_size, iterations, num_traj_sizes, master_iterations):
    script_name = 'testScript.py'  # Replace with the name of your existing script

    proxystore_dir = 'results_proxystore'
    non_proxystore_dir = 'results_non_proxystore'

    # Create directories if they do not exist
    os.makedirs(proxystore_dir, exist_ok=True)
    os.makedirs(non_proxystore_dir, exist_ok=True)

    for master_iter in range(master_iterations):
        print(f"Master Iteration {master_iter+1}/{master_iterations}")

        # Run with ProxyStore
        print(f"Running with ProxyStore")
        command = f'python {script_name} --traj_size {traj_size} --block_size {block_size} --iterations {iterations} --num_traj_sizes {num_traj_sizes} --proxystore'
        subprocess.run(command, shell=True)

        # Move result files to ProxyStore directory
        for file in os.listdir('.'):
            if file.startswith('data_PS') or file.startswith('report_PS') or file.startswith('stats_PS'):
                shutil.move(file, os.path.join(proxystore_dir, file))

        # Clean up the resultant newtraj.dcd files after each master iteration
        for file in os.listdir('.'):
            if file.startswith('newtraj') and file.endswith('.dcd'):
                os.remove(file)

        # Run without ProxyStore
        print(f"Running without ProxyStore")
        command = f'python {script_name} --traj_size {traj_size} --block_size {block_size} --iterations {iterations} --num_traj_sizes {num_traj_sizes}'
        subprocess.run(command, shell=True)

        # Move result files to non-ProxyStore directory
        for file in os.listdir('.'):
            if file.startswith('data') or file.startswith('report') or file.startswith('stats'):
                shutil.move(file, os.path.join(non_proxystore_dir, file))

        # Clean up the resultant newtraj.dcd files after each master iteration
        for file in os.listdir('.'):
            if file.startswith('newtraj') and file.endswith('.dcd'):
                os.remove(file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run multiple instances of the existing script with and without ProxyStore.')
    parser.add_argument('--traj_size', type=int, required=True, help='Total size of the trajectory.')
    parser.add_argument('--block_size', type=int, required=True, help='Size of each block.')
    parser.add_argument('--iterations', type=int, required=True, help='Number of iterations for each run.')
    parser.add_argument('--num_traj_sizes', type=int, required=True, help='Number of trajectory sizes.')
    parser.add_argument('--master_iterations', type=int, required=True, help='Number of master iterations.')

    args = parser.parse_args()

    run_script(args.traj_size, args.block_size, args.iterations, args.num_traj_sizes, args.master_iterations)
