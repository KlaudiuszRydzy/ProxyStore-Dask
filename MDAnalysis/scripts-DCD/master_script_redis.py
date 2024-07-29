import argparse
import subprocess
import os
import shutil

def run_script(traj_size, block_size, iterations, num_traj_sizes, master_iterations):
    script_name = 'testScriptRedis.py'

    proxystore_redis_dir = 'results_proxystore_Redis'

    # Create directories if they do not exist
    os.makedirs(proxystore_redis_dir, exist_ok=True)

    for master_iter in range(master_iterations):
        print(f"Master Iteration {master_iter+1}/{master_iterations}")

        # Run with ProxyStore using Redis
        print(f"Running with ProxyStore using Redis")
        command = f'python {script_name} --traj_size {traj_size} --block_size {block_size} --iterations {iterations} --num_traj_sizes {num_traj_sizes} --proxystore'
        subprocess.run(command, shell=True)

    # Clean up the resultant newtraj.dcd files
    for file in os.listdir('.'):
        if file.startswith('newtraj') and file.endswith('.dcd'):
            os.remove(file)

    # Move result files to ProxyStore Redis directory
    for file in os.listdir('.'):
        if file in ['data_PS.txt', 'report_PS.html', 'stats_PS.txt']:
            shutil.move(file, os.path.join(proxystore_redis_dir, file))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run multiple instances of the existing script with ProxyStore using Redis.')
    parser.add_argument('--traj_size', type=int, required=True, help='Total size of the trajectory.')
    parser.add_argument('--block_size', type=int, required=True, help='Size of each block.')
    parser.add_argument('--iterations', type=int, required=True, help='Number of iterations for each run.')
    parser.add_argument('--num_traj_sizes', type=int, required=True, help='Number of trajectory sizes.')
    parser.add_argument('--master_iterations', type=int, required=True, help='Number of master iterations.')

    args = parser.parse_args()

    run_script(args.traj_size, args.block_size, args.iterations, args.num_traj_sizes, args.master_iterations)