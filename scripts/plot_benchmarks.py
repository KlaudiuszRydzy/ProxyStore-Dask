import matplotlib.pyplot as plt
import json

def create_plots(old_data, new_data, sizes, labels):
    types = ['Python List', 'NumPy Array', 'Pandas DataFrame', 'Polars DataFrame']

    for obj in types:
        old_ser_means = [entry['avg_ser_time'] for entry in old_data if entry['type'] == obj]
        old_ser_stds = [entry['std_ser_time'] for entry in old_data if entry['type'] == obj]
        new_ser_means = [entry['avg_ser_time'] for entry in new_data if entry['type'] == obj]
        new_ser_stds = [entry['std_ser_time'] for entry in new_data if entry['type'] == obj]

        old_des_means = [entry['avg_des_time'] for entry in old_data if entry['type'] == obj]
        old_des_stds = [entry['std_des_time'] for entry in old_data if entry['type'] == obj]
        new_des_means = [entry['avg_des_time'] for entry in new_data if entry['type'] == obj]
        new_des_stds = [entry['std_des_time'] for entry in new_data if entry['type'] == obj]

        # Serialization plot
        plt.figure()
        plt.errorbar(labels, old_ser_means, yerr=old_ser_stds, label='Old Serialization', fmt='-o')
        plt.errorbar(labels, new_ser_means, yerr=new_ser_stds, label='New Serialization', fmt='-o')
        plt.fill_between(labels, 
                         [m-s for m, s in zip(old_ser_means, old_ser_stds)], 
                         [m+s for m, s in zip(old_ser_means, old_ser_stds)], 
                         alpha=0.1)
        plt.fill_between(labels, 
                         [m-s for m, s in zip(new_ser_means, new_ser_stds)], 
                         [m+s for m, s in zip(new_ser_means, new_ser_stds)], 
                         alpha=0.1)
        plt.xlabel('Size')
        plt.ylabel('Time (seconds)')
        plt.title(f'{obj} Serialization Time')
        plt.legend()
        plt.savefig(f'{obj.replace(" ", "_").lower()}_serialization_plot.png')

        # Deserialization plot
        plt.figure()
        plt.errorbar(labels, old_des_means, yerr=old_des_stds, label='Old Deserialization', fmt='-o')
        plt.errorbar(labels, new_des_means, yerr=new_des_stds, label='New Deserialization', fmt='-o')
        plt.fill_between(labels, 
                         [m-s for m, s in zip(old_des_means, old_des_stds)], 
                         [m+s for m, s in zip(old_des_means, old_des_stds)], 
                         alpha=0.1)
        plt.fill_between(labels, 
                         [m-s for m, s in zip(new_des_means, new_des_stds)], 
                         [m+s for m, s in zip(new_des_means, new_des_stds)], 
                         alpha=0.1)
        plt.xlabel('Size')
        plt.ylabel('Time (seconds)')
        plt.title(f'{obj} Deserialization Time')
        plt.legend()
        plt.savefig(f'{obj.replace(" ", "_").lower()}_deserialization_plot.png')

if __name__ == "__main__":
    with open('benchmark_results_old.json', 'r') as f:
        old_data = json.load(f)
    
    with open('benchmark_results_new.json', 'r') as f:
        new_data = json.load(f)
    
    sizes = [1024, 10240, 102400, 1048576, 10485760, 104857600, 1073741824]
    labels = ["1KB", "10KB", "100KB", "1MB", "10MB", "100MB", "1GB"]
    
    create_plots(old_data, new_data, sizes, labels)
