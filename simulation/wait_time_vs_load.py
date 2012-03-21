""" Debugs anomalous performance by plotting the wait time for tasks. """
import simulation
import subprocess

def wait_time_vs_load(file_prefix, utilization, num_servers, tasks_per_job,
                       probes_ratio):
    """ This debugs anomalous performance by plotting wait time.
    
    The output files plot a CDF of wait time for each load (as returned by
    the probe to the server).
    """
    task_length = 100
    arrival_delay = (task_length * tasks_per_job /
                     (num_servers * utilization))
    network_delay = 1
    total_time = 1e4
    simulation.main(["job_arrival_delay=%f" % arrival_delay,
                     "num_users=1",
                     "network_delay=%d" % network_delay,
                     "probes_ratio=%f" % probes_ratio,
                     "task_length_distribution=constant",
                     "num_tasks=%d" % tasks_per_job,
                     "task_length=%d" % task_length,
                     "task_distribution=constant",
                     "load_metric=total",
                     "file_prefix=%s" % file_prefix,
                     "num_servers=%d" % num_servers,
                     "total_time=%d" % total_time,
                     "first_time=True",
                     "record_task_info=True"])
    for job_or_task in ["job", "task"]:
        output_prefix = "%s_%s" % (file_prefix, job_or_task)
        data_filename = "raw_results/%s_wait_cdf" % output_prefix
        # Figure out how many load lines to plot.
        data_file = open(data_filename, "r")
        items = data_file.readline().split("\t")
        num_load_values = len(items) - 2
        data_file.close()
    
        filename = "plot_%s.gp" % output_prefix
        gnuplot_file = open(filename, 'w')
        gnuplot_file.write("set terminal postscript color 'Helvetica' 14\n")
        gnuplot_file.write("set size 0.5,0.5\n")
        gnuplot_file.write("set output 'graphs/%s.ps'\n" % output_prefix)
        gnuplot_file.write("set xlabel 'Task Wait Time (ms)'\n")
        gnuplot_file.write("set ylabel 'Cumulative Probability'\n")
        gnuplot_file.write("set grid ytics\n")
        gnuplot_file.write("plot ")
        for load in range(num_load_values):
            if load != 0:
                gnuplot_file.write(", \\\n")
            gnuplot_file.write("'%s' using %d:1 with l lw 4 title 'Load=%d'"
                               % (data_filename, load+2, load))
        gnuplot_file.close()
        
        subprocess.call(["gnuplot", filename])

def main():
    # Fill this in with whatever experiment should be run.
    wait_time_vs_load("step_high", 0.8, 10000, 200, 1.5)

if __name__ == '__main__':
    main()
