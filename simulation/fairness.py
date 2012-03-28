""" Functionality to help with creating nifty graphs to understand fairness.
"""

import simulation
import stats
import subprocess

def fairness_time(load_metric, cores_per_server):
    """ Plots the number of running tasks for each user, over time. """
    num_tasks = 10
    network_delay = 5
    probes_ratio = 2
    num_servers = 1000
    total_time = 5000
    file_prefix = "fairness"
    first = True
    utilization = 2.0
    num_users = 4
    arrival_delay = (100.0 * num_tasks / (num_servers * cores_per_server *
                                          utilization))
    simulation.main(["job_arrival_delay=%f" % arrival_delay,
                     "network_delay=%d" % network_delay,
                     "probes_ratio=%f" % probes_ratio,
                     "task_length_distribution=constant",
                     "task_distribution=constant",
                     "job_arrival_distribution=poisson",
                     "deterministic=True",
                     "file_prefix=%s" % file_prefix,
                     "num_users=%d" % num_users,
                     "cores_per_server=%d" % cores_per_server,
                     "num_servers=%d" % num_servers,
                     "num_tasks=%d" % num_tasks,
                     "total_time=%d" % total_time,
                     "load_metric=%s" % load_metric,
                     "relative_weights=1,1,1,2",
                     "relative_demands=1,1,1,1",
                     "first_time=%s" % first])
    
    gnuplot_filename = "plot_fairness.gp"
    gnuplot_file = open(gnuplot_filename, "w")
    gnuplot_file.write("set terminal postscript color\n")
    gnuplot_file.write("set output 'graphs/fairness.ps'\n")
    gnuplot_file.write("set size 0.5,0.5\n")
    #gnuplot_file.write("set xlabel 'Time (ms)'\n")
    #gnuplot_file.write("set ylabel 'Number of Running Tasks'\n")
    gnuplot_file.write("set grid ytics\n")
    gnuplot_file.write("set xrange[0:%d]\n" % total_time)
    gnuplot_file.write("plot ")

    # Write total number of running tasks.
    gnuplot_file.write(("'raw_results/%s_running_tasks' using 1:2 lt 0 lw 4 "
                        "with l notitle") % file_prefix)
    for user_id in range(num_users):
        gnuplot_file.write(",\\\n")
        gnuplot_file.write(("'raw_results/%s_running_tasks_%s' using "
                            "1:2 lt %d lw 4 with l notitle") %
                           (file_prefix, user_id, user_id + 1))
    gnuplot_file.close()

    return gnuplot_filename

def fairness_isolation(load_metric, network_delay, probes_ratio,
                       cores_per_server):
    """ This function makes a graph to look at how well we achieve isolation.
    
    We fix the usage of one user at 50% of their allocation, and increase
    the usage of the other user.
    """
    num_users = 2
    num_tasks = 10
    num_servers = 1000
    task_length = 100
    total_time = 2e4
    file_prefix = "fairness_isolation"

    capacity_tasks_per_milli = num_servers * cores_per_server / task_length
    # Demand of user 0, which has constant demand that's 1/4 of cluster
    # calacity (which is half of its share).
    constant_demand_per_milli = capacity_tasks_per_milli / 4.0
    changing_demand_per_milli = capacity_tasks_per_milli / 4.0
    first = True
    while changing_demand_per_milli < (1.5 * capacity_tasks_per_milli):
        total_demand = constant_demand_per_milli + changing_demand_per_milli
        arrival_delay = float(num_tasks) / total_demand
        simulation.main(["job_arrival_delay=%f" % arrival_delay,
                         "network_delay=%d" % network_delay,
                         "probes_ratio=%f" % probes_ratio,
                         "task_length_distribution=constant",
                         "task_distribution=constant",
                         "job_arrival_distribution=poisson",
                         "deterministic=True",
                         "file_prefix=%s" % file_prefix,
                         "num_users=%d" % num_users,
                         "num_servers=%d" % num_servers,
                         "cores_per_server=%d" % cores_per_server,
                         "num_tasks=%d" % num_tasks,
                         "task_length=%d" % task_length,
                         "total_time=%d" % total_time,
                         "load_metric=%s" % load_metric,
                         ("relative_demands=%d,%d" % 
                          (constant_demand_per_milli,
                           changing_demand_per_milli)),
                         "first_time=%s" % first])
        first = False
        changing_demand_per_milli += 0.1 * capacity_tasks_per_milli
        first = False
      
    gnuplot_filename = "plot_fairness_isolation.gp"
    gnuplot_file = open(gnuplot_filename, "w")
    gnuplot_file.write("set terminal postscript color\n")
    gnuplot_file.write("set output 'graphs/fairness_isolation.ps'\n")
    gnuplot_file.write("set size 0.5,0.5\n")
    #gnuplot_file.write("set xlabel 'Utilization'\n")
    #gnuplot_file.write("set ylabel 'Normalized Response Time (ms)'\n")
    gnuplot_file.write("set yrange [0:600]\n")
    gnuplot_file.write("set grid ytics\n")
    gnuplot_file.write("plot ")

    gnuplot_file.write(("'raw_results/%s_response_time' using "
                        "3:4 lt 0 lw 4 with l notitle,\\\n") %
                       (file_prefix))
    gnuplot_file.write(("'raw_results/%s_response_time' using "
                        "3:4:5 lt 0 lw 4 with errorbars "
                        "notitle") % (file_prefix))
    for user_id in range(num_users):
        gnuplot_file.write(",\\\n")
        gnuplot_file.write(("'raw_results/%s_response_time_%s' using "
                            "3:4 lt %d lw 4 with l notitle,\\\n") %
                           (file_prefix, user_id, user_id + 1))
        gnuplot_file.write(("'raw_results/%s_response_time_%s' using "
                            "3:4:5 lt %d lw 4 with errorbars "
                            "notitle") %
                           (file_prefix, user_id, user_id + 1))
    gnuplot_file.close()
    return gnuplot_filename


def main():
    # Fill this in with whatever experiment should be run.
    gnuplot_filename = fairness_isolation("per_user_estimate", 2, 1.5, 16)
    subprocess.call(["gnuplot", gnuplot_filename])

if __name__ == '__main__':
    main()
