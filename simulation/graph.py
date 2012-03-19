""" Functionality to help with creating nifty graphs of the results. """

import simulation
import stats
import subprocess

def evaluate_estimates():
    utilization_granularity = 20 # Number of utilization values
    avg_num_tasks = 200
    num_servers = 1000
    total_time = 1e5
    
    filename = "plot_estimates.gp"
    gnuplot_file = open(filename, 'w')
    gnuplot_file.write("set terminal postscript color\n")
    gnuplot_file.write("set output 'graphs/estimates.ps'\n")
    gnuplot_file.write("set xlabel 'Utilization'\n")
    gnuplot_file.write("set ylabel 'Response Time (in milliseconds)'\n")
    gnuplot_file.write("set grid ytics\n")
    gnuplot_file.write("plot ")

    for count, estimate_queue_length in enumerate([True, False]):
        file_prefix = "estimates_%s" % estimate_queue_length
        first = True
        for i in range(1, utilization_granularity + 1):
            arrival_delay = (100. * avg_num_tasks *
                             utilization_granularity /
                             (num_servers * i))
            simulation.main(["job_arrival_delay=%f" % arrival_delay,
                             "network_delay=10",
                             "probes_ratio=1.1",
                             "task_length_distribution=constant",
                             "task_distribution=constant",
                             "file_prefix=%s" % file_prefix,
                             "num_servers=%d" % num_servers,
                             "total_time=%d" % total_time,
                             "queue_selection=greedy",
                             "estimate_queue_length=%s" % estimate_queue_length,
                             "record_queue_state=False",
                             "first_time=%s" % first])
            first = False
        
        if count > 0:
            gnuplot_file.write(",\\\n")
        gnuplot_file.write(("'raw_results/%s_response_time' using 3:4 title "
                            "'%s, mean' lc %d lw 4 with lp,\\\n") %
                           (file_prefix, estimate_queue_length, count))
        gnuplot_file.write(("'raw_results/%s_response_time' using 3:6 title "
                            "'%s, 99%%' lc %d lw 4 with lp") %
                           (file_prefix, estimate_queue_length, count))
    
    return filename

def fairness_time(load_metric, num_users):
    num_tasks = 10
    network_delay = 5
    probes_ratio = 2
    num_servers = 1000
    total_time = 5000
    file_prefix = "fairness"
    first = True
    arrival_delay = 0.5
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

def fairness_isolation(load_metric, network_delay, probes_ratio):
    """ This function makes a graph to look at how well we achieve isolation.
    
    We fix the usage of one user at 50% of their allocation, and increase
    the usage of the other user.
    """
    num_users = 2
    num_tasks = 10
    num_servers = 1000
    task_length = 100
    total_time = 1000
    file_prefix = "fairness_isolation"

    capacity_tasks_per_milli = num_servers / task_length
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
                        "3:($4 - 3*$7) lt 0 lw 4 with l notitle,\\\n") %
                       (file_prefix))
    gnuplot_file.write(("'raw_results/%s_response_time' using "
                        "3:($4 - 3*$7):5 lt 0 lw 4 with errorbars "
                        "notitle") % (file_prefix))
    for user_id in range(num_users):
        gnuplot_file.write(",\\\n")
        gnuplot_file.write(("'raw_results/%s_response_time_%s' using "
                            "3:($4 - 3*$7) lt %d lw 4 with l notitle,\\\n") %
                           (file_prefix, user_id, user_id + 1))
        gnuplot_file.write(("'raw_results/%s_response_time_%s' using "
                            "3:($4 - 3*$7):5 lt %d lw 4 with errorbars "
                            "notitle") %
                           (file_prefix, user_id, user_id + 1))
    gnuplot_file.close()
    return gnuplot_filename

def fairness(load_metric, num_users):
    num_tasks = 10
    network_delay = 0
    probes_ratio = -1
    num_servers = 1000
    total_time = 1000
        
    first = True
    utilization_granularity = 10  # Number of different utilization values.
    file_prefix = "fairness_rt"
    for i in range(1, utilization_granularity + 1):
        arrival_delay = (100. * num_tasks * utilization_granularity /
                         (num_servers * i))
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
                         "num_tasks=%d" % num_tasks,
                         "total_time=%d" % total_time,
                         "load_metric=%s" % load_metric,
                         "first_time=%s" % first])
        first = False
      
    gnuplot_filename = "plot_fairness_rt.gp"
    gnuplot_file = open(gnuplot_filename, "w")
    gnuplot_file.write("set terminal postscript color\n")
    gnuplot_file.write("set output 'graphs/fairness_rt.ps'\n")
    gnuplot_file.write("set size 0.5,0.5\n")
    #gnuplot_file.write("set xlabel 'Utilization'\n")
    #gnuplot_file.write("set ylabel 'Normalized Response Time (ms)'\n")
    gnuplot_file.write("set yrange [:300]\n")
    gnuplot_file.write("set grid ytics\n")
    gnuplot_file.write("plot ")

    gnuplot_file.write(("'raw_results/%s_response_time' using "
                        "3:($4 - 3*$7) lt 0 lw 4 with l notitle,\\\n") %
                       (file_prefix))
    gnuplot_file.write(("'raw_results/%s_response_time' using "
                        "3:($4 - 3*$7):5 lt 0 lw 4 with errorbars "
                        "notitle") % (file_prefix))
    for user_id in range(num_users):
        gnuplot_file.write(",\\\n")
        gnuplot_file.write(("'raw_results/%s_response_time_%s' using "
                            "3:($4 - 3*$7) lt %d lw 4 with l notitle,\\\n") %
                           (file_prefix, user_id, user_id + 1))
        gnuplot_file.write(("'raw_results/%s_response_time_%s' using "
                            "3:($4 - 3*$7):5 lt %d lw 4 with errorbars "
                            "notitle") %
                           (file_prefix, user_id, user_id + 1))
    gnuplot_file.close()
    return gnuplot_filename

def debug_load_vs_wait(file_prefix, utilization, num_servers, tasks_per_job,
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
    
        
###############################################################################
#               More sophisticated graphing functionality.                    #
###############################################################################

class EffectOfNetworkDelay:
    def __init__(self):
        self.probes_ratio = 1.1
        self.delay_values = [0, 10, 20, 50, 100, 200, 0, 0]
        self.probes_ratio_values = []
        while len(self.probes_ratio_values) < len(self.delay_values) - 2:
            self.probes_ratio_values.append(self.probes_ratio)
        # 2 extra experiments to simulate random placement, and an ideal greedy
        # scheduler.
        self.probes_ratio_values.append(1.0)
        self.probes_ratio_values.append(-1.0)
        assert(len(self.probes_ratio_values) == len(self.delay_values))
        self.num_servers = 1000
        self.total_time = 1e5
        
    def get_prefix(self, trial_number, network_delay, probes_ratio):
        extra = ""
        if trial_number >= 0:
            extra = "_%d" % trial_number
            
        return "results_final_delay%s_%d_%s" % (extra, network_delay,
                                                probes_ratio)
        
    def run_single(self, trial_number):
        """ Using -1 as the trial number indicates that this is for a 
        single run, so the trial number won't be included in the filename. """
        avg_num_tasks = 200. / 6 + 5 * 10. / 6
        for network_delay, probes_ratio in zip(self.delay_values,
                                               self.probes_ratio_values):
            first = True
            utilization_granularity = 20  # Number of different utilization values.
            file_prefix = self.get_prefix(trial_number, network_delay,
                                          probes_ratio)
            for i in range(1, utilization_granularity + 1):
                arrival_delay = (100. * avg_num_tasks *
                                 utilization_granularity /
                                 (self.num_servers * i))
                simulation.main(["job_arrival_delay=%f" % arrival_delay,
                                 "network_delay=%d" % network_delay,
                                 "probes_ratio=%f" % probes_ratio,
                                 "task_length_distribution=facebook",
                                 "task_distribution=bimodal",
                                 "file_prefix=%s" % file_prefix,
                                 "num_servers=%d" % self.num_servers,
                                 "total_time=%d" % self.total_time,
                                 "first_time=%s" % first])
                first = False
        
    def graph_single(self):
        filename = "plot_single_network_delay.gp"
        gnuplot_file = open(filename, 'w')
        gnuplot_file.write("set terminal postscript color\n")
        gnuplot_file.write("set size 0.5,0.5\n")
        gnuplot_file.write("set output 'graphs/single_network_delay.ps'\n")
        gnuplot_file.write("set xlabel 'Utilization'\n")
        gnuplot_file.write("set ylabel 'Normalized Response Time (ms)'\n")
        gnuplot_file.write("set yrange [100:500]\n")
        gnuplot_file.write("set grid ytics\n")
        gnuplot_file.write("set xtics 0.25\n")
        #gnuplot_file.write("set title 'Effect of Network Delay on Response "
        #                   "Time'\n")
        gnuplot_file.write("set key font 'Helvetica,10' left samplen 2 invert "
                           "\n")
        gnuplot_file.write("plot ")
        
        for i, (network_delay, probes_ratio) in enumerate(
                zip(self.delay_values, self.probes_ratio_values)):
            
            output_filename = ("raw_results/%s_response_time" %
                               self.get_prefix(-1, network_delay, probes_ratio))
            if i > 0:
                gnuplot_file.write(', \\\n')
            title = "%dms" % network_delay
            if probes_ratio == 1.0:
                title = "Random"
            gnuplot_file.write(("'%s' using 3:($4 - 3*$7) title '%s' lt %d "
                                "lw 4 with l,\\\n") %
                               (output_filename, title, i))
            gnuplot_file.write(("'%s' using 3:($4 - 3*$7):5 notitle lt %d "
                                "lw 4 with errorbars") % (output_filename, i))
            
        subprocess.call(["gnuplot", filename])
        
    def run(self, num_trials):
        for trial in range(num_trials):
            self.run_single(trial)
            
        filename = "plot_final_network_delay.gp"
        gnuplot_file = open(filename, 'w')
        gnuplot_file.write("set terminal postscript color\n")
        gnuplot_file.write("set size 0.5,0.5\n")
        gnuplot_file.write("set output 'graphs/final_network_delay.ps'\n")
        gnuplot_file.write("set xlabel 'Utilization'\n")
        gnuplot_file.write("set ylabel 'Normalized Response Time (ms)'\n")
        gnuplot_file.write("set yrange [100:500]\n")
        gnuplot_file.write("set grid ytics\n")
        gnuplot_file.write("set xtics 0.25\n")
        #gnuplot_file.write("set title 'Effect of Network Delay on Response "
        #                   "Time'\n")
        gnuplot_file.write("set key font 'Helvetica,10' left samplen 2 invert "
                           "\n")
        gnuplot_file.write("plot ")
        
        for i, (network_delay, probes_ratio) in enumerate(
                zip(self.delay_values, self.probes_ratio_values)):
            # Aggregate results and write to a file.
            # Map of utilization to response times for that utilization.
            results = {}
            for trial in range(num_trials):
                results_filename = ("raw_results/%s_response_time" %
                                    self.get_prefix(trial, network_delay,
                                                    probes_ratio))
                results_file = open(results_filename, "r")
                index = 0
                for line in results_file:
                    values = line.split("\t")
                    if values[0] == "n":
                        continue
                    normalized_response_time = (float(values[3]) -
                                                3*float(values[6]))
                    utilization = float(values[2])
                    if utilization not in results:
                        results[utilization] = []
                    results[utilization].append(normalized_response_time)
                    
            agg_output_filename = ("raw_results/agg_final_delay_%d_%s" %
                                   (network_delay, probes_ratio))
            agg_output_file = open(agg_output_filename, "w")
            agg_output_file.write("Utilization\tResponseTime\tStdDev\n")
            for utilization in sorted(results.keys()):
                avg_response_time = stats.lmean(results[utilization])
                std_dev = stats.lstdev(results[utilization])
                agg_output_file.write("%f\t%f\t%f\n" %
                                      (utilization, avg_response_time, std_dev))
                
            # Plot aggregated results.
            if i > 0:
                gnuplot_file.write(', \\\n')
            title = "%dms" % network_delay
            if probes_ratio == 1.0:
                title = "Random"
            if probes_ratio == -1:
                title = "Ideal"
            gnuplot_file.write(("'%s' using 1:2 title '%s' lt %d lw 4 with l"
                                ",\\\n") % (agg_output_filename, title, i))
            gnuplot_file.write(("'%s' using 1:2:3 notitle lt %d lw 4 with "
                                "errorbars") % (agg_output_filename, i))
            
        subprocess.call(["gnuplot", filename])

class EffectOfProbes:
    def __init__(self, file_prefix, remove_delay=False):
        self.probes_ratio_values = [-1.0, 1.0, 1.2, 1.5]
        self.num_servers = 1000
        self.total_time = 1e4
        
        # Prefix used for all files, including plotting and aggregated data
        # files.  Note that a longer prefix is used for the results from each
        # inidividual trial.
        self.file_prefix = file_prefix
        self.remove_delay = remove_delay

    def get_prefix(self, trial_number, probes_ratio):
        extra = ""
        if trial_number >= 0:
            extra = "_%d" % trial_number
        return "%s%s_%s" % (self.file_prefix, extra, probes_ratio)
        
    def run_single(self, trial_number):
        """ Using -1 as the trial number indicates that this is for a 
        single run, so the trial number won't be included in the filename. """
        avg_num_tasks = 10.
        task_length = 100
        for probes_ratio in self.probes_ratio_values:
            network_delay = 1
            if probes_ratio == -1:
                network_delay = 0
            first = True
            # Number of different utilization values.
            utilization_granularity = 10
            file_prefix = self.get_prefix(trial_number, probes_ratio)
            for i in range(1, utilization_granularity + 1):
                arrival_delay = (task_length * avg_num_tasks *
                                 utilization_granularity /
                                 (self.num_servers * i))
                simulation.main(["job_arrival_delay=%f" % arrival_delay,
                                 "num_users=1",
                                 "network_delay=%d" % network_delay,
                                 "probes_ratio=%f" % probes_ratio,
                                 "task_length_distribution=constant",
                                 "num_tasks=%d" % avg_num_tasks,
                                 "task_length=%d" % task_length,
                                 "task_distribution=constant",
                                 "load_metric=total",
                                 "file_prefix=%s" % file_prefix,
                                 "num_servers=%d" % self.num_servers,
                                 "total_time=%d" % self.total_time,
                                 "first_time=%s" % first])
                first = False
        
    def graph_single(self):
        """ Graphs the result of a single experiment. """
        file_prefix = "%s_single" %  self.file_prefix
        filename = "plot_%s.gp" % file_prefix
        gnuplot_file = open(filename, 'w')
        gnuplot_file.write("set terminal postscript color 'Helvetica' 14\n")
        gnuplot_file.write("set size .5, .4\n")
        gnuplot_file.write("set output 'graphs/%s.ps'\n" % file_prefix)
        gnuplot_file.write("set xlabel 'Utilization'\n")
        gnuplot_file.write("set ylabel 'Job Response Time (ms)'\n")
        gnuplot_file.write("set yrange [0:500]\n")
        gnuplot_file.write("set grid ytics\n")
        gnuplot_file.write("set key at 1,700 horizontal\n")
        gnuplot_file.write("plot ")
        
        for i, probes_ratio in enumerate(self.probes_ratio_values):
            results_filename = ("raw_results/%s_response_time" %
                                self.get_prefix(-1, probes_ratio))
            if i > 0:
                gnuplot_file.write(', \\\n')
            gnuplot_file.write(("'%s' using 3:$7 title '%s' lt %d lw 4"
                                " with l,\\\n") %
                               (results_filename, probes_ratio, i))
            gnuplot_file.write(("'%s' using 3:$7:5 notitle lt %d lw 1 "
                                "with errorbars") % (results_filename, i))
            
        subprocess.call(["gnuplot", filename])
        
    def run(self, num_trials):
        """ Runs the given number of trials.
        
        If num_trials is 1, runs a single trial and graphs the result.
        Otherwise, graphs averaged results over all trials.
        """
        if num_trials == 1:
            self.run_single(-1)
            self.graph_single()
            return

        for trial in range(num_trials):
            print "********Running Trial %s**********" % trial
            self.run_single(trial)
            
        filename = "plot_%s.gp" % self.file_prefix
        gnuplot_file = open(filename, 'w')
        gnuplot_file.write("set terminal postscript color 'Helvetica' 14\n")
        #gnuplot_file.write("set size .5, .5\n")
        gnuplot_file.write("set output 'graphs/%s.ps'\n" % self.file_prefix)
        gnuplot_file.write("set xlabel 'Utilization'\n")
        gnuplot_file.write("set ylabel 'Response Time (ms)'\n")
        gnuplot_file.write("set yrange [0:700]\n")
        gnuplot_file.write("set grid ytics\n")
        #gnuplot_file.write("set xtics 0.25\n")
        extra = ""
        gnuplot_file.write("set title 'Effect of Load Probing on Response "
                           "Time%s'\n" % extra)
        #gnuplot_file.write("set key font 'Helvetica,10' left width -5"
        #                   "title 'Probes:Tasks' samplen 2\n")
        gnuplot_file.write("set key left\n")
        gnuplot_file.write("plot ")
        
        for i, probes_ratio in enumerate(self.probes_ratio_values):
            # Aggregate results and write to a file.
            # Map of utilization to response times for that utilization.
            results = {}
            for trial in range(num_trials):
                results_filename = ("raw_results/%s_response_time" %
                                    self.get_prefix(trial, probes_ratio))
                results_file = open(results_filename, "r")
                index = 0
                for line in results_file:
                    values = line.split("\t")
                    if values[0] == "n":
                        continue
                    # Use median response time.
                    normalized_response_time = float(values[6])
                    if self.remove_delay:
                        normalized_response_time -= 3*float(values[9])
                    utilization = float(values[2])
                    if utilization not in results:
                        results[utilization] = []
                    results[utilization].append(normalized_response_time)
                    
            agg_output_filename = ("raw_results/agg_%s_%f" %
                                   (self.file_prefix, probes_ratio))
            agg_output_file = open(agg_output_filename, "w")
            agg_output_file.write("Utilization\tResponseTime\tStdDev\n")
            for utilization in sorted(results.keys()):
                avg_response_time = stats.lmean(results[utilization])
                std_dev = stats.lstdev(results[utilization])
                agg_output_file.write("%f\t%f\t%f\n" %
                                      (utilization, avg_response_time, std_dev))
                
            # Plot aggregated results.
            if i > 0:
                gnuplot_file.write(', \\\n')
            title = "Probes/Tasks = %s" % probes_ratio
            if probes_ratio == -1:
                title = "Ideal"
            gnuplot_file.write(("'%s' using 1:2 title '%s' lc %d lw 4 with l,"
                                "\\\n") %
                               (agg_output_filename, title, i))
            gnuplot_file.write(("'%s' using 1:2:3 notitle lt %d lw 4 with "
                                "errorbars") % (agg_output_filename, i))
            
        subprocess.call(["gnuplot", filename])
        
def main():
    # Fill this in with whatever experiment should be run.
    debug_load_vs_wait("step_high", 0.8, 10000, 200, 1.5)

if __name__ == '__main__':
    main()
