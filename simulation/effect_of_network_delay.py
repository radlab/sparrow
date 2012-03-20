""" This file runs multiple simulations to measure the effect of network delay.
"""
import simulation
import subprocess

import stats

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

def main():
    experiment = EffectOfNetworkDelay()
    experiment.run(3)

if __name__ == '__main__':
    main()