#
# Copyright 2013 The Regents of The University California
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Quick-and-dirty script to get the response time distribution and plot it
import sys
import os
import subprocess

def main(argv):
  log_dir = argv[1]
  min_n = argv[2]
  max_n = argv[3]

  unqualified_log_files = os.listdir(log_dir)
  log_files = [os.path.join(log_dir, filename) for \
               filename in unqualified_log_files]
  log_files = filter(lambda x: "tpch" in x, log_files)

  out_files = []
  for fname in log_files:
    f = open(fname)
    ip = fname.split("/")[-1:][0].split("_")[2].replace(".log", "")
    lines = filter(lambda k: "trial" in k, f.readlines())
    lines = lines[int(min_n):int(max_n)]
    
    times = map(lambda x: int(x.strip().split("\t")[1]), lines)

    out_file = ip + ".plot_input"
    out = open(out_file, 'w')
    for (i, time) in enumerate(sorted(times)):
      out.write("%s\t%s\n" % (float(i)/len(times), time))
    out.close()
    out_files.append((out_file, ip))

  plot_fname = "tpc_resp_cdf.plt"
  f = open(plot_fname, 'w')
  plot_file = open(plot_fname, 'w')
  plot_file.write("set terminal postscript color\n")
  plot_file.write("set output 'tpc_resp_cdf.ps'\n")
  parts = map(lambda x: "'%s' using 2:1 with lines lw 3 title '%s'"
    % (x[0], x[1]), out_files)
  plot = "plot " + ",\\\n".join(parts)
  plot_file.write(plot + "\n")
  plot_file.close()
  subprocess.check_call("gnuplot %s" % plot_fname, shell=True)
#  subprocess.check_call("rm *.plot_input", shell=True)

    

if __name__ == "__main__":
  main(sys.argv)
