Simulation
================================

The Sparrow simulator is designed to model performance of a deployed Sparrow system.  It simulates jobs with some number of parallel tasks.  Tasks are run on slaves (a `Server`, in the simulation terminology), which each run one task at a time and queue remaining tasks.  Incoming jobs arrive at Sparrow schedulers (`FrontEnd` in the simulation terminology), which probe some number of slaves for their current queue length.  Upon receiving the probe replies, the scheduler places the job's tasks on the least loaded machines.  The job response time is measured as the time from when the job arrives at a scheduler to when its last task completes.

The simulation is event-based: at a high level, it works by creating a large number of `JobArrival` events (simulating a job arriving at a scheduler), and then processes those events in order, using an event queue, until no more events remain.  New events (to process probes, place tasks, etc.) are created and processed, in order, along the way.

The main goal of the simulation is to model a real Sparrow system under a variety of configurations and in various different situations.  To accomplish this, the simulation includes a large number of parameters to adjust the number of probes used, relative to the number of tasks; the network delay; the number of slaves; and much more.

Code Layout
-------------------------
`simulation.py` Code to run a single simulation

`graph.py` Some scripts that use `simulation.py` to run multiple simulations, typically involving varying one or more parameters and graphing the result.

`simulation_tests.py` Unit tests for the simulation.

`stats.py` Statistics functionality to help with interpreting results.

Running a Single Simulation
-------------------------

The Sparrow simulation code mainly lives in `simulation.py`.  To run a single simulation with all of the defaults, simply execute:

<pre>
$ python simulation.py
</pre>

To see the available parameters, run:

<pre>
$ python simulation.py help
</pre>

Results are saved in the `raw_results/` directory, using the prefix given by the `file_prefix` parameter.  the most useful output file is the `<file_prefix>_response_time` file, which gives in formation about the response time at various percentiles.  The handy `<file_prefix>.params` file records the parameterization for the experiment.

Running Larger Experiments
-------------------------
Typically, the most useful simulations involve varying one or more parameters and graphing the result.  The `graph.py` file includes functionality to run experiments using a variety of parameters, and automatically graphs  the results.

Testing the Simulation
-------------------------
<pre>
$ python simulation_tests.py
</pre>
