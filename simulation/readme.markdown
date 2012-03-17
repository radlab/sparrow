Simulation
================================

The Sparrow simulation code mainly lives in `simulation.py`.  To run a single simulation with all of the defaults, simply execute:

<pre>
$ python simulation.py
</pre>

To see the available parameters, run:

<pre>
$ python simulation.py help
</pre>

The simulation code expects to find a 'raw_results/' folder, which is where it places all results, so be sure to create this before running any simulations.

The other files in this directory help with the simulation:

`graph.py` Some scripts to run larger simulations, typically involving varying one or more parameters and graphing the result.  Expects a 'graphs/' folder, which is where all graphs will be placed.  To learn more, see the documentation in the file.

`simulation_tests.py` Unit tests for the simulation.

`stats.py` Statistics functionality to help with interpreting results.

