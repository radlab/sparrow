Sparrow Scheduler
================================
Sparrow is a high throughput, low latency, and fault-tolerant distributed cluster scheduler. Sparrow is designed for applications that require resource allocations frequently for very short jobs, such as analytics frameworks. Sparrow schedules from a distributed set of schedulers that maintain no shared state. Instead, to schedule a job, a scheduler obtains intantaneous load information by sending probes to a subset of worker machines. The scheduler places the job's tasks on the least loaded of the probed workers. This technique allows Sparrow to schedule in milliseconds, two orders of magnitude faster than existing approaches. Sparrow also handles failures: if a scheduler fails, a client simply directs scheduling requests to an alternate scheduler.

To read more about Sparrow, check out our [paper](http://dl.acm.org/citation.cfm?doid=2517349.2522716).

If you're interested in using Sparrow, we recommend that you join the [Sparrow mailing list](https://groups.google.com/group/sparrow-scheduler-users).


Code Layout
-------------------------
`/sparrow/src` (maven-style source directory)

`/sparrow/src/main/java/edu/berkeley/sparrow` (most of the interesting java code)

`/sparrow/src/main/java/edu/berkeley/sparrow/examples` (Example applications that use Sparrow)

`/deploy/`     (contains ec2 deployment scripts)

Building Sparrow
-------------------------

You can build Sparrow using Maven:

<pre>
$ mvn compile
$ mvn package -Dmaven.test.skip=true
</pre>

Sparrow and Spark
------------------------

We have ported [Spark](http://spark.incubator.apache.org/), an in-memory analytics framework, to schedule using Sparrow. To use Spark with Sparrow, you'll need to use the [our forked version of Spark](https://github.com/kayousterhout/spark/tree/sparrow), which includes the Sparrow scheduling plugin.

Research
-------------------------
Sparrow is a research project within the [U.C. Berkeley AMPLab](http://amplab.cs.berkeley.edu/). The Sparrow team, listed roughly order of height, consists of Kay Ousterhout, Patrick Wendell, Matei Zaharia, and Ion Stoica. Please contact us for more information.
