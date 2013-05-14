Sparrow Scheduler
================================
Sparrow is a high throughput, low latency, and fault-tolerant distributed cluster scheduler. Sparrow is designed for applications that require resource allocations frequently for very short jobs, such as analytics frameworks. Sparrow schedules from a distributed set of schedulers that maintain no shared state. Instead, to schedule a job, a scheduler obtains intantaneous load information by sending probes to a subset of worker machines. The scheduler places the job's tasks on the least loaded of the probed workers. This technique allows Sparrow to schedule in milliseconds, two orders of magnitude faster than existing approaches. Sparrow also handles failures: if a scheduler fails, a client simply directs scheduling requests to an alternate scheduler.

To read more about Sparrow, check out our [tech report](http://www.eecs.berkeley.edu/Pubs/TechRpts/2013/EECS-2013-29.html).

If you're interested in using Sparrow, we recommend that you join the [Sparrow mailing list](https://groups.google.com/group/sparrow-scheduler-users).


Code Layout
-------------------------
`/sparrow/src` (maven-style source directory)

`/sparrow/src/main/java/edu/berkeley/sparrow` (most of the interesting java code)

`/deploy/`     (contains ec2 deployment scripts)

Building Sparrow
-------------------------

Sparrow is under development right now and is not yet production ready. You should be able to build it, however, using Maven:

<pre>
$ mvn compile
$ mvn package -Dmaven.test.skip=true
</pre>

There are a variety of deployment related files in `sparrow/deploy/ec2`. These mostly focus on deploying and testing Sparrow in ec2, but for the truly curious, they do give insight on configuring and running Sparrow in its current form.

Research
-------------------------
Sparrow is a research project within the [U.C. Berkeley AMPLab](http://amplab.cs.berkeley.edu/). The Sparrow team, listed roughly order of height, consists of Kay Ousterhout, Patrick Wendell, Matei Zaharia, and Ion Stoica. Please contact us for more information.
