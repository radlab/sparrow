Sparrow Scheduler
================================
Sparrow is a high throughput, low latency, distributed cluster scheduler. Sparrow is designed for applications which require resource allocations frequently for very short jobs, such as analytics frameworks. To ensure that scheduling does not bottleneck such applications, we distributed scheduling over several loosely coordinated machines. Each scheduler employs a constant time scheduling algorithm based on mostly consistent information. These techniques let Sparrow perform task scheduling in milliseconds, two orders of magnitude faster than existing approaches.


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
Sparrow is a research project within the [U.C. Berkeley AMPLab](http://amplab.cs.berkeley.edu/). The Sparrow team, listed roughly order of height, consists of Kay Ousterhout, Patrick Wendell, Matei Zaharia, and Ion Stoica. Plese contact us for more information.
