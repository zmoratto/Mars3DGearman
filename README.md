Mars3DGearman
=============

Gearman scripts for doing 3D processing of Mars Mission data. Currently supports CTX and HRSC. Can totally be more if we like.

## Prerequisite

Install Gearman! Here's how to do that in Ubuntu:
> sudo apt-get install gearman gearman-job-server python-gearman

## Running

You need to start the CTX processor somewhere were he'll never die. This script is the guy who lists off what stereo pairs need to be performed and keeps track of what is finished. Normally I would start this in a screen session on my server.

<pre>
> screen
> (inside screen session)
> cd $PRJDIR
> python ctx_processor.py
> ctrl + a + d
</pre>

At the moment that script is just looks at the included CTX_stereo_pair.txt to see what needs to be performed. This Python code has been fairly unstable for me. I'd like to blame the Python port of Gearman. Alternatively, it is possible to list off the jobs yourself using the gearman command. Here's the HRSC variant. Set -jN to be as many jobs you'd like to be running simultaneously. This number is going to be limited by the number of workers you spawn later on.

<pre>
parallel -j10 "echo {} | gearman -f hrsc_stereo" :::: hrsc_stereo_sort.csv
</pre>

Now you need to start the workers. Currently the workers are stupid and only look for the server, ctx_processor, via the url, localhost. When I want to run jobs on machines at NASA I actually have my server perform a reverse ssh on the gearman port.

That's complicated, so how about the easy version? Well on the same machine running the server, open another terminal that won't die. I'll use screen again.

<pre>
> screen -r
> ctrl + a + c
> cd $BIG_RAID
> mkdir CTX_Processing_Dir
> cd CTX_Processing_Dir
> python $PRJDIR/ctx_worker.py
</pre>

That worker assumes it has access to ASP and ISIS tools. It will quit early if you didn't provide those via $PATH. You can then create as many terminals as you like and workers as you like. You can look at the text output from ctx_processor.py while it is running to see what jobs are currently running and what fraction each job is complete.
