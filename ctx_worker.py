#!/usr/bin/env python
import gearman, socket, time, os, sys, subprocess, threading, sys

gm_worker = gearman.GearmanWorker(['localhost'])
ctx_dir=os.getcwd()

# Check for dependencies
if os.system("which stereo_pprc > /dev/null") != 0:
    print "Unable to find stereo pipeline tools"
    sys.exit()

if os.system("which mroctx2isis > /dev/null") != 0:
    print "Unable to find ISIS tools"
    sys.exit()

if os.system("which parallel > /dev/null") != 0:
    print "Unable to find the parallel command"
    sys.exit()

if not os.path.exists(ctx_dir+"/ctx_url_lookup2"):
    print "Unable to find the ctx_url_lookup2 file"
    sys.exit()

if sys.version_info[1] < 6:
    print "Require python version 2.6 or newer."
    sys.exit()

# Debug utility
def task_test( gearman_worker, gearman_job ):
    gearman_worker.send_job_status( gearman_job, 0, 12 )
    arguments = gearman_job.data.split()
    prefix = "%s/%s" % (arguments[0],arguments[0])
    print arguments
    return "finished"

# Timer function to kill process if it takes too long
def process_timeout( p ):
    if p.poll() == None:
        try:
            print "Process taking too long to complete"
            p.killpg(pro.pid, signal.SIGTERM)
        except:
            pass

# Helper function to run command
def run_cmd( cmd, time=3600 ):
    print "Running cmd: %s" % cmd
    proc = subprocess.Popen( cmd, shell=True, preexec_fn=os.setsid )
    t = threading.Timer( time, process_timeout, [proc] )
    t.start()
    (stdoutdata, stderrdata) = proc.communicate()
    t.cancel()
    if proc.returncode < 0:
        return False
    return True

# Individual steps are [make dir, download left, download right,
# calibrate, pprc, corr, rfne, fltr, tri, dem, move]
def task_process_ctx( gearman_worker, gearman_job ):
    gearman_worker.send_job_status( gearman_job, 0, 12 )
    arguments = gearman_job.data.split()
    job_name = "_".join(arguments)
    prefix = "%s/%s" % (job_name,job_name)
    print arguments

    # Making dir
    gearman_worker.send_job_status( gearman_job, 1, 12 )
    os.chdir(ctx_dir)
    if not os.path.exists(job_name):
        os.mkdir(job_name)
    os.chdir(job_name)

    # download left
    gearman_worker.send_job_status( gearman_job, 2, 12 )
    if not os.path.exists(arguments[0]+".IMG"):
        if not run_cmd( "grep %s %s/ctx_url_lookup2 | xargs curl -O" % (arguments[0],ctx_dir) ):
            return "Failed"

    # download right
    gearman_worker.send_job_status( gearman_job, 3, 12 )
    if not os.path.exists(arguments[1]+".IMG"):
        if not run_cmd( "grep %s %s/ctx_url_lookup2 | xargs curl -O" % (arguments[1],ctx_dir) ):
            return "Failed"

    # calibrate
    gearman_worker.send_job_status( gearman_job, 4, 12 )
    if not run_cmd( "mroctx2isis from=%s.IMG to=%s.1.cub | tee -a log" % (arguments[0],arguments[0]) ):
        return "Failed"
    if not run_cmd( "mroctx2isis from=%s.IMG to=%s.1.cub | tee -a log" % (arguments[1],arguments[1]) ):
        return "Failed"
    if not run_cmd( "spiceinit from=%s.1.cub | tee -a log" % arguments[0] ):
        return "Failed"
    if not run_cmd( "spiceinit from=%s.1.cub | tee -a log" % arguments[1] ):
        return "Failed"
    if not run_cmd( "ctxcal from=%s.1.cub to=%s.2.cub | tee -a log" % (arguments[0],arguments[0]) ):
        return "Failed"
    if not run_cmd( "ctxcal from=%s.1.cub to=%s.2.cub | tee -a log" % (arguments[1],arguments[1]) ):
        return "Failed"
    if not run_cmd( "rm *1.cub" ):
        return "Failed"

    # pprc
    gearman_worker.send_job_status( gearman_job, 5, 12 )
    default = open('stereo.default','w')
    default.write("alignment-method homography\nforce-use-entire-range       # Use entire input range\nprefilter-mode 2\nprefilter-kernel-width 1.4\ncost-mode 2\ncorr-kernel 21 21\nsubpixel-mode 2\nsubpixel-kernel 19 19\nrm-half-kernel 5 5\nrm-min-matches 60\nrm-threshold 3\nrm-cleanup-passes 1\nnear-universe-radius 0.0\nfar-universe-radius 0.0\n")
    default.close()
    if not run_cmd( "stereo_pprc *.2.cub %s | tee -a log" % prefix):
        return "Failed"

    # corr
    gearman_worker.send_job_status( gearman_job, 6, 12 )
    if not run_cmd( "stereo_corr *.2.cub %s | tee -a log" % prefix, 3600*2):
        return "Failed"

    # rfne
    gearman_worker.send_job_status( gearman_job, 7, 12 )
    if not run_cmd( "stereo_rfne *.2.cub %s | tee -a log" % prefix, 3600*6):
        return "Failed"

    # fltr
    gearman_worker.send_job_status( gearman_job, 8, 12 )
    if not run_cmd( "stereo_fltr *.2.cub %s | tee -a log" % prefix):
        return "Failed"

    # tri
    gearman_worker.send_job_status( gearman_job, 9, 12 )
    if not run_cmd( "stereo_tri *.2.cub %s | tee -a log" % prefix):
        return "Failed"

    # dem
    gearman_worker.send_job_status( gearman_job, 10, 12 )
    lat = 0
    if arguments[0][21:22] == 'S':
        lat = -int(arguments[0][19:21])
    else:
        lat = int(arguments[0][19:21])
    if not run_cmd( "point2dem %s-PC.tif --orthoimage %s-L.tif --tr 30 --nodata -32767 --t_srs '+proj=eqc +lat_ts=%i +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +a=3396000 +b=3396000 +units=m +no_defs' | tee -a log" % (prefix,prefix,lat)):
        return "Failed"

    # move and clean up
    gearman_worker.send_job_status( gearman_job, 11, 12 )
    if not run_cmd( "mv %s-DEM.tif %s/DEM/" % (prefix,ctx_dir)):
        return "Failed"
    if not run_cmd( "mv %s-DRG.tif %s/DRG/" % (prefix,ctx_dir)):
        return "Failed"
    if not run_cmd( "parallel bzip2 -z -9 ::: *cub *IMG"):
        return "Failed"
    if not run_cmd( "rm -rf %s" % job_name):
        return "Failed"

    return "Finished"


gm_worker.set_client_id(socket.gethostname())
gm_worker.register_task('ctx_stereo', task_process_ctx)
#gm_worker.register_task('ctx_stereo', task_test)

gm_worker.work()
