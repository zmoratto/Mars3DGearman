#!/usr/bin/env python
import gearman, socket, time, os, sys, subprocess, threading, sys, signal

gm_worker = gearman.GearmanWorker(['localhost'])
hrsc_dir=os.getcwd()

# Check for dependencies
if os.system("which stereo_pprc > /dev/null") != 0:
    print "Unable to find stereo pipeline tools"
    sys.exit()

if os.system("which hrsc2isis > /dev/null") != 0:
    print "Unable to find ISIS tools"
    sys.exit()

if os.system("which parallel > /dev/null") != 0:
    print "Unable to find the parallel command"
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
            os.killpg(p.pid, signal.SIGTERM)
        except Exception as e:
            print "Exception cause "
            print e
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
def task_process_hrsc( gearman_worker, gearman_job ):
    gearman_worker.send_job_status( gearman_job, 0, 12 )
    arguments = gearman_job.data.split(",")
    arguments = [i.strip("\"") for i in arguments]

    lprefix = arguments[1][:-4]
    rprefix = arguments[2][:-4]
    job_name = lprefix
    prefix = "%s/%s" % (job_name,job_name)

    # Making dir
    gearman_worker.send_job_status( gearman_job, 1, 12 )
    os.chdir(hrsc_dir)
    if not os.path.exists(job_name):
        os.mkdir(job_name)
    os.chdir(job_name)

    # download p12
    gearman_worker.send_job_status( gearman_job, 2, 12 )
    if not os.path.exists(arguments[1]+".bz2"):
        if not os.path.exists(arguments[1]):
            if not run_cmd( "curl -O http://pds-geosciences.wustl.edu/mex/mex-m-hrsc-3-rdr-v2/mexhrsc_0001/%s%s" % (arguments[0],arguments[1]) ):
                return "Failed"
    else:
        run_cmd( "bzip2 -d %s.bz2" % arguments[1] );

    # download p22
    gearman_worker.send_job_status( gearman_job, 3, 12 )
    if not os.path.exists(arguments[2]+".bz2"):
        if not os.path.exists(arguments[2]):
            if not run_cmd( "curl -O http://pds-geosciences.wustl.edu/mex/mex-m-hrsc-3-rdr-v2/mexhrsc_0001/%s%s" % (arguments[0],arguments[2]) ):
                return "Failed"
    else:
        run_cmd( "bzip2 -d %s.bz2" % arguments[2] );

    # calibrate
    gearman_worker.send_job_status( gearman_job, 4, 12 )
    if not run_cmd( "hrsc2isis from=%s to=%s.cub | tee -a log" % (arguments[1],lprefix) ):
        return "Failed"
    if not run_cmd( "hrsc2isis from=%s to=%s.cub | tee -a log" % (arguments[2],rprefix) ):
        return "Failed"
    if not run_cmd( "spiceinit ckpredicted=true from=%s.cub | tee -a log" % lprefix ):
        return "Failed"
    if not run_cmd( "spiceinit ckpredicted=true from=%s.cub | tee -a log" % rprefix ):
        return "Failed"

    # pprc
    gearman_worker.send_job_status( gearman_job, 5, 12 )
    default = open('stereo.default','w')
    default.write("alignment-method affineepipolar\nforce-use-entire-range       # Use entire input range\nprefilter-mode 2\nprefilter-kernel-width 1.4\ncost-mode 2\ncorr-kernel 21 21\nsubpixel-mode 1\nsubpixel-kernel 21 21\nrm-half-kernel 5 5\nrm-min-matches 60\nrm-threshold 3\nrm-cleanup-passes 1\nnear-universe-radius 0.0\nfar-universe-radius 0.0\n")
    default.close()
    if not run_cmd( "stereo_pprc *.cub %s | tee -a log" % prefix ):
        return "Failed"

    # corr
    gearman_worker.send_job_status( gearman_job, 6, 12, 7200 )
    if not run_cmd( "stereo_corr --corr-timeout 30 *.cub %s | tee -a log" % prefix):
        return "Failed"

    # rfne
    gearman_worker.send_job_status( gearman_job, 7, 12 )
    if not run_cmd( "stereo_rfne *.cub %s | tee -a log" % prefix):
        return "Failed"

    # fltr
    gearman_worker.send_job_status( gearman_job, 8, 12 )
    if not run_cmd( "stereo_fltr *.cub %s | tee -a log" % prefix):
        return "Failed"

    # tri
    gearman_worker.send_job_status( gearman_job, 9, 12 )
    if not run_cmd( "stereo_tri *.cub %s | tee -a log" % prefix):
        return "Failed"

    # dem
    gearman_worker.send_job_status( gearman_job, 10, 12 )
    lat = 0
    if arguments[0][21:22] == 'S':
        lat = -int(arguments[0][19:21])
    else:
        lat = int(arguments[0][19:21])
    if not run_cmd( "point2dem %s-PC.tif --orthoimage %s-L.tif --nodata -32767 --t_srs IAU2000:49910 | tee -a log" % (prefix,prefix,lat)):
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
    if not run_cmd( "rm -rf *cub" ):
        return "Failed"
    if not run_cmd( "rm -rf *IMG" ):
        return "Failed"

    return "Finished"


gm_worker.set_client_id(socket.gethostname())
gm_worker.register_task('hrsc_stereo', task_process_hrsc)

gm_worker.work()
