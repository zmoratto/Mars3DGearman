#!/usr/bin/env python
import gearman, socket, time, os, sys

gm_worker = gearman.GearmanWorker(['localhost'])
ctx_dir=os.getcwd()

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

def task_test( gearman_worker, gearman_job ):
    gearman_worker.send_job_status( gearman_job, 0, 12 )
    arguments = gearman_job.data.split()
    prefix = "%s/%s" % (arguments[0],arguments[0])
    print arguments
    return "finished"

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
        os.system("grep %s %s/ctx_url_lookup2 | xargs curl -O" % (arguments[0],ctx_dir))

    # download right
    gearman_worker.send_job_status( gearman_job, 3, 12 )
    if not os.path.exists(arguments[1]+".IMG"):
        os.system("grep %s %s/ctx_url_lookup2 | xargs curl -O" % (arguments[1],ctx_dir))

    # calibrate
    gearman_worker.send_job_status( gearman_job, 4, 12 )
    os.system("mroctx2isis from=%s.IMG to=%s.1.cub | tee -a log" % (arguments[0],arguments[0]) )
    os.system("mroctx2isis from=%s.IMG to=%s.1.cub | tee -a log" % (arguments[1],arguments[1]) )
    os.system("spiceinit from=%s.1.cub | tee -a log" % arguments[0] )
    os.system("spiceinit from=%s.1.cub | tee -a log" % arguments[1] )
    os.system("ctxcal from=%s.1.cub to=%s.2.cub | tee -a log" % (arguments[0],arguments[0]) )
    os.system("ctxcal from=%s.1.cub to=%s.2.cub | tee -a log" % (arguments[1],arguments[1]) )
    os.system("rm *1.cub")

    # pprc
    gearman_worker.send_job_status( gearman_job, 5, 12 )
    default = open('stereo.default','w')
    default.write("alignment-method homography\nforce-use-entire-range       # Use entire input range\nprefilter-mode 2\nprefilter-kernel-width 1.4\ncost-mode 2\ncorr-kernel 21 21\nsubpixel-mode 2\nsubpixel-kernel 19 19\nrm-half-kernel 5 5\nrm-min-matches 60\nrm-threshold 3\nrm-cleanup-passes 1\nnear-universe-radius 0.0\nfar-universe-radius 0.0\n")
    default.close()
    os.system("stereo_pprc *.2.cub %s | tee -a log" % prefix)

    # corr
    gearman_worker.send_job_status( gearman_job, 6, 12 )
    os.system("stereo_corr *.2.cub %s | tee -a log" % prefix)

    # rfne
    gearman_worker.send_job_status( gearman_job, 7, 12 )
    os.system("stereo_rfne *.2.cub %s | tee -a log" % prefix)

    # fltr
    gearman_worker.send_job_status( gearman_job, 8, 12 )
    os.system("stereo_fltr *.2.cub %s | tee -a log" % prefix)

    # tri
    gearman_worker.send_job_status( gearman_job, 9, 12 )
    os.system("stereo_tri *.2.cub %s | tee -a log" % prefix)

    # dem
    gearman_worker.send_job_status( gearman_job, 10, 12 )
    lat = 0
    if arguments[0][21:22] == 'S':
        lat = -int(arguments[0][19:21])
    else:
        lat = int(arguments[0][19:21])
    os.system("point2dem %s-PC.tif --orthoimage %s-L.tif --tr 30 --nodata -32767 --t_srs '+proj=eqc +lat_ts=%i +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +a=3396000 +b=3396000 +units=m +no_defs' | tee -a log" % (prefix,prefix,lat))

    # move and clean up
    gearman_worker.send_job_status( gearman_job, 11, 12 )
    os.system("mv %s-DEM.tif %s/DEM/" % (prefix,ctx_dir))
    os.system("mv %s-DRG.tif %s/DRG/" % (prefix,ctx_dir))
    os.system("parallel bzip2 -z -9 ::: *cub *IMG")
    os.system("rm -rf %s" % job_name)

    return "Finished"


gm_worker.set_client_id(socket.gethostname())
gm_worker.register_task('ctx_stereo', task_process_ctx)
#gm_worker.register_task('ctx_stereo', task_test)

gm_worker.work()
