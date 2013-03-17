#!/usr/bin/env python
import gearman, time, os, datetime

gm_client = gearman.GearmanClient(['localhost'])
ctx_dir = os.getcwd()

requests = []
name = []

log = open('ctx_processing.log','a')

def draw_screen_status():
    try:
        gm_client.get_job_statuses( requests )
    except KeyError:
        pass
    os.system('clear')
    restart = True
    while restart:
        restart = False
        for i in range( len(requests) ):
            if requests[i].complete:
                print "%s : %s" % (name[i], requests[i].state)
                log.write("[%s] %s : %s %s\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),name[i],requests[i],requests[i].status))
                requests.pop(i)
                name.pop(i)
                restart = True
                break

    for i in range( len(requests) ):
        status = requests[i].status
        if "running" in status and status["running"]:
            print "%s : %s %i/%i" % (name[i], requests[i].state, status["numerator"],status["denominator"])
        else:
            print "%s : %s" % (name[i], requests[i].state)



print 'Sending jobs...'
job_file = open('CTX_stereo_pair.txt','r')
for line in job_file.readlines():
    prefix = "_".join(line.split())
    if not os.path.exists(ctx_dir + "/DEM/" + prefix + "-DEM.tif"):
        name.append(line.split()[0])
        requests.append(gm_client.submit_job('ctx_stereo',line,wait_until_complete=False))
    else:
        print "Skipping %s" % line

    while len(requests) > 9:
        draw_screen_status()
        time.sleep(0.5)

while len(requests) > 0:
    draw_screen_status()
    time.sleep(0.5)
