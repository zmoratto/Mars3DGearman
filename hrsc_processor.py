#!/usr/bin/env python
import gearman, time, os, datetime

gm_client = gearman.GearmanClient(['localhost'])
ctx_dir = os.getcwd()

requests = []
name = []

log = open('hrsc_processing.log','a')

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
job_file = open('hrsc_stereo_sort.csv','r')
for line in job_file.readlines():
    name.append(line.split(",")[1])
    requests.append(gm_client.submit_job('hrsc_stereo',line,wait_until_complete=False))

    while len(requests) > 19:
        draw_screen_status()
        time.sleep(0.5)

while len(requests) > 0:
    draw_screen_status()
    time.sleep(0.5)
