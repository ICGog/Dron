import boto
import boto.ec2
from boto.ec2.connection import EC2Connection
import time
import string
import sys
import os

def get_connection():
    # Connect to us-east
    if boto.ec2.regions()[2].name == u'us-east-1':
        return boto.ec2.regions()[2].connect()
    else:
        raise Exception('Regions have changes')

def start_instances(imageId, instanceType, num):
    conn = get_connection()
    reservation = conn.run_instances(image_id=imageId,
                                     security_groups=["icgog"],
                                     key_name="DronTest",
                                     min_count=num, max_count=num,
                                     instance_type=instanceType,
                                     user_data="")
    di = open('dron_instances', 'a')
    instances = []
    nodes = []
    for instance in reservation.instances:
        instance.update()
        while instance.state == u'pending':
            time.sleep(5)
            instance.update()
        if instance.state == u'running':
            print 'Started instance: ', instance
            instances.append(instance.public_dns_name)
            nodes.append(instance.private_dns_name.rsplit('.', 2)[0])
            di.write(instance.id + '\n')
        else:
            print 'Could not start: ', instance
    di.close()
    return (instances, nodes)

def start_cluster(imageId, instType, num, wpn, sImageId, sInstType, sNum):
    (workers, wIps) = start_instances(imageId, instType, num)
    print workers, wIps
    (schedulers, sIps) = start_instances(sImageId, sInstType, sNum)
    print schedulers, sIps
    write_exports(workers, wIps, wpn, schedulers, sIps)

def write_exports(workers, wIps, wpn, schedulers, sIps):
    file = open('dron_exports', 'w')
    dWorkers = 'export DRON_WORKERS="'
    for ip in wIps:
        for wpn in range(1, wpn + 1):
            dWorkers += 'w' + str(wpn) + '@' + ip + ' '
    file.write(dWorkers + '"\n')
    file.write('export DRON_MASTERS="' + 'dron@' + sIps[0] + '"\n')
    dDb = 'export DRON_DB="' + 'dron@' + sIps[0] + ' '
    dSchedulers = 'export DRON_SCHEDULERS="'
    for ip in sIps:
        dSchedulers += 's@' + ip + ' '
        dDb += 's@' + ip + ' '
    file.write(dSchedulers + '"\n')
    file.write(dDb + '"\n')
    dNodes = 'export DRON_NODES="'
    for node in (workers + schedulers):
        dNodes += node + ' '
    file.write(dNodes + '"\n\n')

    file.write('export ADDR_NODES_PER_WORKER=' + str(wpn) + '\n')

    aWorkers = ''
    for worker in workers:
        aWorkers += worker + ' '
    file.write('export ADDR_WORKERS="' + aWorkers + '"\n')
    aSchedulers = ''
    for scheduler in schedulers:
        aSchedulers += scheduler + ' '
    file.write('export ADDR_SCHEDULERS="' + aSchedulers + '"\n')
    file.write('export ADDR_MASTERS="' + schedulers[0] + '"\n')
    file.write('export ADDR_NODES="' + aSchedulers + ' ' + aWorkers + '"\n\n')
    file.write('export ERL_LIBS="/home/ubuntu/Dron/lib"\n')
    file.close()

def stop_cluster():
    instFile = open('dron_instances', 'r')
    conn = get_connection()
    instances = []
    for instance in instFile:
        print instance
        instances.append(instance.rstrip())
    print "Stopping: " + str(instances)
    conn.terminate_instances(instances)

def main():
    """ python ec2.py start wImageId wInstanceType wNum nodesPerWorker
sImageId wInstanceType sNum"""
    if sys.argv[1] == "start":
        start_cluster(sys.argv[2], sys.argv[3], int(sys.argv[4]),
                      int(sys.argv[5]), sys.argv[6], sys.argv[7],
                      int(sys.argv[8]))
    elif sys.argv[1] == "stop":
        stop_cluster()
    else:
        print 'Unknown Option'

if __name__ == "__main__":
    main()
