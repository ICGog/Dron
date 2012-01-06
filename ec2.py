import boto
import boto.ec2
from boto.ec2.connection import EC2Connection
import time
import string
import sys
import os

def get_connection():
    return boto.ec2.regions()[0].connect()

def start_cluster(imageId, n, wPerNode):
    conn = get_connection()
    workers = ''
    reservation = conn.run_instances(image_id=imageId,
                                     security_groups=["Dron"],
                                     min_count=n, max_count=n,
                                     instance_type="m1.large",
                                     user_data="""#!/bin/bash
su - ubuntu -c "erl -detached -sname dron -pa Dron/ebin Dron/lib/gen_leader/ebin Dron/lib/mochiweb/ebin -I Dron/include"
""")
    for instance in reservation.instances:
        instance.update()
        while instance.state == u'pending':
            time.sleep(5)
            instance.update()
        if instance.state == u'running':
            print 'Started: ', instance
            for wNum in range(1, wPerNode):
                workers = (workers + 'dron' + str(wNum) + '@' +
                           string.split(instance.private_dns_name, '.')[0] +
                           ' ')
        else:
            print 'Could not start: ', instance
    print 'Workers: ', workers
    master = conn.run_instances(image_id=imageId,
                                security_groups=["Dron"],
                                min_count=1, max_count=1,
                                instance_type="m1.large").instances[0]
    master.update()
    while master.state == u'pending':
        time.sleep(5)
        master.update()
    if master.state != u'running':
        print 'Could not start master: ', master
        return
    print 'Master Address: ', master.public_dns_name

def stop_cluster():
    conn = get_connection()
    for reserv in conn.get_all_instances():
        for instance in reserv.instances:
            if instance.state != u'terminated':
                print 'Terminating: ', instance
                instance.terminate()

def main():
    if sys.argv[1] == "start":
        start_cluster(sys.argv[2], int(sys.argv[3]), int(sys.argv[4]))
    elif sys.argv[1] == "stop":
        stop_cluster()
    else:
        print 'Unknown Option'

if __name__ == "__main__":
    main()
