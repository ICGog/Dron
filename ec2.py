import boto
import boto.ec2
from boto.ec2.connection import EC2Connection
import time
import sys
import os

def get_connection():
    return boto.ec2.regions()[0].connect()

def start_cluster(n):
    conn = get_connection()
    f = open('instances', 'w')
    reservation = conn.run_instances(image_id="ami-771d2203",
                                     security_groups=["Dron"],
                                     min_count=n, max_count=n,
                                     instance_type="t1.micro",
                                     user_data="""#!/bin/bash
su - ubuntu -c "erl -detached -sname dron"
""")
    for instance in reservation.instances:
        instance.update()
        while instance.state == u'pending':
            time.sleep(5)
            instance.update()
        if instance.state == u'running':
            print 'Starting: ', instance
            f.write('dron@' + instance.private_dns_name + ' ')
        else:
            print 'Could not start: ', instance
    f.close()
    master = conn.run_instances(image_id="ami-771d2203",
                                security_groups=["Dron"],
                                min_count=1, max_count=1,
                                instance_type="t1.micro").instances[0]
    master.update()
    while master.state == u'pending':
        time.sleep(5)
        master.update()
    if master.state != u'running':
        print 'Could not start master: ', master
        return
    print 'Master Address: ', master.public_dns_name
    os.system("scp instances ubuntu@" + master.public_dns_name + ":")
    os.system("ssh ubuntu@" + master.public_dns_name + """mv instances Dron\ ;
cd Dron ;
make DRON_WORKERS=`cat instances` run""")

def stop_cluster():
    conn = get_connection()
    for reserv in conn.get_all_instances():
        for instance in reserv.instances:
            if instance.state != u'terminated':
                print 'Terminating: ', instance
                instance.terminate()

def main():
    if sys.argv[1] == "start":        
        start_cluster(int(sys.argv[2]))
    elif sys.argv[1] == "stop":
        stop_cluster()
    else:
        print 'Unknown Option'

if __name__ == "__main__":
    main()
