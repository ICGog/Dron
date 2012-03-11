#!/bin/bash
# If DRON_STOP is set then it stops all the nodes.

source ~/Dron/dron_exports
export ERL_MAX_ETS_TABLES=65536
export ERL_MAX_PORTS=16384

# Start scheduler nodes
echo "Starting schedulers $(ADDR_SCHEDULERS)"
for instance in $ADDR_SCHEDULERS
do
    if [ -z "$DRON_STOP" ]; then
        ssh ubuntu@$instance "export ERL_LIBS=\"~/Dron/lib\"; erl -pa ~/Dron/ebin ~/Dron/lib -i ~/Dron/include -detached -sname s -s -e +P256000"
    else
        ssh ubuntu@$instance "erl_call -sname s -q"
    fi
done

echo "Starting workers $(ADDR_WORKERS)"
# Start worker nodes
for instance in $ADDR_WORKERS
do
    for w in `seq 1 $ADDR_NODES_PER_WORKER`
    do
        if [ -z "$DRON_STOP" ]; then
            ssh ubuntu@$instance "export ERL_LIBS=\"~/Dron/lib\";erl -pa ~/Dron/ebin ~/Dron/lib -i ~/Dron/include -detached -sname w$w -s -e +P256000"
        else
            ssh ubuntu@$instance "erl_call -sname w$w -q"
        fi
    done
done

if [ -z "$DRON_STOP" ]; then
    echo "Starting master $(ADDR_MASTERS)"
    for master in $ADDR_MASTERS
    do
        scp dron_exports ubuntu@$master:Dron/
        # Start master node
        ssh ubuntu@$master "cd Dron; source dron_exports; erl -pa ~/Dron/ebin ~/Dron/lib -i ~/Dron/include -sname dron -boot start_sasl -config dron -s dron +P256000"
    done
fi
