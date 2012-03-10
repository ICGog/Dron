export INCLUDE_DIR=include
export EBIN_DIR=ebin
export LOG_DIR=log
export LIB_DIR=lib
export DRON_NODE=dron

DRON_NODES=w1 w2 w3
REBAR=./rebar

DIALYZER=dialyzer
DIALYZER_OPTS=-Wno_return -Wrace_conditions -Wunderspecs

ERL_OPTS=-pa $(EBIN_DIR) -pa $(LIB_DIR) -I $(INCLUDE_DIR) -sname $(DRON_NODE) -boot start_sasl -config dron -s dron -env ERL_MAX_ETS_TABLES 65536 -env ERL_MAX_PORTS 16384 +P256000

START_WORKERS=for worker in $(DRON_NODES) ; do \
		echo "Starting worker $$worker" ; \
		echo 'code:add_pathsa(["$(realpath $(EBIN_DIR))","$(realpath $(LIB_DIR))"]).' | \
		erl_call -sname $$worker -s -e ; \
	      done
RUN=erl $(ERL_OPTS)
STOP=$(MAKE) stop_workers
STOP_WORKERS=for worker in $(DRON_NODES) ; do \
		echo "Stopping worker $$worker" ; \
		erl_call -sname $$worker -q ; \
             done


.PHONY: all
all: deps compile

compile:
	$(REBAR) compile

deps:
	$(REBAR) get-deps

test:
	$(REBAR) skip_deps=true eunit

doc:
	$(REBAR) doc

run: compile
	mkdir -p $(LOG_DIR)
	$(MAKE) start_workers
	DRON_WORKERS="$(DRON_WORKERS)" \
	$(RUN)
	$(STOP)

.PHONY: clean
clean:
	rm -rf $(LOG_DIR)
	rm -rf dron_coordinator_*
	rm -rf dron_scheduler_*
	$(REBAR) clean
	rm -rf Mnesia.*

.PHONY: clean_ec2
clean_ec2:
	rm -rf dron_exports

.PHONY: start_workers
start_workers:
	$(START_WORKERS)

.PHONY: stop_workers
stop_workers:
	$(STOP_WORKERS)

analyze: compile
	$(DIALYZER) $(DIALYZER_OPTS) -r ebin/
