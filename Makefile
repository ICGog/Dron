export INCLUDE_DIR=include
export EBIN_DIR=ebin
export LOG_DIR=log
export LIB_DIR=lib
export DRON_NODE=dron

DRON_WORKERS=w1 w2 w3
REBAR=./rebar

DIALYZER=dialyzer
DIALYZER_OPTS=-Wno_return -Wrace_conditions -Wunderspecs

ERL_OPTS=-pa $(EBIN_DIR) -I $(INCLUDE_DIR) -sname $(DRON_NODE) -boot start_sasl -config dron -s dron -pa $(LIB_DIR)/gen_leader/ebin -pa $(LIB_DIR)/mochiweb/ebin -env ERL_MAX_ETS_TABLES 65536 -env ERL_MAX_PORTS 16384 +P256000

ifdef EC2_WORKERS
	START_WORKERS=python ec2.py start $(IMAGE_ID) $(EC2_WORKERS) $(WORKERS_PER_NODE)
	RUN=
	STOP=
	STOP_WORKERS=python ec2.py stop
else
	START_WORKERS=for worker in $(DRON_WORKERS) ; do \
			echo "Starting worker $$worker" ; \
			echo 'code:add_pathsa(["$(realpath $(EBIN_DIR))","$(realpath $(LIB_DIR))/gen_leader/ebin","$(realpath $(LIB_DIR))/mochiweb/ebin"]).' | \
			erl_call -sname $$worker -s -e ; \
		      done
	RUN=erl $(ERL_OPTS)
	STOP=$(MAKE) stop_workers
	STOP_WORKERS=for worker in $(DRON_WORKERS) ; do \
			echo "Stopping worker $$worker" ; \
			erl_call -sname $$worker -q ; \
	             done
endif

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

.PHONY: start_workers
start_workers:
	$(START_WORKERS)

.PHONY: stop_workers
stop_workers:
	$(STOP_WORKERS)

analyze: compile
	$(DIALYZER) $(DIALYZER_OPTS) -r ebin/
