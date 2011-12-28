export INCLUDE_DIR=include
export EBIN_DIR=ebin
export LOG_DIR=log
export LIB_DIR=lib
export DRON_NODE=dron

DRON_WORKERS=w1 w2 w3
REBAR=./rebar

DIALYZER=dialyzer
DIALYZER_OPTS=-Wno_return -Wrace_conditions -Wunderspecs

ERL_OPTS=-pa $(EBIN_DIR) -I $(INCLUDE_DIR) -sname $(DRON_NODE) -boot start_sasl -config dron -s dron -pa $(LIB_DIR)/gen_leader/ebin -pa $(LIB_DIR)/mochiweb/ebin

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
	erl $(ERL_OPTS)
	$(MAKE) stop_workers

.PHONY: clean
clean:
	rm -rf $(LOG_DIR)
	$(REBAR) clean
	rm -rf Mnesia.*

.PHONY: start_workers
start_workers:
	for worker in $(DRON_WORKERS) ; do \
		echo "Starting worker $$worker" ; \
		echo 'code:add_pathsa(["$(realpath $(EBIN_DIR))","$(realpath $(LIB_DIR))/gen_leader/ebin","$(realpath $(LIB_DIR))/mochiweb/ebin"]).' | \
		erl_call -sname $$worker -s -e ; \
	done

.PHONY: stop_workers
stop_workers:
	for worker in $(DRON_WORKERS) ; do \
		echo "Stopping worker $$worker" ; \
		erl_call -sname $$worker -q ; \
	done

analyze: compile
	$(DIALYZER) $(DIALYZER_OPTS) -r ebin/
