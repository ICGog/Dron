export INCLUDE_DIR=include
export SOURCE_DIR=src
export TEST_DIR=test
export EBIN_DIR=ebin
export LIB_DIR=lib
export LOG_DIR=log
export WWW_DIR=www
export DRON_NODE=dron

DRON_WORKERS=w1 w2 w3
ERROR_LOG=$(LOG_DIR)/dron.log
INCLUDES=$(wildcard $(INCLUDE_DIR)/*.hrl)
SOURCES=$(wildcard $(SOURCE_DIR)/*.erl)
TARGETS=$(patsubst $(SOURCE_DIR)/%.erl, $(EBIN_DIR)/%.beam, $(SOURCES)) $(ADDITIONAL_ERL_TARGETS)
TEST_SOURCES=$(wildcard $(TEST_DIR)/*.erl)
TEST_TARGETS=$(patsubst $(TEST_DIR)/%.erl, $(TEST_DIR)/%.beam, $(TEST_SOURCES))
ADDITIONAL_ERL_SOURCES=$(LIB_DIR)/mochijson2/mochijson2.erl $(LIB_DIR)/gen_leader.erl
ADDITIONAL_ERL_TARGETS=$(EBIN_DIR)/mochijson2.beam $(EBIN_DIR)/gen_leader.beam
DIALYZER=dialyzer
DIALYZER_OPTS=-Wno_return -Wrace_conditions -Wunderspecs

ERLC_OPTS=-I $(INCLUDE_DIR) -o $(EBIN_DIR) -pa $(EBIN_DIR) -Wall -v +debug_info
ERL_OPTS=-pa $(EBIN_DIR) -pa $(TEST_DIR) -I $(INCLUDE_DIR) -sname $(DRON_NODE) -s dron -boot start_sasl -config dron
WORKER_ERL_OPTS=-pa $(EBIN_DIR) -pa $(TEST_DIR) -boot start_sasl -config dron

.PHONY: all
all: compile

compile: $(ADDITIONAL_ERL_TARGETS) $(TARGETS)

compile_tests: $(TEST_TARGETS)

run: $(ADDITIONAL_ERL_TARGETS) $(TARGETS)
	$(MAKE) start_workers
	mkdir -p $(LOG_DIR)
	DRON_WORKERS="$(DRON_WORKERS)" WORKER_ERL_OPTS="$(WORKER_ERL_OPTS)" \
	erl $(ERL_OPTS)
	$(MAKE) stop_workers

.PHONY: clean
clean:
	rm -f $(TARGETS)
	rm -rf $(LOG_DIR)
	rm -rf $(TEST_DIR)/*.beam
	rm -rf Mnesia.*
	$(MAKE) -C $(TEST_DIR) clean

.PHONY: start_workers
start_workers: $(TARGETS)
	for worker in $(DRON_WORKERS) ; do \
		echo "Starting worker $$worker" ; \
		echo 'code:add_pathsa(["$(realpath $(EBIN_DIR))"]).' | \
		erl_call -sname $$worker -s -e ; \
	done

.PHONY: stop_workers
stop_workers: $(TARGETS)
	for worker in $(DRON_WORKERS) ; do \
		echo "Stopping worker $$worker" ; \
		erl_call -sname $$worker -q ; \
	done

analyze: compile
	$(DIALYZER) $(DIALYZER_OPTS) -r ebin/

################################################################################
# Internal
################################################################################

$(ADDITIONAL_ERL_TARGETS): $(ADDITIONAL_ERL_SOURCES)
	for src in $(ADDITIONAL_ERL_SOURCES) ; do \
		erlc $(ERLC_OPTS) $$src ; \
	done

$(EBIN_DIR)/%.beam: $(SOURCE_DIR)/%.erl $(INCLUDES)
	erlc $(ERLC_OPTS) $<

$(TEST_TARGETS): $(TEST_DIR)

.PHONY: $(TEST_DIR)
$(TEST_DIR):
	$(MAKE) -C $(TEST_DIR) compile