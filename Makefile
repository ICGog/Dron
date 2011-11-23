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
TARGETS=$(patsubst $(SOURCE_DIR)/%.erl, $(EBIN_DIR)/%.beam, $(SOURCES))
TEST_SOURCES=$(wildcard $(TEST_DIR)/*.erl)
TEST_TARGETS=$(patsubst $(TEST_DIR)/%.erl, $(TEST_DIR)/%.beam, $(TEST_SOURCES))

ERLC_OPTS=-I $(INCLUDE_DIR) -o $(EBIN_DIR) -Wall -v
ERL_OPTS=-pa $(EBIN_DIR) -pa $(TEST_DIR) -I $(INCLUDE_DIR) -sname $(DRON_NODE) -s dron
WORKER_ERL_OPTS=-pa $(EBIN_DIR) -pa $(TEST_DIR)

all: compile

compile: $(TARGETS)

compile_tests: $(TEST_TARGETS)

run: $(TARGETS)
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

################################################################################
# Internal
################################################################################

$(EBIN_DIR)/%.beam: $(SOURCE_DIR)/%.erl $(INCLUDES)
	erlc $(ERLC_OPTS) $<

$(TEST_TARGETS): $(TEST_DIR)

.PHONY: $(TEST_DIR)
$(TEST_DIR):
	$(MAKE) -C $(TEST_DIR) compile
