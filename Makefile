export INCLUDE_DIR=include
export SOURCE_DIR=src
export TEST_DIR=test
export EBIN_DIR=ebin
export LIB_DIR=lib
export LOG_DIR=log
export WWW_DIR=www
export THRIFT_DIR=thrift
export DRON_NODE=dron
export THRIFT_BIN=/usr/local/bin/thrift

DRON_WORKERS=w1 w2 w3
ERROR_LOG=$(LOG_DIR)/dron.log
INCLUDES=$(wildcard $(INCLUDE_DIR)/*.hrl)
THRIFT_INCLUDES=$(wildcard $(THRIFT_DIR)/gen-erl/*.hrl)
SOURCES=$(wildcard $(SOURCE_DIR)/*.erl)
TARGETS=$(patsubst $(SOURCE_DIR)/%.erl, $(EBIN_DIR)/%.beam, $(SOURCES))
TEST_SOURCES=$(wildcard $(TEST_DIR)/*.erl)
TEST_TARGETS=$(patsubst $(TEST_DIR)/%.erl, $(TEST_DIR)/%.beam, $(TEST_SOURCES))

ERLC_OPTS=-I $(INCLUDE_DIR) -I $(THRIFT_DIR)/gen-erl -o $(EBIN_DIR) -Wall -v
ERL_OPTS=-pa $(EBIN_DIR) -pa $(TEST_DIR) -I $(INCLUDE_DIR) -I $(THRIFT_DIR)/gen-erl -sname $(DRON_NODE) -s dron
WORKER_ERL_OPTS=-pa $(EBIN_DIR) -pa $(TEST_DIR)

.PHONY: all
all: compile

compile: thrift $(TARGETS)

compile_tests: thrift $(TEST_TARGETS)

.PHONY: thrift
thrift: 
	$(THRIFT_BIN) -r --gen erl -o $(THRIFT_DIR) $(THRIFT_DIR)/dron.thrift

run: thrift $(TARGETS)
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
	rm -rf $(THRIFT_DIR)/gen-erl
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

$(EBIN_DIR)/%.beam: $(SOURCE_DIR)/%.erl $(INCLUDES) $(THRIFT_INCLUDES)
	erlc $(ERLC_OPTS) $<

$(TEST_TARGETS): $(TEST_DIR)

.PHONY: $(TEST_DIR)
$(TEST_DIR):
	$(MAKE) -C $(TEST_DIR) compile