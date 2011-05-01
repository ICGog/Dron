export INCLUDE_DIR=include
export SOURCE_DIR=src
export EBIN_DIR=ebin
export LIB_DIR=lib
export LOG_DIR=log
export WWW_DIR=www
export DRON_NODE=dron_master

DRON_WORKERS=w1 w2 w3
INCLUDES=$(wildcard $(INCLUDE_DIR)/*.hrl)
SOURCES=$(wildcard $(SOURCE_DIR)/*.erl)
TARGETS=$(patsubst $(SOURCE_DIR)/%.erl, $(EBIN_DIR)/%.beam, $(SOURCES))

ERL_OPTS=-pa $(EBIN_DIR) -sname $(DRON_NODE) dron
WORKER_ERL_OPTS=-pa $(EBIN_DIR)

all: compile

compile: $(TARGETS)

run: $(TARGETS)
	$(MAKE) start_workers
	mkdir -p $(LOG_DIR)
	erl $(ERL_OPTS)
	$(MAKE) stop_workers

clean:
	rm -f $(TARGETS)
	rm -rf $(LOG_DIR)
	rm -rf Mnesia.*

.PHONY: start_workers
start_workers: $(TARGETS)
	for worker in $(DRON_WORKERS) ; do \
		echo "Starting worker $$worker" ; \
		erl_call -sname $$worker
		done

.PHONY: stop_workers
stop_workers: $(TARGETS)
	for worker in $(DRON_WORKERS) ; do \
		echo "Stopping worker $$worker" ; \
		erl_call -sname $$worker -q ; \
		done