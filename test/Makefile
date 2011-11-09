
TEST_SOURCES=$(wildcard *.erl)
TEST_TARGETS=$(patsubst %.erl, %.beam, $(TEST_SOURCES))
INCLUDES=$(wildcard ../$(INCLUDE_DIR)/*.hrl)

ERLC_OPTS=-I ../$(INCLUDE_DIR) -o ./ -Wall -v +debug_info

ERL_OPTS=-pa ../$(EBIN_DIR) -pa ./ -sname $(DRON_NODE) -noshell
WORKER_ERL_OPTS=-pa ../$(EBIN_DIR) -pa ./

compile: $(TEST_TARGETS)

%.beam: %.erl $(INCLUDES)
	erlc $(ERLC_OPTS) $<

clean:
	rm -rf $(TEST_TARGETS)