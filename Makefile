PACKAGE         ?= ldb
VERSION         ?= $(shell git describe --tags)
BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR            = $(shell pwd)/rebar3
MAKE						 = make

.PHONY: rel deps test eqc plots

all: compile

##
## Compilation targets
##

compile:
	$(REBAR) compile

clean: packageclean
	$(REBAR) clean

packageclean:
	rm -fr *.deb
	rm -fr *.tar.gz

##
## Test targets
##

check: test xref dialyzer lint

test: ct eunit

lint:
	${REBAR} as lint lint

eqc:
	${REBAR} as test eqc

eunit:
	${REBAR} as test eunit

ct:
	${REBAR} as test ct

shell:
	${REBAR} shell --apps ldb

basic:
	pkill -9 beam.smp; \
		${REBAR} as test ct --readable=false --suite=test/ldb_basic_simulation_SUITE

logs:
	  tail -F priv/lager/*/log/*.log

DIALYZER_APPS = kernel stdlib erts sasl eunit syntax_tools compiler crypto

include tools.mk
