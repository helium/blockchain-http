.PHONY: compile rel cover test typecheck doc ci start stop reset

REBAR=./rebar3
SHORTSHA=`git rev-parse --short HEAD`
PKG_NAME_VER=${SHORTSHA}

OS_NAME=$(shell uname -s)

ifeq (${OS_NAME},FreeBSD)
make="gmake"
else
MAKE="make"
endif

compile:
	$(REBAR) compile

shell:
	$(REBAR) shell

clean:
	$(REBAR) clean

cover:
	$(REBAR) cover

test:
	$(REBAR) as test do eunit,ct

ci:
	$(REBAR) as test do eunit,ct,cover && $(REBAR) do xref, dialyzer
	$(REBAR) covertool generate
	#codecov --required -f _build/test/covertool/blockchain_http.covertool.xml

typecheck:
	$(REBAR) dialyzer

doc:
	$(REBAR) edoc

release:
	$(REBAR) as prod do release


start:
	cp -f .env ./_build/prod/rel/blockchain_http/
	./_build/prod/rel/blockchain_http/bin/blockchain_http start

stop:
	-./_build/prod/rel/blockchain_http/bin/blockchain_http stop

reset: stop
	rm -rf rm -rf ./_build/prod/rel/blockchain_http/log/*

console:
	./_build/prod/rel/blockchain_http/bin/blockchain_http remote_console
