ifeq ($(DEBUG), true)
	DEBUG_FLAG = -DDEBUG
else
  DEBUG_FLAG =
endif

all:
	@mkdir -p ebin
	@erlc -Wall -o ebin $(DEBUG_FLAG) src/db.erl src/map_reduce.erl

tests:
	@erlc -Wall -o test -DTEST $(DEBUG_FLAG) src/db.erl src/map_reduce.erl test/map_reduce_test.erl
	@erl -pa test -eval 'file:set_cwd("test"), eunit:test([db, map_reduce, map_reduce_test]), init:stop()'
