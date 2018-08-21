all:
	@mkdir -p ebin
	@erlc -Wall -o ebin src/db.erl

tests:
	@erlc -Wall -o ebin -DTEST src/db.erl
	@erl -pa ebin -eval 'eunit:test(db), init:stop()'

