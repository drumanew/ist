-module(map_reduce).

-export([start/1]).

-define(DELIMITERS, [
                      $ 
                    , $\r
                    , $\n
                    , $\t

                    , $~
                    , $`
                    , $!
                    , $@
                    , $#
                    , $$
                    , $%
                    , $^
                    , $&
                    , $*
                    , $(
                    , $)
                    , $_
                    , $-
                    , $+
                    , $=
                    , ${
                    , $[
                    , $}
                    , $]
                    , $|
                    , $\\
                    , $:
                    , $;
                    , $"
                    , $'
                    , $<
                    , $,
                    , $>
                    , $.
                    , $?
                    , $/

                    , $1, $2, $3, $4, $5, $6, $7, $8, $9, $0

                    %% TODO
                    ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

start(Files) ->
    Self = self(),
    log("start reduce process~n"),
    Reduce = spawn_link(fun () -> reduce(Self) end),
    receive
        {started, Reduce} ->
            log("reduce process started: ~p~n", [Reduce]),
            ok
    end,
    log("start map processes~n"),
    Pids = [ spawn_link(fun () -> map(File, Reduce, Self) end) || File <- Files ],
    ok = wait_all(Pids),
    retrieve(Reduce).

%% private

reduce(Parent) ->
    Parent ! {started, self()},
    reduce(Parent, #{}).

reduce(Parent, Acc) ->
    receive
        {data, Data} ->
            reduce(Parent, merge(Acc, Data));
        {get, Ref, From} ->
            From ! {reply, Ref, Acc}
    end.

map(File, Reduce, Parent) ->
    Reduce ! {data, parse(File)},
    Parent ! {done, Ref = erlang:make_ref(), self()},
    receive
        {ok, Ref} -> ok
    end.

wait_all([]) ->
    ok;

wait_all(Pids) ->
    receive
        {done, Ref, Pid} ->
            Pid ! {ok, Ref},
            log("map process ~p done~n", [Pid]),
            wait_all(Pids -- [Pid])
    end.

retrieve(Reduce) ->
    Reduce ! {get, Ref = erlang:make_ref(), self()},
    receive
        {reply, Ref, Data} -> Data
    end.

merge(A, B) ->
    maps:fold(fun(K, V, Map) ->
                  maps:update_with(K, fun(X) -> X + V end, V, Map)
              end, A, B).

parse(File) ->
    case file:read_file(File) of
        {ok, Binary} ->
            word_stats(unicode:characters_to_nfc_list(Binary));
        _ ->
            #{}
    end.

word_stats(CharList) ->
    Words = string:lexemes(CharList, ?DELIMITERS),
    lists:foldl(fun (Word, Acc) ->
                    maps:update_with(Word, fun (X) -> X + 1 end, 1, Acc)
                end, #{}, Words).


log(Format) ->
    log(Format, []).

-ifdef(DEBUG).

log(Format, Args) ->
    io:format(standard_error, Format, Args).

-else.

log(_, _) -> ok.

-endif.

-ifdef(TEST).

simple_test_() ->
    Tmp = "test.tmp",
    Example = <<"a b c\n"
                "a,b,c,"
                "a-b-c">>,
    Setup = fun () -> ok = file:write_file(Tmp, Example) end,
    Cleanup = fun (_) -> ok = file:delete(Tmp) end,
    Tests =
        [
          ?_assertEqual(#{ "a" => 3, "b" => 3, "c" => 3 }, start([Tmp]))
        ],
    {setup, Setup, Cleanup, Tests}.

-endif.
