-module(db).

-export([start_link/0,
         stop/0]).

-export([new/1,
         create/2,
         read/2,
         update/2,
         delete/2]).

-type db_name() :: string().
-type db_key()  :: integer().
-type db_username() :: string().
-type db_city() :: string().
-type db_record() :: {db_key(), db_username(), db_city()}.

-spec new(DdName :: db_name()) ->
      ok.

-spec create(Record :: db_record(), DbName :: db_name()) ->
      {ok, Record :: db_record()} |
      {error, Reason :: term()}.

-spec read(Key :: db_key(), DbName :: db_name()) ->
      {ok, Record :: db_record()} |
      {error, Reason :: term()}.

-spec update(Record :: db_record(), DbName :: db_name()) ->
      {ok, Record :: db_record()} |
      {error, Reason :: term()}.

-spec delete(Key :: db_key(), DbName :: db_name()) ->
      ok |
      {error, Reason :: term()}.

-behaviour(gen_server).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         code_change/3,
         terminate/2]).

-record(state, {tabs = #{}}).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Start / stop

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:stop(?MODULE).

%% CRUD API

new(DbName) ->
    call({new, DbName}).

create(Record, DbName) ->
    call({create, Record, DbName}).

read(Key, DbName) ->
    call({read, Key, DbName}).

update(Record, DbName) ->
    call({update, Record, DbName}).

delete(Key, DbName) ->
    call({delete, Key, DbName}).

%% gen_server callbacks

init([]) ->
    {ok, #state{}}.

handle_call({new, DbName}, _From, State = #state{ tabs = Tabs }) ->
    {reply, ok, State#state{ tabs = Tabs#{ DbName => #{} } }};

handle_call({create, {Key, User, City} = Record, DbName}, _From, State = #state{ tabs = Tabs }) ->
    {Reply, NewState} =
        case Tabs of
            #{ DbName := Tab } ->
                case Tab of
                    #{ Key := _  } ->
                        {{error, key_exists}, State};
                    _ ->
                        {{ok, Record}, State#state{ tabs = Tabs#{ DbName => Tab#{ Key => {User, City} } } }}
                end;
            _ ->
                {{error, db_not_exist}, State}
        end,
    {reply, Reply, NewState};

handle_call({read, Key, DbName}, _From, State = #state{ tabs = Tabs }) ->
    Reply =
        case Tabs of
            #{ DbName := Tab } ->
                case Tab of
                    #{ Key := {User, City} } ->
                        {ok, {Key, User, City}};
                    _ ->
                        {error, key_not_exist}
                end;
            _ ->
                {error, db_not_exist}
        end,
    {reply, Reply, State};

handle_call({update, {Key, User, City} = Record, DbName}, _From, State = #state{ tabs = Tabs }) ->
    {Reply, NewState} =
        case Tabs of
            #{ DbName := Tab } ->
                case Tab of
                    #{ Key := _  } ->
                        {{ok, Record}, State#state{ tabs = Tabs#{ DbName => Tab#{ Key => {User, City} } } }};
                    _ ->
                        {{error, key_not_exist}, State}
                end;
            _ ->
                {{error, db_not_exist}, State}
        end,
    {reply, Reply, NewState};

handle_call({delete, Key, DbName}, _From, State = #state{ tabs = Tabs }) ->
    {Reply, NewState} =
        case Tabs of
            #{ DbName := Tab } ->
                case Tab of
                    #{ Key := _  } ->
                        {ok, State#state{ tabs = Tabs#{ DbName => maps:remove(Key, Tab) } }};
                    _ ->
                        {{error, key_not_exist}, State}
                end;
            _ ->
                {{error, db_not_exist}, State}
        end,
    {reply, Reply, NewState};

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

code_change(_Vsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% private

call(Request) ->
  gen_server:call(?MODULE, Request, infinity).

%% Tests

-ifdef(TEST).

simple_test_() ->
    Setup = fun () -> {ok, _} = start_link() end,
    Cleanup = fun (_) -> stop() end,
    Db1 = "test1",
    Db2 = "test2",
    K1 = 0,
    K2 = 1,
    R1_1 = {K1, "John", "New York"},
    R1_2 = {K1, "Bob", "Los Angeles"},
    R2 = {K2, "Bill", "Kickapoo"},
    Tests =
      [
        ?_assertEqual(ok, new(Db1))

      , ?_assertEqual({ok, R1_1}, create(R1_1, Db1))
      , ?_assertEqual({error, db_not_exist}, create(R1_1, Db2))
      , ?_assertEqual({error, key_exists}, create(R1_1, Db1))

      , ?_assertEqual({ok, R1_1}, read(K1, Db1))
      , ?_assertEqual({error, db_not_exist}, read(K1, Db2))
      , ?_assertEqual({error, key_not_exist}, read(K2, Db1))

      , ?_assertEqual({ok, R1_2}, update(R1_2, Db1))
      , ?_assertEqual({error, db_not_exist}, update(R1_2, Db2))
      , ?_assertEqual({error, key_not_exist}, update(R2, Db1))

      , ?_assertEqual(ok, delete(K1, Db1))
      , ?_assertEqual({error, db_not_exist}, delete(K1, Db2))
      , ?_assertEqual({error, key_not_exist}, delete(K2, Db1))
      ],
    {setup, Setup, Cleanup, Tests}.

-endif.
