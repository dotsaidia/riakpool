%% @author David Weldon
%% @doc riakpool implements a pool of riak protocol buffer clients. In order to
%% use a connection, a call to {@link execute/1} must be made. This will check
%% out a connection from the pool to use it, however before using it, it is kept 
%% usage for other requests at the same time by checking it back in. This ensures
%% that a given connection can be in use by by several external process at a time.
%% If no existing connections are found, a new one will be established. A maximum
%% allowed connections in a pool is tracked.
%%
%% Prior to any calls to {@link execute/1}, the pool must be started. This can
%% be accomplished in one of two ways:
%%
%% 1. Before the server is started, set the riakpool application environment
%%    variables `riakpool_host' and `riakpool_port'.
%%
%% 2. After the server is started, call {@link start_pool/0} or
%%    {@link start_pool/2}

-module(riakpool).
-behaviour(gen_server).
-export([count/0,
         execute/1,
         start_link/0,
         start_pool/0,
         start_pool/2,
         stop/0]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(MAX_ALLOWED_CONNECTIONS, 512).

-type host() :: string() | atom().

-record(state, {host :: host(), port ::non_neg_integer(), pids, no_of_connections = 0 ::non_neg_integer()}).

%% @doc Returns the number of connections as seen by the supervisor.
-spec count() -> integer().
count() ->
    Props = supervisor:count_children(riakpool_connection_sup),
    case proplists:get_value(active, Props) of
        N when is_integer(N) -> N;
        undefined -> 0
    end.

%% @doc Finds the next available connection pid from the pool and calls
%% `Fun(Pid)'. Returns `{ok, Value}' if the call was successful, and
%% `{error, any()}' otherwise. If no connection could be found, a new connection
%% will be established.
%% ```
%% > riakpool:execute(fun(C) -> riakc_pb_socket:ping(C) end).
%% {ok,pong}
%% '''
-spec execute(fun((pid()) -> any())) -> {ok, Value::any()} | {error, any()}.
execute(Fun) ->
    case gen_server:call(?MODULE, check_out, infinity) of
        {ok, Pid} ->
            try {ok, Fun(Pid)}
            catch _:E -> {error, E}
            end;
        {error, E} -> {error, E}
    end.

%% @doc Starts the server.
-spec start_link() -> {ok, pid()} | {error, any()}.
start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Starts a connection pool to a server listening on {"127.0.0.1", 8087}.
%% Note that a pool can only be started once.
-spec start_pool() -> ok | {error, any()}.
start_pool() -> start_pool("127.0.0.1", 8087).

%% @doc Starts a connection pool to a server listening on {`Host', `Port'}.
%% Note that a pool can only be started once.
-spec start_pool(host(), integer()) -> ok | {error, any()}.
start_pool(Host, Port) when is_integer(Port) ->
    gen_server:call(?MODULE, {start_pool, Host, Port}).

%% @doc Stops the server.
-spec stop() -> ok.
stop() -> gen_server:cast(?MODULE, stop).

%% @hidden
init([]) ->
    process_flag(trap_exit, true),
    case [application:get_env(P) || P <- [riakpool_host, riakpool_port]] of
        [{ok, Host}, {ok, Port}] when is_integer(Port) ->
            {ok, new_state(Host, Port)};
        _ -> {ok, undefined}
    end.

%% @hidden
handle_call({start_pool, Host, Port}, _From, undefined) ->
    case new_state(Host, Port) of
        State=#state{} -> {reply, ok, State};
        undefined -> {reply, {error, connection_error}, undefined}
    end;
handle_call({start_pool, _Host, _Port}, _From, State=#state{}) ->
    {reply, {error, pool_already_started}, State};
handle_call(check_out, _From, undefined) ->
    {reply, {error, pool_not_started}, undefined};
handle_call(check_out, _From, State=#state{host=Host, port=Port, pids=Pids, no_of_connections=Qlen}) ->
    case next_pid(Host, Port, Pids, Qlen) of
        {ok, Pid, NewPids, NewQlen} -> {reply, {ok, Pid}, State#state{pids=NewPids, no_of_connections=NewQlen}};
        {error, NewPids, NewQlen} ->
            {reply, {error, connection_error}, State#state{pids=NewPids, no_of_connections=NewQlen}}
    end;
handle_call(_Request, _From, State) -> {reply, ok, State}.

%% @hidden
handle_cast({check_in, Pid}, State=#state{pids=Pids}) ->
    NewPids = queue:in(Pid, Pids),
    {noreply, State#state{pids=NewPids}};
handle_cast(stop, State) -> {stop, normal, State};
handle_cast(_Msg, State) -> {noreply, State}.

%% @hidden
handle_info(_Info, State) -> {noreply, State}.

%% @hidden
terminate(_Reason, undefined) -> ok;
terminate(_Reason, #state{pids=Pids}) ->
    StopFun =
        fun(Pid) ->
            case is_process_alive(Pid) of
                true -> riakc_pb_socket:stop(Pid);
                false -> ok
            end
        end,
    [StopFun(Pid) || Pid <- queue:to_list(Pids)], ok.

%% @hidden
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% @doc Returns a state with a single pid if a connection could be established,
%% otherwise returns undefined.
-spec new_state(host(), integer()) -> #state{} | undefined.
new_state(Host, Port) ->
    case new_connection(Host, Port) of
        {ok, Pid} ->
            #state{host=Host, port=Port, pids=queue:in(Pid, queue:new()), no_of_connections = 1};
        error -> undefined
    end.

%% @doc Returns {ok, Pid} if a new connection was established and added to the
%% supervisor, otherwise returns error.
-spec new_connection(host(), integer()) -> {ok, pid()} | error.
new_connection(Host, Port) ->
    case supervisor:start_child(riakpool_connection_sup, [Host, Port]) of
        {ok, Pid} when is_pid(Pid) -> {ok, Pid};
        {ok, Pid, _} when is_pid(Pid) -> {ok, Pid};
        _ -> error
    end.

%% @doc Recursively dequeues Pids in search of a live connection. Once alive connection 
%% is got, it returned and also put back at the end of the queue to be resused. Dead
%% connections are removed from the queue as it is searched. A MAX_ALLOWED_CONNECTIONS is
%% maintained. If the connections goes below the maximum, a new one will be established. 
%% Returns {ok, Pid, queue:in(Pid, NewPids), NoOfConnections} where NewPids is the queue 
%% after any necessary dequeues. Returns error if no live connection could be found and no 
%% new connection could be established. For effeciency, we track the number of connections
%% instead of determining it every time we need a connection.
-spec next_pid(host(), integer(), queue:queue(), integer()) -> {ok, pid(), queue:queue(), integer()} |
                                                    {error, queue:queue(), integer()}.
next_pid(Host, Port, Pids, NoOfConnections) ->
    case NoOfConnections < ?MAX_ALLOWED_CONNECTIONS of
        true ->
            case new_connection(Host, Port) of
                {ok, Pid} -> {ok, Pid, queue:in(Pid, Pids), NoOfConnections + 1};
                error -> {error, Pids, NoOfConnections}
            end;
        false ->
            {{value, Pid}, NewPids} = queue:out(Pids),
            try riakc_pb_socket:is_connected(Pid) of
                true -> 
                    % A connection can be used for several requests at a time
                    {ok, Pid, queue:in(Pid, NewPids), NoOfConnections};
                {false, _List} ->
                    % Means the connection has failed to be established, but the process is alive.
                    % We kick it off.
                    ok = riakc_pb_socket:stop(Pid),
                    next_pid(Host, Port, NewPids, NoOfConnections - 1)
            catch _:_ ->
                % Means the connection process is dead, so we kick it off
                next_pid(Host, Port, NewPids, NoOfConnections - 1)
            end
    end.
