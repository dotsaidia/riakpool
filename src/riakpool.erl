%% -------------------------------------------------------------------
%%
%% riakpool
%%
%% A bounded, elastic pool of Riak Protocol Buffers client connections.
%%
%% Pool behaviour:
%%
%% 1. Reuse an available connection whenever possible.
%%
%% 2. If all connections are busy and the pool is below the configured
%%    maximum, create another connection.
%%
%% 3. If all connections are busy and the maximum has been reached,
%%    queue the requesting process until a connection is returned.
%%
%% 4. Connections are checked back into the pool after execute/1,
%%    including when the supplied function raises, exits or throws.
%%
%% 5. Dead or disconnected connections are discarded rather than
%%    returned to the available queue.
%%
%% The pool therefore grows according to concurrent demand, but never
%% exceeds riakpool_max_connections.
%%
%% Configuration example:
%%
%%     config :riakpool,
%%       riakpool_host: ~c"127.0.0.1",
%%       riakpool_port: 8087,
%%       riakpool_max_connections: 128
%%
%% Prior to calls to execute/1, the pool must be started either:
%%
%% 1. Through the riakpool application environment, or
%%
%% 2. By calling start_pool/0 or start_pool/2 after the application
%%    has started.
%%
%% -------------------------------------------------------------------

-module(riakpool).

-behaviour(gen_server).

%% -------------------------------------------------------------------
%% Public API
%% -------------------------------------------------------------------

-export([
    count/0,
    execute/1,
    start_link/0,
    start_pool/0,
    start_pool/2,
    stop/0
]).

%% -------------------------------------------------------------------
%% gen_server callbacks
%% -------------------------------------------------------------------

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).


%% -------------------------------------------------------------------
%% Constants
%% -------------------------------------------------------------------

-define(DEFAULT_HOST, "127.0.0.1").
-define(DEFAULT_PORT, 8087).

%% Conservative default.
%%
%% This is PER APPLICATION INSTANCE, not cluster-wide.
-define(DEFAULT_MAX_CONNECTIONS, 128).


%% -------------------------------------------------------------------
%% Types
%% -------------------------------------------------------------------

-type host() :: string() | atom().

%% -------------------------------------------------------------------
%% State
%% -------------------------------------------------------------------

-record(state, {
    host :: host(),

    port :: non_neg_integer(),

    %% Connections currently available for checkout.
    %%
    %% Checked-out connections are deliberately NOT in this queue.
    available = queue:new() :: queue:queue(),

    %% Total number of pool-created connections:
    %%
    %%     available + checked-out
    %%
    %% This value is periodically reconciled against the connection
    %% supervisor so that dead connection processes do not permanently
    %% corrupt the count.
    total = 0 :: non_neg_integer(),

    %% Hard per-application-node connection limit.
    max_connections = ?DEFAULT_MAX_CONNECTIONS :: pos_integer(),

    %% Callers waiting because the connection limit has been reached.
    waiters = queue:new() :: queue:queue()
}).


%% ===================================================================
%% Public API
%% ===================================================================

%% -------------------------------------------------------------------
%% count/0
%% -------------------------------------------------------------------

%% @doc
%% Returns the number of active Riak PB connection processes currently
%% supervised by riakpool_connection_sup.
%%
%% This includes both available and checked-out connections.
-spec count() -> non_neg_integer().
count() ->
    Props =
        supervisor:count_children(
            riakpool_connection_sup
        ),

    case proplists:get_value(active, Props) of

        N when is_integer(N) ->
            N;

        undefined ->
            0
    end.


%% -------------------------------------------------------------------
%% execute/1
%% -------------------------------------------------------------------

%% @doc
%% Checks out one connection, executes Fun(Pid), and always attempts to
%% return the connection to the pool afterwards.
%%
%% If all connections are busy and the maximum pool size has been
%% reached, this call waits until a connection becomes available.
%%
%% Example:
%%
%%     riakpool:execute(
%%         fun(Pid) ->
%%             riakc_pb_socket:ping(Pid)
%%         end
%%     ).
%%
%%     {ok, pong}
%%
-spec execute(fun((pid()) -> Value)) ->
    {ok, Value} |
    {error, term()}.
execute(Fun)
        when is_function(Fun, 1) ->

    case gen_server:call(
        ?MODULE,
        check_out,
        infinity
    ) of

        {ok, Pid} ->
            try
                {ok, Fun(Pid)}
            catch

                Class:Reason ->
                    {error, {Class, Reason}}

            after
                gen_server:cast(
                    ?MODULE,
                    {check_in, Pid}
                )
            end;

        {error, Reason} ->
            {error, Reason}
    end.


%% -------------------------------------------------------------------
%% start_link/0
%% -------------------------------------------------------------------

-spec start_link() ->
    {ok, pid()} |
    {error, term()}.
start_link() ->
    gen_server:start_link(
        {local, ?MODULE},
        ?MODULE,
        [],
        []
    ).


%% -------------------------------------------------------------------
%% start_pool/0
%% -------------------------------------------------------------------

%% @doc
%% Starts a Riak pool using the default endpoint.
-spec start_pool() ->
    ok |
    {error, term()}.
start_pool() ->
    start_pool(
        ?DEFAULT_HOST,
        ?DEFAULT_PORT
    ).


%% -------------------------------------------------------------------
%% start_pool/2
%% -------------------------------------------------------------------

%% @doc
%% Starts the pool using Host and Port.
%%
%% The maximum connection count is read from:
%%
%%     riakpool_max_connections
%%
-spec start_pool(
    host(),
    non_neg_integer()
) ->
    ok |
    {error, term()}.
start_pool(Host, Port)
        when is_integer(Port),
             Port > 0 ->

    gen_server:call(
        ?MODULE,
        {start_pool, Host, Port},
        infinity
    ).


%% -------------------------------------------------------------------
%% stop/0
%% -------------------------------------------------------------------

-spec stop() -> ok.
stop() ->
    gen_server:cast(
        ?MODULE,
        stop
    ).


%% ===================================================================
%% gen_server callbacks
%% ===================================================================

%% -------------------------------------------------------------------
%% init/1
%% -------------------------------------------------------------------

%% @hidden
init([]) ->
    process_flag(
        trap_exit,
        true
    ),

    MaxConnections =
        configured_max_connections(),

    case [
        application:get_env(
            riakpool,
            riakpool_host
        ),

        application:get_env(
            riakpool,
            riakpool_port
        )
    ] of

        [
            {ok, Host},
            {ok, Port}
        ]
        when is_integer(Port),
             Port > 0 ->

            case new_state(
                Host,
                Port,
                MaxConnections
            ) of

                State = #state{} ->
                    {ok, State};

                undefined ->
                    {ok, undefined}
            end;

        _ ->
            {ok, undefined}
    end.


%% -------------------------------------------------------------------
%% handle_call/3
%% -------------------------------------------------------------------

%% Start an uninitialized pool.
handle_call(
    {start_pool, Host, Port},
    _From,
    undefined
) ->
    MaxConnections =
        configured_max_connections(),

    case new_state(
        Host,
        Port,
        MaxConnections
    ) of

        State = #state{} ->
            {
                reply,
                ok,
                State
            };

        undefined ->
            {
                reply,
                {error, connection_error},
                undefined
            }
    end;


%% Pool already started.
handle_call(
    {start_pool, _Host, _Port},
    _From,
    State = #state{}
) ->
    {
        reply,
        {error, pool_already_started},
        State
    };


%% No usable pool exists.
handle_call(
    check_out,
    _From,
    undefined
) ->
    {
        reply,
        {error, pool_not_started},
        undefined
    };


%% Checkout from a running pool.
handle_call(
    check_out,
    From,
    State0 = #state{}
) ->
    State =
        reconcile_total(State0),

    checkout_connection(
        From,
        State
    );


%% Unknown synchronous calls.
handle_call(
    _Request,
    _From,
    State
) ->
    {
        reply,
        {error, unsupported_request},
        State
    }.


%% -------------------------------------------------------------------
%% handle_cast/2
%% -------------------------------------------------------------------

%% Ignore a delayed check-in if the pool has not been initialized.
handle_cast(
    {check_in, _Pid},
    undefined
) ->
    {
        noreply,
        undefined
    };


%% Return a connection to the pool.
handle_cast(
    {check_in, Pid},
    State0 = #state{}
) ->
    State =
        reconcile_total(State0),

    check_in_connection(
        Pid,
        State
    );


handle_cast(
    stop,
    State
) ->
    {
        stop,
        normal,
        State
    };


handle_cast(
    _Message,
    State
) ->
    {
        noreply,
        State
    }.


%% -------------------------------------------------------------------
%% handle_info/2
%% -------------------------------------------------------------------

%% The current implementation does not link the pool gen_server directly
%% to individual Riak PB connections. The connection supervisor owns
%% those children.
handle_info(
    _Info,
    State
) ->
    {
        noreply,
        State
    }.


%% -------------------------------------------------------------------
%% terminate/2
%% -------------------------------------------------------------------

terminate(
    _Reason,
    undefined
) ->
    ok;


terminate(
    _Reason,
    #state{
        available = Available
    }
) ->
    lists:foreach(
        fun maybe_stop_connection/1,
        queue:to_list(Available)
    ),

    ok.


%% -------------------------------------------------------------------
%% code_change/3
%% -------------------------------------------------------------------

code_change(
    _OldVersion,
    State,
    _Extra
) ->
    {
        ok,
        State
    }.


%% ===================================================================
%% Pool initialization
%% ===================================================================

%% -------------------------------------------------------------------
%% configured_max_connections/0
%% -------------------------------------------------------------------

-spec configured_max_connections() ->
    pos_integer().
configured_max_connections() ->
    case application:get_env(
        riakpool,
        riakpool_max_connections
    ) of

        {ok, Max}
        when is_integer(Max),
             Max > 0 ->
            Max;

        _ ->
            ?DEFAULT_MAX_CONNECTIONS
    end.


%% -------------------------------------------------------------------
%% new_state/3
%% -------------------------------------------------------------------

%% @doc
%% Creates the initial pool with one Riak PB connection.
%%
%% The maximum connection count is enforced per application instance.
-spec new_state(
    host(),
    non_neg_integer(),
    pos_integer()
) ->
    #state{} |
    undefined.
new_state(
    Host,
    Port,
    MaxConnections
) ->
    case new_connection(
        Host,
        Port
    ) of

        {ok, Pid} ->
            #state{
                host = Host,
                port = Port,
                available =
                    queue:in(
                        Pid,
                        queue:new()
                    ),
                total = 1,
                max_connections =
                    MaxConnections,
                waiters =
                    queue:new()
            };

        error ->
            undefined
    end.


%% ===================================================================
%% Checkout
%% ===================================================================

%% -------------------------------------------------------------------
%% checkout_connection/2
%% -------------------------------------------------------------------

-spec checkout_connection(
    gen_server:from(),
    #state{}
) ->
    {
        reply,
        {ok, pid()} |
        {error, term()},
        #state{}
    } |
    {
        noreply,
        #state{}
    }.
checkout_connection(
    From,
    State0 = #state{
        available = Available
    }
) ->
    case take_available_connection(
        Available
    ) of

        %% Reuse an existing healthy idle connection.
        {
            ok,
            Pid,
            NewAvailable,
            Discarded
        } ->
            NewTotal =
                safe_subtract(
                    State0#state.total,
                    Discarded
                ),

            State =
                State0#state{
                    available =
                        NewAvailable,
                    total =
                        NewTotal
                },

            {
                reply,
                {ok, Pid},
                State
            };


        %% No usable idle connection exists.
        {
            empty,
            NewAvailable,
            Discarded
        } ->
            Total1 =
                safe_subtract(
                    State0#state.total,
                    Discarded
                ),

            State1 =
                State0#state{
                    available =
                        NewAvailable,
                    total =
                        Total1
                },

            checkout_without_available(
                From,
                State1
            )
    end.


%% -------------------------------------------------------------------
%% checkout_without_available/2
%% -------------------------------------------------------------------

-spec checkout_without_available(
    gen_server:from(),
    #state{}
) ->
    {
        reply,
        {ok, pid()} |
        {error, term()},
        #state{}
    } |
    {
        noreply,
        #state{}
    }.
checkout_without_available(
    _From,
    State = #state{
        host = Host,
        port = Port,
        total = Total,
        max_connections = Max
    }
)
        when Total < Max ->

    case new_connection(
        Host,
        Port
    ) of

        {ok, Pid} ->
            {
                reply,
                {ok, Pid},
                State#state{
                    total =
                        Total + 1
                }
            };

        error ->
            {
                reply,
                {error, connection_error},
                State
            }
    end;


%% Pool capacity reached.
%%
%% The caller remains blocked inside gen_server:call/3 until a checked-out
%% connection is returned.
checkout_without_available(
    From,
    State = #state{
        waiters = Waiters
    }
) ->
    NewWaiters =
        queue:in(
            From,
            Waiters
        ),

    {
        noreply,
        State#state{
            waiters =
                NewWaiters
        }
    }.


%% ===================================================================
%% Check-in
%% ===================================================================

%% -------------------------------------------------------------------
%% check_in_connection/2
%% -------------------------------------------------------------------

-spec check_in_connection(
    pid(),
    #state{}
) ->
    {
        noreply,
        #state{}
    }.
check_in_connection(
    Pid,
    State
) ->
    case connection_usable(Pid) of

        true ->
            hand_connection_to_waiter_or_pool(
                Pid,
                State
            );

        false ->
            maybe_stop_connection(Pid),

            State1 =
                State#state{
                    total =
                        safe_subtract(
                            State#state.total,
                            1
                        )
                },

            serve_waiter_if_possible(
                State1
            )
    end.


%% -------------------------------------------------------------------
%% hand_connection_to_waiter_or_pool/2
%% -------------------------------------------------------------------

-spec hand_connection_to_waiter_or_pool(
    pid(),
    #state{}
) ->
    {
        noreply,
        #state{}
    }.
hand_connection_to_waiter_or_pool(
    Pid,
    State = #state{
        waiters = Waiters,
        available = Available
    }
) ->
    case queue:out(Waiters) of

        %% Somebody is already waiting.
        %%
        %% Transfer the connection directly rather than putting it into
        %% the available queue and immediately taking it out again.
        {
            {value, From},
            NewWaiters
        } ->
            gen_server:reply(
                From,
                {ok, Pid}
            ),

            {
                noreply,
                State#state{
                    waiters =
                        NewWaiters
                }
            };


        %% Nobody is waiting.
        %%
        %% Return the connection to the idle pool.
        {
            empty,
            _Queue
        } ->
            {
                noreply,
                State#state{
                    available =
                        queue:in(
                            Pid,
                            Available
                        )
                }
            }
    end.


%% -------------------------------------------------------------------
%% serve_waiter_if_possible/1
%% -------------------------------------------------------------------

%% A returned connection may have been dead/disconnected.
%%
%% If a caller is waiting and dropping the dead connection brought the
%% pool below its maximum, try to create a replacement immediately.
-spec serve_waiter_if_possible(
    #state{}
) ->
    {
        noreply,
        #state{}
    }.
serve_waiter_if_possible(
    State = #state{
        waiters = Waiters
    }
) ->
    case queue:out(Waiters) of

        {
            empty,
            _Queue
        } ->
            {
                noreply,
                State
            };


        {
            {value, From},
            NewWaiters
        } ->
            maybe_create_connection_for_waiter(
                From,
                NewWaiters,
                State
            )
    end.


%% -------------------------------------------------------------------
%% maybe_create_connection_for_waiter/3
%% -------------------------------------------------------------------

-spec maybe_create_connection_for_waiter(
    gen_server:from(),
    queue:queue(),
    #state{}
) ->
    {
        noreply,
        #state{}
    }.
maybe_create_connection_for_waiter(
    From,
    NewWaiters,
    State = #state{
        host = Host,
        port = Port,
        total = Total,
        max_connections = Max
    }
)
        when Total < Max ->

    case new_connection(
        Host,
        Port
    ) of

        {ok, Pid} ->
            gen_server:reply(
                From,
                {ok, Pid}
            ),

            {
                noreply,
                State#state{
                    total =
                        Total + 1,
                    waiters =
                        NewWaiters
                }
            };

        error ->
            %% Preserve FIFO order if a replacement cannot currently
            %% be created.
            RestoredWaiters =
                queue:in_r(
                    From,
                    NewWaiters
                ),

            {
                noreply,
                State#state{
                    waiters =
                        RestoredWaiters
                }
            }
    end;


maybe_create_connection_for_waiter(
    From,
    NewWaiters,
    State
) ->
    %% Capacity is still exhausted.
    %%
    %% Put the waiter back at the front of the queue.
    RestoredWaiters =
        queue:in_r(
            From,
            NewWaiters
        ),

    {
        noreply,
        State#state{
            waiters =
                RestoredWaiters
        }
    }.


%% ===================================================================
%% Available connection management
%% ===================================================================

%% -------------------------------------------------------------------
%% take_available_connection/1
%% -------------------------------------------------------------------

%% @doc
%% Searches the idle connection queue for a healthy connection.
%%
%% Dead or disconnected connections are discarded.
%%
%% Discarded contains the number of unusable connections removed so
%% that total accounting can be corrected.
-spec take_available_connection(
    queue:queue()
) ->
    {
        ok,
        pid(),
        queue:queue(),
        non_neg_integer()
    } |
    {
        empty,
        queue:queue(),
        non_neg_integer()
    }.
take_available_connection(Available) ->
    take_available_connection(
        Available,
        0
    ).


-spec take_available_connection(
    queue:queue(),
    non_neg_integer()
) ->
    {
        ok,
        pid(),
        queue:queue(),
        non_neg_integer()
    } |
    {
        empty,
        queue:queue(),
        non_neg_integer()
    }.
take_available_connection(
    Available,
    Discarded
) ->
    case queue:out(Available) of

        {
            {value, Pid},
            Rest
        } ->
            case connection_usable(Pid) of

                true ->
                    {
                        ok,
                        Pid,
                        Rest,
                        Discarded
                    };

                false ->
                    maybe_stop_connection(
                        Pid
                    ),

                    take_available_connection(
                        Rest,
                        Discarded + 1
                    )
            end;


        {
            empty,
            _Queue
        } ->
            {
                empty,
                Available,
                Discarded
            }
    end.


%% ===================================================================
%% Connection creation / validation
%% ===================================================================

%% -------------------------------------------------------------------
%% new_connection/2
%% -------------------------------------------------------------------

-spec new_connection(
    host(),
    non_neg_integer()
) ->
    {ok, pid()} |
    error.
new_connection(
    Host,
    Port
) ->
    case supervisor:start_child(
        riakpool_connection_sup,
        [
            Host,
            Port
        ]
    ) of

        {ok, Pid}
        when is_pid(Pid) ->
            {ok, Pid};


        {ok, Pid, _Info}
        when is_pid(Pid) ->
            {ok, Pid};


        _ ->
            error
    end.


%% -------------------------------------------------------------------
%% connection_usable/1
%% -------------------------------------------------------------------

-spec connection_usable(pid()) ->
    boolean().
connection_usable(Pid)
        when is_pid(Pid) ->

    case is_process_alive(Pid) of

        false ->
            false;


        true ->
            try
                case riakc_pb_socket:is_connected(
                    Pid
                ) of

                    true ->
                        true;

                    _ ->
                        false
                end

            catch
                _:_ ->
                    false
            end
    end.


%% -------------------------------------------------------------------
%% maybe_stop_connection/1
%% -------------------------------------------------------------------

-spec maybe_stop_connection(pid()) ->
    ok.
maybe_stop_connection(Pid)
        when is_pid(Pid) ->

    case is_process_alive(Pid) of

        true ->
            try
                riakc_pb_socket:stop(
                    Pid
                )
            catch
                _:_ ->
                    ok
            end;

        false ->
            ok
    end.


%% ===================================================================
%% Accounting
%% ===================================================================

%% -------------------------------------------------------------------
%% reconcile_total/1
%% -------------------------------------------------------------------

%% @doc
%% Reconciles our total with the connection supervisor.
%%
%% This protects the pool against stale accounting if a PB connection
%% process died outside the normal check-in path.
-spec reconcile_total(#state{}) ->
    #state{}.
reconcile_total(
    State = #state{}
) ->
    ActualTotal =
        count(),

    State#state{
        total =
            ActualTotal
    }.


%% -------------------------------------------------------------------
%% safe_subtract/2
%% -------------------------------------------------------------------

-spec safe_subtract(
    non_neg_integer(),
    non_neg_integer()
) ->
    non_neg_integer().
safe_subtract(
    Value,
    Amount
)
        when Value >= Amount ->
    Value - Amount;


safe_subtract(
    _Value,
    _Amount
) ->
    0.
