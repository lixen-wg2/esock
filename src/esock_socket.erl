-module(esock_socket).
-behaviour(gen_server).

-include("esock.hrl").
-include_lib("kernel/include/inet_sctp.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
    start_link/1,
    normalize_config/1,
    acquire/3,
    release/2,
    socknames/1,
    register_peer/4,
    unregister_peer/3,
    connect/4,
    cancel_connect/3,
    accept/5
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(DEFAULT_PENDING_TIMEOUT, 10000).
-define(DEFAULT_MAX_PENDING, 1024).
-define(STOP_LINGER, 1000).

-record(route, {
    ref :: reference(),
    lease :: reference(),
    acceptor :: pid(),
    monitor :: reference(),
    remote_addrs :: [inet:ip_address()] | any,
    remote_port :: inet:port_number() | any
}).

-record(pending, {
    ref :: reference(),
    socket :: term(),
    assoc_id :: non_neg_integer(),
    info :: esock:peer_info(),
    timer :: reference() | undefined,
    peer_ref = undefined :: reference() | undefined,
    acceptor = undefined :: pid() | undefined,
    monitor = undefined :: reference() | undefined,
    lease = undefined :: reference() | undefined
}).

-record(connect, {
    ref :: reference(),
    lease :: reference(),
    owner :: pid(),
    monitor :: reference() | undefined,
    assoc_id = undefined :: gen_sctp:assoc_id() | undefined,
    remote_addrs :: [inet:ip_address()],
    remaining :: [inet:ip_address()],
    current :: inet:ip_address(),
    remote_port :: inet:port_number(),
    options = [] :: [gen_sctp:option()],
    errors = [] :: [term()],
    canceled = false :: boolean(),
    started = false :: boolean(),
    async = undefined :: term()
}).

-record(state, {
    config :: map(),
    socket :: term(),
    local_addrs :: [inet:ip_address()],
    local_port :: inet:port_number(),
    leases = #{} :: #{reference() => {pid(), reference()}},
    routes = [] :: [#route{}],
    pending = #{} :: #{reference() => #pending{}},
    pending_order :: queue:queue(reference()),
    connects = [] :: [#connect{}],
    stop_timer = undefined :: {reference(), reference()} | undefined,
    backend = gen_sctp :: gen_sctp | socket
}).

start_link(Config) ->
    gen_server:start_link(?MODULE, Config, []).

normalize_config(Options) when is_map(Options) ->
    LocalAddrs0 = maps:get(local_addrs, Options, [loopback]),
    LocalPort = maps:get(local_port, Options, 0),
    SocketOptions = maps:get(socket_options, Options, []),
    PendingTimeout = maps:get(pending_timeout, Options, ?DEFAULT_PENDING_TIMEOUT),
    MaxPending = maps:get(max_pending, Options, ?DEFAULT_MAX_PENDING),
    Backend = maps:get(backend, Options, gen_sctp),
    case
        normalize_local_config(
            LocalAddrs0, LocalPort, SocketOptions, PendingTimeout, MaxPending, Backend
        )
    of
        {ok, LocalAddrs} ->
            case backend_supported(Backend) of
                true ->
                    Key =
                        case LocalPort of
                            0 -> {unique, make_ref()};
                            _ -> {sctp, Backend, LocalAddrs, LocalPort}
                        end,
                    Config = #{
                        key => Key,
                        backend => Backend,
                        local_addrs => LocalAddrs,
                        local_port => LocalPort,
                        socket_options => SocketOptions,
                        pending_timeout => PendingTimeout,
                        max_pending => MaxPending
                    },
                    {ok, Key, Config};
                false ->
                    {error, {unsupported_backend, Backend}}
            end;
        {error, _} = Error ->
            Error
    end;
normalize_config(Options) ->
    {error, {invalid_socket_options, Options}}.

backend_supported(gen_sctp) -> true;
backend_supported(socket) -> esock_socket_api:supported().

acquire(Pid, Owner, Config) ->
    call(Pid, {acquire, Owner, Config}).

release(Pid, Lease) ->
    call(Pid, {release, Lease}).

socknames(Pid) ->
    call(Pid, socknames).

register_peer(Pid, Lease, Options, Caller) ->
    call(Pid, {register_peer, Lease, Options, Caller}).

unregister_peer(Pid, Lease, PeerRef) ->
    call(Pid, {unregister_peer, Lease, PeerRef}).

connect(Pid, Lease, Options, Caller) ->
    call(Pid, {connect, Lease, Options, Caller}).

cancel_connect(Pid, Lease, ConnectRef) ->
    call(Pid, {cancel_connect, Lease, ConnectRef}).

accept(Pid, Lease, PendingRef, Owner, Options) ->
    call(Pid, {accept, Lease, PendingRef, Owner, Options}).

init(Config) ->
    process_flag(trap_exit, true),
    case open(Config) of
        {ok, Socket, LocalAddrs, LocalPort} ->
            State = #state{
                config = Config,
                socket = Socket,
                local_addrs = LocalAddrs,
                local_port = LocalPort,
                pending_order = queue:new(),
                backend = maps:get(backend, Config)
            },
            {ok, rearm(State)};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call({acquire, Owner, Config}, _From, State0) ->
    case compatible(Config, State0#state.config) of
        true ->
            Lease = make_ref(),
            Monitor = erlang:monitor(process, Owner),
            Leases = (State0#state.leases)#{Lease => {Owner, Monitor}},
            State = cancel_stop(State0#state{leases = Leases}),
            {reply, {ok, #esock_socket_ref{pid = self(), lease = Lease}}, State};
        false ->
            {reply, {error, {socket_option_conflict, State0#state.config, Config}}, State0}
    end;
handle_call({release, Lease}, _From, State0) ->
    State = maybe_schedule_stop(remove_lease(Lease, State0)),
    {reply, ok, State};
handle_call(socknames, _From, State) ->
    Reply = {ok, {State#state.local_addrs, State#state.local_port}},
    {reply, Reply, State};
handle_call({register_peer, Lease, Options, Caller}, _From, State0) ->
    case maps:is_key(Lease, State0#state.leases) of
        false ->
            {reply, {error, invalid_lease}, State0};
        true ->
            register_peer_route(Lease, Options, Caller, State0)
    end;
handle_call({unregister_peer, Lease, PeerRef}, _From, State0) ->
    case take_route(PeerRef, State0#state.routes) of
        {value, #route{lease = Lease} = Route, Routes} ->
            erlang:demonitor(Route#route.monitor, [flush]),
            {reply, ok, State0#state{routes = Routes}};
        {value, _Route, _Routes} ->
            {reply, {error, not_owner}, State0};
        false ->
            {reply, {error, not_found}, State0}
    end;
handle_call({connect, Lease, Options, Caller}, _From, State0) ->
    case maps:is_key(Lease, State0#state.leases) of
        false ->
            {reply, {error, invalid_lease}, State0};
        true ->
            start_connect(Lease, Options, Caller, State0)
    end;
handle_call({cancel_connect, Lease, ConnectRef}, _From, State0) ->
    case take_connect(ConnectRef, State0#state.connects) of
        {value, #connect{lease = Lease, started = false} = Connect, Connects} ->
            maybe_demonitor(Connect#connect.monitor),
            {reply, ok, maybe_start_queued(State0#state{connects = Connects})};
        {value, #connect{lease = Lease} = Connect, Connects} ->
            Canceled = cancel_connect_record(
                State0#state.backend, State0#state.socket, Connect
            ),
            State = maybe_start_queued(State0#state{connects = [Canceled | Connects]}),
            {reply, ok, State};
        {value, _Connect, _Connects} ->
            {reply, {error, not_owner}, State0};
        false ->
            {reply, {error, not_found}, State0}
    end;
handle_call({accept, Lease, PendingRef, Owner, Options}, {Caller, _Tag}, State0) ->
    accept_pending(Lease, Caller, PendingRef, Owner, Options, State0);
handle_call(_Request, _From, State) ->
    {reply, {error, bad_request}, State}.

handle_cast(_Message, State) ->
    {noreply, State}.

handle_socket_message(Message, State0) ->
    case take_connect_by_async(Message, State0#state.connects) of
        {value, Connect, Connects} ->
            handle_connect_message(Message, Connect, Connects, State0);
        false ->
            handle_listener_message(Message, State0)
    end.

handle_connect_message(Message, Connect, Connects, State0) ->
    Result = connect_message_result(State0#state.socket, Message),
    case Result of
        {ok, AssocId} ->
            Updated = Connect#connect{assoc_id = AssocId, async = undefined},
            maybe_start_queued(State0#state{connects = [Updated | Connects]});
        {error, _Reason} when Connect#connect.canceled =:= true ->
            maybe_start_queued(State0#state{connects = Connects});
        {error, Reason} ->
            Error = {Connect#connect.current, Reason},
            Retry = Connect#connect{
                assoc_id = undefined,
                async = undefined,
                errors = [Error | Connect#connect.errors]
            },
            case connect_next(Retry, State0) of
                {ok, Next} ->
                    maybe_start_queued(State0#state{connects = [Next | Connects]});
                {error, ConnectReason} ->
                    maybe_demonitor(Connect#connect.monitor),
                    Connect#connect.owner !
                        {esock, Connect#connect.ref, {error, ConnectReason}},
                    maybe_start_queued(State0#state{connects = Connects})
            end
    end.

connect_message_result(Socket, {'$socket', Socket, select, _Handle}) ->
    esock_socket_api:finish_connect(Socket);
connect_message_result(_Socket, {'$socket', _SocketRef, completion, {_Handle, ok}}) ->
    {ok, undefined};
connect_message_result(_Socket, {'$socket', _SocketRef, completion, {_Handle, {ok, AssocId}}}) ->
    {ok, AssocId};
connect_message_result(_Socket, {'$socket', _SocketRef, completion, {_Handle, {error, Reason}}}) ->
    {error, Reason};
connect_message_result(_Socket, {'$socket', _SocketRef, abort, {_Handle, Reason}}) ->
    {error, Reason}.

handle_listener_message(Message, State0) ->
    case esock_socket_api:listener_message(State0#state.socket, Message) of
        {ok, {event, RemoteAddr, RemotePort, Event}} ->
            rearm(handle_sctp(RemoteAddr, RemotePort, {[], Event}, State0));
        {ok, {data, RemoteAddr, RemotePort, Info, Data}} ->
            rearm(handle_sctp(RemoteAddr, RemotePort, {[Info], Data}, State0));
        pending ->
            State0;
        ignore ->
            State0;
        {error, closed} ->
            exit({shutdown, {socket_closed, closed}});
        {error, Reason} ->
            exit({recvmsg, Reason})
    end.

take_connect_by_async(Message, Connects) ->
    case socket_message_handle(Message) of
        undefined ->
            false;
        Handle ->
            take_first(
                fun(Connect) -> async_handle(Connect#connect.async) =:= Handle end,
                Connects
            )
    end.

socket_message_handle({'$socket', _Socket, select, Handle}) -> Handle;
socket_message_handle({'$socket', _Socket, completion, {Handle, _Result}}) -> Handle;
socket_message_handle({'$socket', _Socket, abort, {Handle, _Reason}}) -> Handle;
socket_message_handle(_Message) -> undefined.

async_handle({select_info, _Operation, Handle}) -> Handle;
async_handle({completion_info, _Operation, Handle}) -> Handle;
async_handle(_) -> undefined.

cancel_async(#connect{async = undefined}, State) ->
    State;
cancel_async(#connect{async = Async}, #state{backend = socket, socket = Socket} = State) ->
    _ = esock_socket_api:cancel(Socket, Async),
    State.

handle_info({sctp, Socket, RemoteAddr, RemotePort, Data}, #state{socket = Socket} = State0) ->
    State = handle_sctp(RemoteAddr, RemotePort, Data, State0),
    {noreply, rearm(State)};
handle_info(
    {'$socket', Socket, _Tag, _Info} = Message,
    #state{backend = socket, socket = Socket} = State0
) ->
    {noreply, handle_socket_message(Message, State0)};
handle_info({pending_timeout, PendingRef}, State0) ->
    {noreply, drop_pending(PendingRef, State0)};
handle_info(
    {'EXIT', Socket, Reason},
    #state{backend = gen_sctp, socket = Socket} = State
) ->
    {stop, {shutdown, {socket_closed, Reason}}, State};
handle_info(
    {stop_if_unused, StopRef},
    #state{stop_timer = {StopRef, _TimerRef}, leases = Leases} = State
) ->
    case map_size(Leases) of
        0 -> {stop, normal, State#state{stop_timer = undefined}};
        _ -> {noreply, State#state{stop_timer = undefined}}
    end;
handle_info({'DOWN', Monitor, process, _Pid, _Reason}, State0) ->
    State1 = remove_monitor(Monitor, State0),
    {noreply, maybe_schedule_stop(State1)};
handle_info(_Message, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    maps:foreach(
        fun(_Ref, #pending{socket = Socket}) ->
            close_socket(State#state.backend, Socket)
        end,
        State#state.pending
    ),
    close_socket(State#state.backend, State#state.socket),
    try esock_registry:socket_down(self()) of
        ok -> ok
    catch
        _:_ -> ok
    end,
    ok.

call(Pid, Request) ->
    try gen_server:call(Pid, Request, infinity) of
        Reply -> Reply
    catch
        exit:{noproc, _} -> {error, socket_stopping};
        exit:{normal, _} -> {error, socket_stopping};
        exit:{shutdown, _} -> {error, socket_stopping}
    end.

normalize_local_config(
    LocalAddrs0, LocalPort, SocketOptions, PendingTimeout, MaxPending, Backend
) when
    is_list(LocalAddrs0),
    LocalAddrs0 =/= [],
    is_integer(LocalPort),
    0 =< LocalPort,
    LocalPort =< 65535,
    is_list(SocketOptions),
    (Backend =:= gen_sctp orelse Backend =:= socket),
    (is_integer(PendingTimeout) andalso 0 < PendingTimeout) orelse PendingTimeout =:= infinity,
    (is_integer(MaxPending) andalso 0 < MaxPending) orelse MaxPending =:= infinity
->
    case normalize_addresses(LocalAddrs0) of
        {ok, LocalAddrs} ->
            case same_family(LocalAddrs) andalso valid_socket_options(SocketOptions) of
                true -> {ok, LocalAddrs};
                false -> {error, {invalid_socket_options, SocketOptions}}
            end;
        {error, _} = Error ->
            Error
    end;
normalize_local_config(
    LocalAddrs, LocalPort, SocketOptions, PendingTimeout, MaxPending, Backend
) ->
    {error,
        {invalid_socket_options, #{
            local_addrs => LocalAddrs,
            local_port => LocalPort,
            socket_options => SocketOptions,
            pending_timeout => PendingTimeout,
            max_pending => MaxPending,
            backend => Backend
        }}}.

normalize_addresses(Addrs) ->
    try
        {ok, lists:usort([normalize_address(Addr) || Addr <- Addrs])}
    catch
        throw:{invalid_address, _} = Error -> {error, Error}
    end.

normalize_address(loopback) ->
    {127, 0, 0, 1};
normalize_address(any) ->
    {0, 0, 0, 0};
normalize_address({A, B, C, D} = Address) when
    is_integer(A),
    0 =< A,
    A =< 255,
    is_integer(B),
    0 =< B,
    B =< 255,
    is_integer(C),
    0 =< C,
    C =< 255,
    is_integer(D),
    0 =< D,
    D =< 255
->
    Address;
normalize_address({A, B, C, D, E, F, G, H} = Address) when
    is_integer(A),
    0 =< A,
    A =< 65535,
    is_integer(B),
    0 =< B,
    B =< 65535,
    is_integer(C),
    0 =< C,
    C =< 65535,
    is_integer(D),
    0 =< D,
    D =< 65535,
    is_integer(E),
    0 =< E,
    E =< 65535,
    is_integer(F),
    0 =< F,
    F =< 65535,
    is_integer(G),
    0 =< G,
    G =< 65535,
    is_integer(H),
    0 =< H,
    H =< 65535
->
    Address;
normalize_address(Address) ->
    throw({invalid_address, Address}).

same_family([First | Rest]) ->
    Size = tuple_size(First),
    lists:all(fun(Address) -> tuple_size(Address) =:= Size end, Rest).

valid_socket_options(Options) ->
    Reserved = [active, binary, list, mode, ip, ifaddr, port, sctp_events, type],
    not lists:any(
        fun
            ({Key, _}) -> lists:member(Key, Reserved);
            (Key) -> lists:member(Key, Reserved)
        end,
        Options
    ).

compatible(New, Existing) ->
    Keys = [backend, local_addrs, local_port, socket_options, pending_timeout, max_pending],
    maps:with(Keys, New) =:= maps:with(Keys, Existing).

open(Config) ->
    case maps:get(backend, Config) of
        gen_sctp -> open_gen_sctp(Config);
        socket -> open_socket_api(Config)
    end.

open_gen_sctp(Config) ->
    LocalAddrs = maps:get(local_addrs, Config),
    LocalPort = maps:get(local_port, Config),
    UserOptions = maps:get(socket_options, Config),
    AddressOptions = [{ip, Address} || Address <- LocalAddrs],
    Options = [binary, {active, once}, {port, LocalPort} | AddressOptions ++ UserOptions],
    case gen_sctp:open(Options) of
        {ok, Socket} ->
            case gen_sctp:listen(Socket, true) of
                ok ->
                    case socket_names(Socket, LocalAddrs, LocalPort) of
                        {ok, ActualAddrs, ActualPort} ->
                            {ok, Socket, ActualAddrs, ActualPort};
                        {error, Reason} ->
                            gen_sctp:close(Socket),
                            {error, Reason}
                    end;
                {error, Reason} ->
                    gen_sctp:close(Socket),
                    {error, {listen, Reason}}
            end;
        {error, Reason} ->
            {error, {open, Reason}}
    end.

open_socket_api(Config) ->
    esock_socket_api:open(
        maps:get(local_addrs, Config),
        maps:get(local_port, Config),
        maps:get(socket_options, Config)
    ).

socket_names(Socket, ConfiguredAddrs, ConfiguredPort) ->
    esock_names:socket(inet:socknames(Socket), ConfiguredAddrs, ConfiguredPort).

register_peer_route(Lease, Options, Caller, State0) when is_map(Options) ->
    Acceptor = maps:get(acceptor, Options, Caller),
    RemoteAddrs0 = maps:get(remote_addrs, Options, any),
    RemotePort = maps:get(remote_port, Options, any),
    case normalize_peer(RemoteAddrs0, RemotePort, Acceptor) of
        {ok, RemoteAddrs} ->
            Route = #route{
                ref = make_ref(),
                lease = Lease,
                acceptor = Acceptor,
                monitor = erlang:monitor(process, Acceptor),
                remote_addrs = RemoteAddrs,
                remote_port = RemotePort
            },
            case conflicting_route(Route, State0#state.routes) of
                false ->
                    State = assign_pending(Route, State0),
                    {reply, {ok, Route#route.ref}, State};
                #route{ref = ExistingRef} ->
                    erlang:demonitor(Route#route.monitor, [flush]),
                    {reply, {error, {peer_conflict, ExistingRef}}, State0}
            end;
        {error, _} = Error ->
            {reply, Error, State0}
    end;
register_peer_route(_Lease, Options, _Caller, State) ->
    {reply, {error, {invalid_peer_options, Options}}, State}.

normalize_peer(any, RemotePort, Acceptor) when
    is_pid(Acceptor), RemotePort =:= any
->
    {ok, any};
normalize_peer(any, RemotePort, Acceptor) when
    is_pid(Acceptor), is_integer(RemotePort), 0 < RemotePort, RemotePort =< 65535
->
    {ok, any};
normalize_peer(RemoteAddrs0, RemotePort, Acceptor) when
    is_pid(Acceptor),
    is_list(RemoteAddrs0),
    RemoteAddrs0 =/= [],
    ((is_integer(RemotePort) andalso 0 < RemotePort andalso RemotePort =< 65535) orelse
        RemotePort =:= any)
->
    normalize_addresses(RemoteAddrs0);
normalize_peer(RemoteAddrs, RemotePort, Acceptor) ->
    {error,
        {invalid_peer_options, #{
            remote_addrs => RemoteAddrs,
            remote_port => RemotePort,
            acceptor => Acceptor
        }}}.

conflicting_route(Route, Routes) ->
    case [Existing || Existing <- Routes, routes_overlap(Route, Existing)] of
        [] -> false;
        [Existing | _] -> Existing
    end.

routes_overlap(
    #route{remote_addrs = Addrs1, remote_port = Port1},
    #route{remote_addrs = Addrs2, remote_port = Port2}
) ->
    ports_overlap(Port1, Port2) andalso addresses_overlap(Addrs1, Addrs2).

ports_overlap(any, _Port) -> true;
ports_overlap(_Port, any) -> true;
ports_overlap(Port, Port) -> true;
ports_overlap(_Port1, _Port2) -> false.

addresses_overlap(any, _Addrs) ->
    true;
addresses_overlap(_Addrs, any) ->
    true;
addresses_overlap(Addrs1, Addrs2) ->
    lists:any(fun(Address) -> lists:member(Address, Addrs2) end, Addrs1).

assign_pending(Route, State0) ->
    Pending = State0#state.pending,
    case
        take_first(
            fun(PendingRef) ->
                case maps:get(PendingRef, Pending) of
                    #pending{peer_ref = undefined, info = Info} ->
                        route_matches(Route, Info);
                    #pending{} ->
                        false
                end
            end,
            queue:to_list(State0#state.pending_order)
        )
    of
        {value, PendingRef, PendingOrder} ->
            dispatch(
                Route,
                maps:get(PendingRef, Pending),
                State0#state{pending_order = queue:from_list(PendingOrder)}
            );
        false ->
            State0#state{routes = State0#state.routes ++ [Route]}
    end.

route_matches(#route{remote_addrs = Addrs, remote_port = Port}, Info) ->
    PeerAddrs = maps:get(remote_addrs, Info),
    PeerPort = maps:get(remote_port, Info),
    port_matches(Port, PeerPort) andalso address_matches(Addrs, PeerAddrs).

port_matches(any, _Port) -> true;
port_matches(Port, Port) -> true;
port_matches(_Configured, _Actual) -> false.

address_matches(any, _Addrs) ->
    true;
address_matches(Configured, Actual) ->
    lists:any(fun(Address) -> lists:member(Address, Actual) end, Configured).

dispatch(Route, Pending0, State0) ->
    SocketRef = #esock_socket_ref{pid = self(), lease = Route#route.lease},
    Info = Pending0#pending.info,
    Route#route.acceptor !
        {
            esock,
            Route#route.ref,
            {association, SocketRef, Pending0#pending.ref, Info}
        },
    Pending = Pending0#pending{
        peer_ref = Route#route.ref,
        acceptor = Route#route.acceptor,
        monitor = Route#route.monitor,
        lease = Route#route.lease
    },
    PendingMap = (State0#state.pending)#{Pending#pending.ref => Pending},
    State0#state{pending = PendingMap}.

start_connect(Lease, Options, Caller, State0) when is_map(Options) ->
    Owner = maps:get(owner, Options, Caller),
    RemoteAddrs0 = maps:get(remote_addrs, Options, undefined),
    RemotePort = maps:get(remote_port, Options, undefined),
    ConnectOptions = maps:get(connect_options, Options, []),
    case normalize_connect(RemoteAddrs0, RemotePort, Owner, ConnectOptions) of
        {ok, RemoteAddrs} ->
            case conflicting_connect(RemoteAddrs, RemotePort, State0#state.connects) of
                false ->
                    Ref = make_ref(),
                    Monitor = erlang:monitor(process, Owner),
                    Connect0 = #connect{
                        ref = Ref,
                        lease = Lease,
                        owner = Owner,
                        monitor = Monitor,
                        remote_addrs = RemoteAddrs,
                        remaining = RemoteAddrs,
                        current = hd(RemoteAddrs),
                        remote_port = RemotePort,
                        options = ConnectOptions
                    },
                    case connect_can_start(Connect0, State0#state.connects, State0#state.backend) of
                        false ->
                            {reply, {ok, Ref}, State0#state{
                                connects = [Connect0 | State0#state.connects]
                            }};
                        true ->
                            case connect_next(Connect0, State0) of
                                {ok, Connect} ->
                                    State = maybe_start_queued(State0#state{
                                        connects = [Connect | State0#state.connects]
                                    }),
                                    {reply, {ok, Ref}, State};
                                {error, Reason} ->
                                    erlang:demonitor(Monitor, [flush]),
                                    {reply, {error, Reason}, State0}
                            end
                    end;
                #connect{ref = ExistingRef} ->
                    {reply, {error, {connect_conflict, ExistingRef}}, State0}
            end;
        {error, _} = Error ->
            {reply, Error, State0}
    end;
start_connect(_Lease, Options, _Caller, State) ->
    {reply, {error, {invalid_connect_options, Options}}, State}.

normalize_connect(RemoteAddrs0, RemotePort, Owner, ConnectOptions) when
    is_list(RemoteAddrs0),
    RemoteAddrs0 =/= [],
    is_pid(Owner),
    is_integer(RemotePort),
    0 < RemotePort,
    RemotePort =< 65535,
    is_list(ConnectOptions)
->
    normalize_addresses(RemoteAddrs0);
normalize_connect(RemoteAddrs, RemotePort, Owner, ConnectOptions) ->
    {error,
        {invalid_connect_options, #{
            remote_addrs => RemoteAddrs,
            remote_port => RemotePort,
            owner => Owner,
            connect_options => ConnectOptions
        }}}.

conflicting_connect(RemoteAddrs, RemotePort, Connects) ->
    case
        [
            Connect
         || Connect <- Connects,
            Connect#connect.canceled =:= false,
            Connect#connect.remote_port =:= RemotePort,
            addresses_overlap(RemoteAddrs, Connect#connect.remote_addrs)
        ]
    of
        [] -> false;
        [Connect | _] -> Connect
    end.

connect_next(#connect{remaining = []} = Connect, _State) ->
    {error, {connect, lists:reverse(Connect#connect.errors)}};
connect_next(#connect{remaining = [Address | Rest]} = Connect0, State) ->
    case connect_init(State, Address, Connect0#connect.remote_port, Connect0#connect.options) of
        {ok, AssocId} ->
            {ok, Connect0#connect{
                assoc_id = AssocId,
                started = true,
                async = undefined,
                current = Address,
                remaining = Rest
            }};
        {ok, AssocId, Async} ->
            {ok, Connect0#connect{
                assoc_id = AssocId,
                started = true,
                async = Async,
                current = Address,
                remaining = Rest
            }};
        {error, Reason} ->
            Error = {Address, Reason},
            connect_next(
                Connect0#connect{remaining = Rest, errors = [Error | Connect0#connect.errors]},
                State
            )
    end.

connect_can_start(_Connect, _Connects, gen_sctp) ->
    true;
connect_can_start(Connect, Connects, socket) ->
    not socket_connect_pending(Connects) andalso
        not ambiguous_canceled_connect(Connect, Connects).

socket_connect_pending(Connects) ->
    lists:any(
        fun
            (#connect{started = true, async = Async}) ->
                Async =/= undefined;
            (_) ->
                false
        end,
        Connects
    ).

ambiguous_canceled_connect(Connect, Connects) ->
    lists:any(
        fun
            (#connect{canceled = true, started = true, assoc_id = undefined} = Existing) ->
                Existing#connect.remote_port =:= Connect#connect.remote_port andalso
                    addresses_overlap(
                        Existing#connect.remote_addrs, Connect#connect.remote_addrs
                    );
            (_) ->
                false
        end,
        Connects
    ).

maybe_start_queued(#state{backend = gen_sctp} = State) ->
    State;
maybe_start_queued(#state{connects = Connects} = State0) ->
    case socket_connect_pending(Connects) of
        true ->
            State0;
        false ->
            case
                take_last(
                    fun(Connect) ->
                        Connect#connect.started =:= false andalso
                            connect_can_start(Connect, Connects, socket)
                    end,
                    Connects
                )
            of
                {value, Connect0, Rest} ->
                    case connect_next(Connect0, State0) of
                        {ok, Connect} ->
                            maybe_start_queued(State0#state{connects = [Connect | Rest]});
                        {error, Reason} ->
                            maybe_demonitor(Connect0#connect.monitor),
                            Connect0#connect.owner !
                                {esock, Connect0#connect.ref, {error, Reason}},
                            maybe_start_queued(State0#state{connects = Rest})
                    end;
                false ->
                    State0
            end
    end.

accept_pending(Lease, Caller, PendingRef, Owner, Options, State0) ->
    case maps:take(PendingRef, State0#state.pending) of
        {#pending{peer_ref = undefined}, _PendingMap} ->
            {reply, {error, not_assigned}, State0};
        {#pending{acceptor = Acceptor, lease = PendingLease}, _PendingMap} when
            Acceptor =/= Caller; PendingLease =/= Lease
        ->
            {reply, {error, not_owner}, State0};
        {Pending, PendingMap} ->
            cancel_timer(Pending#pending.timer),
            maybe_demonitor(Pending#pending.monitor),
            Socket = Pending#pending.socket,
            case
                prepare_handoff(
                    State0#state.backend, Socket, Pending#pending.assoc_id, Owner, Options
                )
            of
                ok ->
                    Assoc = make_assoc(
                        State0#state.backend,
                        Pending#pending.assoc_id,
                        Socket,
                        Pending#pending.info
                    ),
                    {reply, {ok, Assoc}, State0#state{pending = PendingMap}};
                {error, Reason} ->
                    close_socket(State0#state.backend, Socket),
                    {reply, {error, {handoff, Reason}}, State0#state{pending = PendingMap}}
            end;
        error ->
            {reply, {error, not_found}, State0}
    end.

prepare_handoff(gen_sctp, Socket, _AssocId, Owner, Options) ->
    PassiveOptions = proplists:delete(active, Options) ++ [{active, false}],
    case inet:setopts(Socket, PassiveOptions) of
        ok -> gen_sctp:controlling_process(Socket, Owner);
        {error, _} = Error -> Error
    end;
prepare_handoff(socket, Socket, _AssocId, Owner, Options) ->
    PassiveOptions = proplists:delete(active, Options),
    case esock_socket_api:setopts(Socket, PassiveOptions) of
        ok -> esock_socket_api:controlling_process(Socket, Owner);
        {error, _} = Error -> Error
    end.

handle_sctp(
    RemoteAddr,
    RemotePort,
    {_Ancillary, #sctp_assoc_change{state = comm_up} = Change},
    State0
) ->
    association_up(RemoteAddr, RemotePort, Change, State0);
handle_sctp(
    RemoteAddr,
    RemotePort,
    {_Ancillary, #sctp_assoc_change{} = Change},
    State0
) ->
    association_failed(RemoteAddr, RemotePort, Change, State0);
handle_sctp(_RemoteAddr, _RemotePort, _Data, State) ->
    State.

association_up(RemoteAddr, RemotePort, Change, State0) ->
    AssocId = Change#sctp_assoc_change.assoc_id,
    case peeloff(State0#state.backend, State0#state.socket, AssocId) of
        {ok, AssocSocket} ->
            case prepare_peeled(State0#state.backend, AssocSocket) of
                ok ->
                    route_association(AssocSocket, RemoteAddr, RemotePort, Change, State0);
                {error, Reason} ->
                    close_socket(State0#state.backend, AssocSocket),
                    association_setup_failed(
                        RemoteAddr, RemotePort, Change, {setopts, Reason}, State0
                    )
            end;
        {error, Reason} ->
            _ = abort(State0#state.backend, State0#state.socket, AssocId),
            association_setup_failed(
                RemoteAddr, RemotePort, Change, {peeloff, Reason}, State0
            )
    end.

association_setup_failed(RemoteAddr, RemotePort, Change, Reason, State0) ->
    AssocId = Change#sctp_assoc_change.assoc_id,
    case take_connect_for_event(AssocId, RemoteAddr, RemotePort, State0#state.connects) of
        {value, Connect, Connects} ->
            maybe_demonitor(Connect#connect.monitor),
            State1 = cancel_async(Connect, State0#state{connects = Connects}),
            case Connect#connect.canceled of
                true ->
                    ok;
                false ->
                    Connect#connect.owner !
                        {esock, Connect#connect.ref, {error, {association_setup, Reason}}}
            end,
            maybe_start_queued(State1);
        false ->
            State0
    end.

route_association(AssocSocket, RemoteAddr, RemotePort, Change, State0) ->
    Info = peer_info(AssocSocket, RemoteAddr, RemotePort, Change, State0),
    AssocId = Change#sctp_assoc_change.assoc_id,
    case take_connect_for_event(AssocId, RemoteAddr, RemotePort, State0#state.connects) of
        {value, #connect{canceled = true} = Connect, Connects} ->
            close_socket(State0#state.backend, AssocSocket),
            maybe_start_queued(cancel_async(Connect, State0#state{connects = Connects}));
        {value, Connect, Connects} ->
            maybe_start_queued(
                complete_connect(
                    AssocSocket, Change, Info, Connect, State0#state{connects = Connects}
                )
            );
        false ->
            queue_incoming(AssocSocket, Change, Info, State0)
    end.

complete_connect(AssocSocket, Change, Info, Connect, State0) ->
    maybe_demonitor(Connect#connect.monitor),
    State1 = cancel_async(Connect, State0),
    case controlling_process(State1#state.backend, AssocSocket, Connect#connect.owner) of
        ok ->
            Assoc = make_assoc(
                State1#state.backend,
                Change#sctp_assoc_change.assoc_id,
                AssocSocket,
                Info
            ),
            Connect#connect.owner ! {esock, Connect#connect.ref, {connected, Assoc, Info}},
            State1;
        {error, Reason} ->
            close_socket(State1#state.backend, AssocSocket),
            Connect#connect.owner ! {esock, Connect#connect.ref, {error, {handoff, Reason}}},
            State1
    end.

queue_incoming(AssocSocket, Change, Info, State0) ->
    PendingRef = make_ref(),
    Timeout = maps:get(pending_timeout, State0#state.config),
    Timer = start_pending_timer(Timeout, PendingRef),
    Pending = #pending{
        ref = PendingRef,
        socket = AssocSocket,
        assoc_id = Change#sctp_assoc_change.assoc_id,
        info = Info,
        timer = Timer
    },
    case take_matching_route(Pending#pending.info, State0#state.routes) of
        {value, Route, Routes} ->
            PendingMap = (State0#state.pending)#{PendingRef => Pending},
            dispatch(
                Route,
                Pending,
                State0#state{pending = PendingMap, routes = Routes}
            );
        false ->
            case pending_limit_reached(State0) of
                true ->
                    cancel_timer(Timer),
                    close_socket(State0#state.backend, AssocSocket),
                    State0;
                false ->
                    PendingMap = (State0#state.pending)#{PendingRef => Pending},
                    State0#state{
                        pending = PendingMap,
                        pending_order = queue:in(PendingRef, State0#state.pending_order)
                    }
            end
    end.

pending_limit_reached(#state{config = Config, pending = Pending}) ->
    MaxPending = maps:get(max_pending, Config),
    MaxPending =/= infinity andalso map_size(Pending) >= MaxPending.

association_failed(RemoteAddr, RemotePort, Change, State0) ->
    AssocId = Change#sctp_assoc_change.assoc_id,
    case take_connect_for_event(AssocId, RemoteAddr, RemotePort, State0#state.connects) of
        {value, #connect{canceled = true} = Connect, Connects} ->
            maybe_start_queued(cancel_async(Connect, State0#state{connects = Connects}));
        {value, Connect0, Connects} ->
            State1 = cancel_async(Connect0, State0#state{connects = Connects}),
            Error = {Connect0#connect.current, Change},
            Connect1 = Connect0#connect{
                assoc_id = undefined,
                async = undefined,
                errors = [Error | Connect0#connect.errors]
            },
            case connect_next(Connect1, State1) of
                {ok, Connect} ->
                    maybe_start_queued(State1#state{connects = [Connect | Connects]});
                {error, Reason} ->
                    erlang:demonitor(Connect0#connect.monitor, [flush]),
                    Connect0#connect.owner ! {esock, Connect0#connect.ref, {error, Reason}},
                    maybe_start_queued(State1)
            end;
        false ->
            State0
    end.

peer_info(Socket, RemoteAddr, RemotePort, Change, State) ->
    AssocId = Change#sctp_assoc_change.assoc_id,
    {RemoteAddrs, ActualRemotePort} = names(
        State#state.backend, Socket, peernames, AssocId, [RemoteAddr], RemotePort
    ),
    {LocalAddrs, ActualLocalPort} = names(
        State#state.backend,
        Socket,
        socknames,
        AssocId,
        State#state.local_addrs,
        State#state.local_port
    ),
    #{
        local_addrs => LocalAddrs,
        local_port => ActualLocalPort,
        remote_addrs => RemoteAddrs,
        remote_port => ActualRemotePort,
        inbound_streams => Change#sctp_assoc_change.inbound_streams,
        outbound_streams => Change#sctp_assoc_change.outbound_streams
    }.

names(gen_sctp, Socket, Function, _AssocId, DefaultAddrs, DefaultPort) ->
    esock_names:association(inet:Function(Socket), DefaultAddrs, DefaultPort);
names(socket, Socket, Function, AssocId, DefaultAddrs, DefaultPort) ->
    Result =
        case Function of
            socknames -> esock_socket_api:socknames(Socket, AssocId);
            peernames -> esock_socket_api:peernames(Socket, AssocId)
        end,
    esock_names:association(Result, DefaultAddrs, DefaultPort).

make_assoc(Backend, AssocId, Socket, Info) ->
    #esock_assoc_ref{
        backend = Backend,
        socket = Socket,
        assoc_id = AssocId,
        local_addrs = maps:get(local_addrs, Info),
        local_port = maps:get(local_port, Info),
        remote_addrs = maps:get(remote_addrs, Info),
        remote_port = maps:get(remote_port, Info),
        inbound_streams = maps:get(inbound_streams, Info),
        outbound_streams = maps:get(outbound_streams, Info)
    }.

take_matching_route(Info, Routes) ->
    take_first(fun(Route) -> route_matches(Route, Info) end, Routes).

take_connect_by_assoc_id(AssocId, Connects) ->
    take_first(
        fun(Connect) ->
            Connect#connect.assoc_id =/= undefined andalso Connect#connect.assoc_id =:= AssocId
        end,
        Connects
    ).

take_connect_by_endpoint(RemoteAddr, RemotePort, Connects) ->
    take_last(
        fun(Connect) ->
            Connect#connect.started =:= true andalso
                Connect#connect.assoc_id =:= undefined andalso
                Connect#connect.remote_port =:= RemotePort andalso
                lists:member(RemoteAddr, Connect#connect.remote_addrs)
        end,
        Connects
    ).

take_connect_for_event(AssocId, RemoteAddr, RemotePort, Connects) ->
    case take_connect_by_assoc_id(AssocId, Connects) of
        false -> take_connect_by_endpoint(RemoteAddr, RemotePort, Connects);
        Match -> Match
    end.

take_route(Ref, Routes) ->
    take_first(fun(Route) -> Route#route.ref =:= Ref end, Routes).

take_connect(Ref, Connects) ->
    take_first(fun(Connect) -> Connect#connect.ref =:= Ref end, Connects).

take_first(Predicate, List) ->
    take_first(Predicate, List, []).

take_last(Predicate, List) ->
    case take_first(Predicate, lists:reverse(List)) of
        {value, Value, Rest} -> {value, Value, lists:reverse(Rest)};
        false -> false
    end.

take_first(_Predicate, [], _Before) ->
    false;
take_first(Predicate, [Value | Rest], Before) ->
    case Predicate(Value) of
        true -> {value, Value, lists:reverse(Before, Rest)};
        false -> take_first(Predicate, Rest, [Value | Before])
    end.

drop_pending(PendingRef, State0) ->
    case maps:take(PendingRef, State0#state.pending) of
        {Pending, PendingMap} ->
            maybe_demonitor(Pending#pending.monitor),
            close_socket(State0#state.backend, Pending#pending.socket),
            remove_pending_order(PendingRef, State0#state{pending = PendingMap});
        error ->
            State0
    end.

remove_pending_order(PendingRef, State) ->
    PendingOrder = queue:from_list(
        lists:delete(PendingRef, queue:to_list(State#state.pending_order))
    ),
    State#state{pending_order = PendingOrder}.

remove_monitor(Monitor, State0) ->
    LeaseRefs = [
        Lease
     || {Lease, {_Owner, LeaseMonitor}} <- maps:to_list(State0#state.leases),
        LeaseMonitor =:= Monitor
    ],
    State1 = lists:foldl(fun remove_lease/2, State0, LeaseRefs),
    Routes = [Route || Route <- State1#state.routes, Route#route.monitor =/= Monitor],
    StateWithConnects = cancel_matching_connects(
        fun(Connect) -> Connect#connect.monitor =:= Monitor end, State1
    ),
    PendingRefs = [
        Ref
     || {Ref, Pending} <- maps:to_list(State1#state.pending),
        Pending#pending.monitor =:= Monitor
    ],
    State2 = StateWithConnects#state{routes = Routes},
    maybe_start_queued(lists:foldl(fun drop_pending/2, State2, PendingRefs)).

remove_lease(Lease, State0) ->
    case maps:take(Lease, State0#state.leases) of
        {{_Owner, Monitor}, Leases} ->
            erlang:demonitor(Monitor, [flush]),
            Routes0 = State0#state.routes,
            {RemovedRoutes, Routes} = lists:partition(
                fun(Route) -> Route#route.lease =:= Lease end,
                Routes0
            ),
            lists:foreach(
                fun(Route) -> erlang:demonitor(Route#route.monitor, [flush]) end,
                RemovedRoutes
            ),
            StateWithConnects = cancel_matching_connects(
                fun(Connect) -> Connect#connect.lease =:= Lease end, State0
            ),
            PendingRefs = [
                Ref
             || {Ref, Pending} <- maps:to_list(State0#state.pending),
                Pending#pending.lease =:= Lease
            ],
            State1 = StateWithConnects#state{leases = Leases, routes = Routes},
            maybe_start_queued(lists:foldl(fun drop_pending/2, State1, PendingRefs));
        error ->
            State0
    end.

cancel_matching_connects(Predicate, State) ->
    Connects = lists:foldr(
        fun(Connect, Acc) ->
            case Predicate(Connect) of
                false ->
                    [Connect | Acc];
                true when Connect#connect.started =:= false ->
                    maybe_demonitor(Connect#connect.monitor),
                    Acc;
                true ->
                    [
                        cancel_connect_record(
                            State#state.backend, State#state.socket, Connect
                        )
                        | Acc
                    ]
            end
        end,
        [],
        State#state.connects
    ),
    State#state{connects = Connects}.

maybe_schedule_stop(#state{leases = Leases, stop_timer = undefined} = State) when
    map_size(Leases) =:= 0
->
    StopRef = make_ref(),
    TimerRef = erlang:send_after(?STOP_LINGER, self(), {stop_if_unused, StopRef}),
    State#state{stop_timer = {StopRef, TimerRef}};
maybe_schedule_stop(State) ->
    State.

cancel_stop(#state{stop_timer = undefined} = State) ->
    State;
cancel_stop(#state{stop_timer = {_StopRef, TimerRef}} = State) ->
    erlang:cancel_timer(TimerRef),
    State#state{stop_timer = undefined}.

rearm(#state{backend = gen_sctp, socket = Socket} = State) ->
    case inet:setopts(Socket, [{active, once}]) of
        ok -> State;
        {error, closed} -> exit({shutdown, {socket_closed, closed}});
        {error, Reason} -> exit({setopts, Reason})
    end;
rearm(#state{backend = socket, socket = Socket} = State) ->
    ok = esock_socket_api:arm_listener(Socket),
    State.

maybe_demonitor(undefined) ->
    ok;
maybe_demonitor(Monitor) ->
    erlang:demonitor(Monitor, [flush]),
    ok.

start_pending_timer(infinity, _PendingRef) ->
    undefined;
start_pending_timer(Timeout, PendingRef) ->
    erlang:send_after(Timeout, self(), {pending_timeout, PendingRef}).

cancel_timer(undefined) ->
    ok;
cancel_timer(Timer) ->
    erlang:cancel_timer(Timer),
    ok.

cancel_connect_record(_Backend, _Socket, #connect{canceled = true} = Connect) ->
    Connect;
cancel_connect_record(
    gen_sctp, Socket, #connect{monitor = Monitor, assoc_id = AssocId} = Connect
) ->
    maybe_demonitor(Monitor),
    _ = gen_sctp:abort(Socket, #sctp_assoc_change{assoc_id = AssocId}),
    Connect#connect{monitor = undefined, canceled = true};
cancel_connect_record(
    socket,
    Socket,
    #connect{monitor = Monitor, assoc_id = AssocId} = Connect
) ->
    maybe_demonitor(Monitor),
    %% Canceling a pending socket:connect/3 leaves the kernel destination
    %% uncertain. Keep the operation alive and consume its completion before
    %% another connect is started on this one-to-many socket.
    _ = esock_socket_api:abort(Socket, AssocId),
    Connect#connect{monitor = undefined, canceled = true}.

connect_init(#state{backend = gen_sctp, socket = Socket}, Address, Port, Options) ->
    case gen_sctp:connectx_init(Socket, [Address], Port, Options) of
        {error, enotsup} ->
            case gen_sctp:connect_init(Socket, Address, Port, Options) of
                ok -> {ok, undefined};
                {error, _} = Error -> Error
            end;
        Result ->
            Result
    end;
connect_init(#state{backend = socket, socket = Socket}, Address, Port, Options) ->
    esock_socket_api:connect(Socket, Address, Port, Options).

peeloff(gen_sctp, Socket, AssocId) ->
    gen_sctp:peeloff(Socket, AssocId);
peeloff(socket, Socket, AssocId) ->
    esock_socket_api:peeloff(Socket, AssocId).

prepare_peeled(gen_sctp, Socket) ->
    inet:setopts(Socket, [{active, false}]);
prepare_peeled(socket, _Socket) ->
    ok.

controlling_process(gen_sctp, Socket, Owner) ->
    gen_sctp:controlling_process(Socket, Owner);
controlling_process(socket, Socket, Owner) ->
    esock_socket_api:controlling_process(Socket, Owner).

abort(gen_sctp, Socket, AssocId) ->
    gen_sctp:abort(Socket, #sctp_assoc_change{assoc_id = AssocId});
abort(socket, Socket, AssocId) ->
    esock_socket_api:abort(Socket, AssocId).

close_socket(gen_sctp, Socket) ->
    gen_sctp:close(Socket);
close_socket(socket, Socket) ->
    esock_socket_api:close(Socket).

-ifdef(TEST).
counter_wrap_is_not_an_async_connect_message_test() ->
    Message = {'$socket', listener, counter_wrap, recv_oct},
    State = #state{
        backend = socket,
        socket = listener,
        pending_order = queue:new(),
        connects = [#connect{async = undefined}]
    },
    ?assertEqual(State, handle_socket_message(Message, State)).

pending_assignment_is_fifo_test() ->
    FirstRef = make_ref(),
    SecondRef = make_ref(),
    RouteRef = make_ref(),
    Lease = make_ref(),
    Info1 = #{remote_addrs => [{127, 0, 0, 1}], remote_port => 1111},
    Info2 = #{remote_addrs => [{127, 0, 0, 1}], remote_port => 2222},
    Pending = #{
        FirstRef => #pending{ref = FirstRef, info = Info1},
        SecondRef => #pending{ref = SecondRef, info = Info2}
    },
    Route = #route{
        ref = RouteRef,
        lease = Lease,
        acceptor = self(),
        remote_addrs = any,
        remote_port = any
    },
    State = assign_pending(Route, #state{
        pending = Pending,
        pending_order = queue:from_list([FirstRef, SecondRef])
    }),
    ?assertMatch(
        #pending{peer_ref = RouteRef},
        maps:get(FirstRef, State#state.pending)
    ),
    ?assertEqual([SecondRef], queue:to_list(State#state.pending_order)),
    receive
        {esock, RouteRef, {association, _SocketRef, FirstRef, Info1}} -> ok
    after 1000 ->
        error(pending_not_dispatched)
    end.

conflicting_connect_ignores_canceled_test() ->
    RemoteAddrs = [{127, 0, 0, 1}],
    RemotePort = 3868,
    Canceled = #connect{
        remote_addrs = RemoteAddrs,
        remote_port = RemotePort,
        canceled = true
    },
    ?assertEqual(false, conflicting_connect(RemoteAddrs, RemotePort, [Canceled])).
-endif.
