-module(esock_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet_sctp.hrl").

-define(TIMEOUT, 5000).
-define(LOOPBACK, {127, 0, 0, 1}).

shared_socket_test_() ->
    test_case(fun shared_socket/0).

ephemeral_sockets_are_distinct_test_() ->
    test_case(fun ephemeral_sockets_are_distinct/0).

socket_configuration_conflict_test_() ->
    test_case(fun socket_configuration_conflict/0).

lease_owner_cleanup_test_() ->
    test_case(fun lease_owner_cleanup/0).

dead_listener_port_stops_socket_test_() ->
    test_case(fun dead_listener_port_stops_socket/0).

registry_rejects_unexpected_call_test_() ->
    test_case(fun registry_rejects_unexpected_call/0).

registry_restart_stops_registered_sockets_test_() ->
    test_case(fun registry_restart_stops_registered_sockets/0).

open_without_registry_returns_error_test_() ->
    test_case(fun open_without_registry_returns_error/0).

fixed_peer_routing_test_() ->
    test_case(fun fixed_peer_routing/0).

pending_accept_is_limited_to_acceptor_test_() ->
    test_case(fun pending_accept_is_limited_to_acceptor/0).

direct_association_owner_test_() ->
    test_case(fun direct_association_owner/0).

isolated_association_owners_test_() ->
    test_case(fun isolated_association_owners/0).

late_peer_registration_test_() ->
    test_case(fun late_peer_registration/0).

pending_association_timeout_test_() ->
    test_case(fun pending_association_timeout/0).

pending_association_limit_test_() ->
    test_case(fun pending_association_limit/0).

infinite_pending_timeout_test_() ->
    test_case(fun infinite_pending_timeout/0).

overlapping_peer_registration_test_() ->
    test_case(fun overlapping_peer_registration/0).

dead_acceptor_is_removed_test_() ->
    test_case(fun dead_acceptor_is_removed/0).

outgoing_association_test_() ->
    test_case(fun outgoing_association/0).

outgoing_address_failover_test_() ->
    test_case(fun outgoing_address_failover/0).

canceled_connect_can_retry_test_() ->
    test_case(fun canceled_connect_can_retry/0).

multiple_outgoing_associations_test_() ->
    test_case(fun multiple_outgoing_associations/0).

default_send_parameters_test_() ->
    test_case(fun default_send_parameters/0).

socket_backend_incoming_test_() ->
    socket_test_case(fun socket_backend_incoming/0).

socket_backend_outgoing_test_() ->
    socket_test_case(fun socket_backend_outgoing/0).

socket_backend_failed_connect_can_retry_test_() ->
    socket_test_case(fun socket_backend_failed_connect_can_retry/0).

socket_backend_address_failover_test_() ->
    socket_test_case(fun socket_backend_address_failover/0).

socket_backend_canceled_connect_can_retry_test_() ->
    socket_test_case(fun socket_backend_canceled_connect_can_retry/0).

socket_backend_multiple_outgoing_associations_test_() ->
    socket_test_case(fun socket_backend_multiple_outgoing_associations/0).

socket_backend_active_n_test_() ->
    socket_test_case(fun socket_backend_active_n/0).

socket_backend_reactivation_test_() ->
    socket_test_case(fun socket_backend_reactivation/0).

socket_backend_default_send_parameters_test_() ->
    socket_test_case(fun socket_backend_default_send_parameters/0).

unsupported_socket_backend_test_() ->
    case esock_socket_api:supported() of
        true -> [];
        false -> test_case(fun unsupported_socket_backend/0)
    end.

invalid_socket_options_test_() ->
    test_case(fun invalid_socket_options/0).

test_case(Test) ->
    {setup, fun setup/0, fun teardown/1, fun(_State) -> Test end}.

socket_test_case(Test) ->
    case esock_socket_api:supported() of
        true -> test_case(Test);
        false -> []
    end.

setup() ->
    {ok, _} = application:ensure_all_started(esock),
    ok.

teardown(_State) ->
    ok = application:stop(esock).

shared_socket() ->
    Port = free_port(),
    Options = #{local_addrs => [?LOOPBACK], local_port => Port},
    {ok, Socket1} = esock:open(Options),
    {ok, Socket2} = esock:open(Options),
    ?assertEqual(esock:socket_id(Socket1), esock:socket_id(Socket2)),
    ?assertEqual({ok, {[?LOOPBACK], Port}}, esock:socknames(Socket1)),
    SocketPid = esock:socket_id(Socket1),
    ok = esock:release(Socket1),
    ?assert(is_process_alive(SocketPid)),
    ok = esock:release(Socket2),
    ok = wait_until(fun() -> not is_process_alive(SocketPid) end).

ephemeral_sockets_are_distinct() ->
    Options = #{local_addrs => [?LOOPBACK], local_port => 0},
    {ok, Socket1} = esock:open(Options),
    {ok, Socket2} = esock:open(Options),
    ?assertNotEqual(esock:socket_id(Socket1), esock:socket_id(Socket2)),
    {ok, {[?LOOPBACK], Port1}} = esock:socknames(Socket1),
    {ok, {[?LOOPBACK], Port2}} = esock:socknames(Socket2),
    ?assert(Port1 > 0),
    ?assert(Port2 > 0),
    ?assertNotEqual(Port1, Port2),
    ok = esock:release(Socket1),
    ok = esock:release(Socket2).

socket_configuration_conflict() ->
    Port = free_port(),
    {ok, Socket} = esock:open(#{
        local_addrs => [?LOOPBACK],
        local_port => Port,
        pending_timeout => 100
    }),
    ?assertMatch(
        {error, {socket_option_conflict, _, _}},
        esock:open(#{
            local_addrs => [?LOOPBACK],
            local_port => Port,
            pending_timeout => 200
        })
    ),
    ok = esock:release(Socket).

lease_owner_cleanup() ->
    Parent = self(),
    Port = free_port(),
    Owner = spawn(fun() ->
        {ok, Socket} = esock:open(#{local_addrs => [?LOOPBACK], local_port => Port}),
        Parent ! {owned_socket, Socket},
        receive
            stop -> ok
        end
    end),
    Socket =
        receive
            {owned_socket, Value} -> Value
        after ?TIMEOUT ->
            error(owner_open_timeout)
        end,
    SocketPid = esock:socket_id(Socket),
    Monitor = erlang:monitor(process, Owner),
    Owner ! stop,
    receive
        {'DOWN', Monitor, process, Owner, normal} -> ok
    after ?TIMEOUT ->
        error(owner_stop_timeout)
    end,
    ok = wait_until(fun() -> not is_process_alive(SocketPid) end).

dead_listener_port_stops_socket() ->
    Port = free_port(),
    Options = #{local_addrs => [?LOOPBACK], local_port => Port},
    {ok, Socket} = esock:open(Options),
    SocketPid = esock:socket_id(Socket),
    {links, Links} = process_info(SocketPid, links),
    [ListenerPort] = [Link || Link <- Links, is_port(Link)],
    Monitor = erlang:monitor(process, SocketPid),
    SocketPid ! {'EXIT', ListenerPort, closed},
    receive
        {'DOWN', Monitor, process, SocketPid, {shutdown, {socket_closed, closed}}} -> ok
    after ?TIMEOUT ->
        error(listener_did_not_stop)
    end,
    {ok, Replacement} = esock:open(Options),
    ok = esock:release(Replacement).

registry_rejects_unexpected_call() ->
    Registry = whereis(esock_registry),
    ?assertEqual({error, bad_request}, gen_server:call(esock_registry, unexpected)),
    ?assertEqual(Registry, whereis(esock_registry)).

registry_restart_stops_registered_sockets() ->
    Port = free_port(),
    Options = #{local_addrs => [?LOOPBACK], local_port => Port},
    {ok, Socket} = esock:open(Options),
    SocketPid = esock:socket_id(Socket),
    SocketMonitor = erlang:monitor(process, SocketPid),
    Registry = whereis(esock_registry),
    SocketSupervisor = whereis(esock_socket_sup),
    ok = gen_server:stop(Registry, shutdown, infinity),
    receive
        {'DOWN', SocketMonitor, process, SocketPid, shutdown} -> ok
    after ?TIMEOUT ->
        error(orphaned_listener)
    end,
    ok = wait_until(fun() ->
        NewRegistry = whereis(esock_registry),
        NewSocketSupervisor = whereis(esock_socket_sup),
        is_pid(NewRegistry) andalso
            NewRegistry =/= Registry andalso
            is_pid(NewSocketSupervisor) andalso
            NewSocketSupervisor =/= SocketSupervisor
    end),
    {ok, Replacement} = esock:open(Options),
    ok = esock:release(Replacement).

open_without_registry_returns_error() ->
    ok = supervisor:terminate_child(esock_sup, esock_registry),
    try
        ?assertEqual(
            {error, socket_stopping},
            esock:open(#{local_addrs => [?LOOPBACK], local_port => free_port()})
        )
    after
        {ok, _Registry} = supervisor:restart_child(esock_sup, esock_registry)
    end.

fixed_peer_routing() ->
    ListenPort = free_port(),
    ClientPort1 = free_port(),
    ClientPort2 = free_port(),
    {ok, Socket} = esock:open(#{local_addrs => [?LOOPBACK], local_port => ListenPort}),
    {ok, Peer1} = esock:register_peer(Socket, #{
        remote_addrs => [?LOOPBACK],
        remote_port => ClientPort1
    }),
    {ok, Peer2} = esock:register_peer(Socket, #{
        remote_addrs => [?LOOPBACK],
        remote_port => ClientPort2
    }),
    {ok, Client1} = client_socket(ClientPort1),
    {ok, Client2} = client_socket(ClientPort2),
    AssocId2 = connect_client(Client2, ListenPort),
    AssocId1 = connect_client(Client1, ListenPort),
    {Pending2, Info2} = receive_pending(Peer2),
    {Pending1, Info1} = receive_pending(Peer1),
    ?assertEqual(ClientPort1, maps:get(remote_port, Info1)),
    ?assertEqual(ClientPort2, maps:get(remote_port, Info2)),
    {ok, Assoc1} = esock:accept(Socket, Pending1, self()),
    {ok, Assoc2} = esock:accept(Socket, Pending2, self()),
    ok = esock:activate(Assoc1, once),
    ok = esock:activate(Assoc2, once),
    ok = client_send(Client1, AssocId1, 46, <<"from-one">>),
    ok = client_send(Client2, AssocId2, 47, <<"from-two">>),
    ?assertMatch({ok, {data, #{ppid := 46}, <<"from-one">>}}, receive_data(Assoc1)),
    ?assertMatch({ok, {data, #{ppid := 47}, <<"from-two">>}}, receive_data(Assoc2)),
    ok = esock:send(Assoc1, #{ppid => 48}, <<"to-one">>),
    {ok, {_Address, _Port, [RecvInfo], <<"to-one">>}} = sctp_recv(Client1, ?TIMEOUT),
    ?assertEqual(48, RecvInfo#sctp_sndrcvinfo.ppid),
    close_all([Assoc1, Assoc2], [Client1, Client2], Socket).

pending_accept_is_limited_to_acceptor() ->
    ListenPort = free_port(),
    ClientPort = free_port(),
    {ok, Socket} = esock:open(#{local_addrs => [?LOOPBACK], local_port => ListenPort}),
    {ok, OtherSocket} = esock:open(#{
        local_addrs => [?LOOPBACK],
        local_port => ListenPort
    }),
    {ok, Peer} = esock:register_peer(Socket, #{
        remote_addrs => [?LOOPBACK],
        remote_port => ClientPort
    }),
    ?assertEqual({error, not_owner}, esock:unregister_peer(OtherSocket, Peer)),
    {ok, Client} = client_socket(ClientPort),
    _AssocId = connect_client(Client, ListenPort),
    {Pending, _Info} = receive_pending(Peer),
    Parent = self(),
    _Attacker = spawn(fun() ->
        Parent ! {hijack, esock:accept(Socket, Pending, self())}
    end),
    receive
        {hijack, Result} ->
            ?assertEqual({error, not_owner}, Result)
    after ?TIMEOUT ->
        error(hijack_timeout)
    end,
    ?assertEqual({error, not_owner}, esock:accept(OtherSocket, Pending, self())),
    {ok, Assoc} = esock:accept(Socket, Pending, self()),
    lists:foreach(fun esock:close/1, [Assoc]),
    gen_sctp:close(Client),
    ok = esock:release(Socket),
    ok = esock:release(OtherSocket).

direct_association_owner() ->
    ListenPort = free_port(),
    ClientPort = free_port(),
    {ok, Socket} = esock:open(#{local_addrs => [?LOOPBACK], local_port => ListenPort}),
    {ok, Peer} = esock:register_peer(Socket, #{
        remote_addrs => [?LOOPBACK],
        remote_port => ClientPort
    }),
    {ok, Client} = client_socket(ClientPort),
    AssocId = connect_client(Client, ListenPort),
    {Pending, _Info} = receive_pending(Peer),
    Parent = self(),
    Owner = spawn(fun() -> association_owner(Parent) end),
    {ok, Assoc} = esock:accept(Socket, Pending, Owner),
    Owner ! {association, Assoc, self()},
    receive
        {association_ready, Owner} -> ok
    after ?TIMEOUT ->
        error(owner_ready_timeout)
    end,
    ok = client_send(Client, AssocId, 49, <<"direct">>),
    receive
        {association_data, Owner, {ok, {data, #{ppid := 49}, <<"direct">>}}} -> ok
    after ?TIMEOUT ->
        error(owner_data_timeout)
    end,
    ok = gen_sctp:close(Client),
    ok = esock:release(Socket).

isolated_association_owners() ->
    ListenPort = free_port(),
    ClientPort1 = free_port(),
    ClientPort2 = free_port(),
    {ok, Socket} = esock:open(#{local_addrs => [?LOOPBACK], local_port => ListenPort}),
    {ok, Peer1} = esock:register_peer(Socket, #{
        remote_addrs => [?LOOPBACK],
        remote_port => ClientPort1
    }),
    {ok, Peer2} = esock:register_peer(Socket, #{
        remote_addrs => [?LOOPBACK],
        remote_port => ClientPort2
    }),
    {ok, Client1} = client_socket(ClientPort1),
    {ok, Client2} = client_socket(ClientPort2),
    AssocId1 = connect_client(Client1, ListenPort),
    AssocId2 = connect_client(Client2, ListenPort),
    {Pending1, _Info1} = receive_pending(Peer1),
    {Pending2, _Info2} = receive_pending(Peer2),
    Parent = self(),
    BlockedOwner = spawn(fun() -> blocked_association_owner(Parent) end),
    FastOwner = spawn(fun() -> association_owner(Parent) end),
    {ok, Assoc1} = esock:accept(Socket, Pending1, BlockedOwner),
    {ok, Assoc2} = esock:accept(Socket, Pending2, FastOwner),
    BlockedOwner ! {association, Assoc1, Parent},
    FastOwner ! {association, Assoc2, Parent},
    ok = wait_owner_ready(BlockedOwner),
    ok = wait_owner_ready(FastOwner),
    ok = client_send(Client1, AssocId1, 60, <<"blocked">>),
    ok = client_send(Client2, AssocId2, 61, <<"independent">>),
    receive
        {association_data, FastOwner, {ok, {data, #{ppid := 61}, <<"independent">>}}} ->
            ok
    after ?TIMEOUT ->
        error(independent_owner_timeout)
    end,
    BlockedOwner ! stop,
    ok = gen_sctp:close(Client1),
    ok = gen_sctp:close(Client2),
    ok = esock:release(Socket).

late_peer_registration() ->
    ListenPort = free_port(),
    ClientPort = free_port(),
    {ok, Socket} = esock:open(#{local_addrs => [?LOOPBACK], local_port => ListenPort}),
    {ok, Client} = client_socket(ClientPort),
    _AssocId = connect_client(Client, ListenPort),
    timer:sleep(25),
    {ok, Peer} = esock:register_peer(Socket, #{
        remote_addrs => [?LOOPBACK],
        remote_port => ClientPort
    }),
    {Pending, _Info} = receive_pending(Peer),
    {ok, Assoc} = esock:accept(Socket, Pending, self()),
    close_all([Assoc], [Client], Socket).

pending_association_timeout() ->
    ListenPort = free_port(),
    ClientPort = free_port(),
    {ok, Socket} = esock:open(#{
        local_addrs => [?LOOPBACK],
        local_port => ListenPort,
        pending_timeout => 50
    }),
    {ok, Client} = client_socket(ClientPort),
    _AssocId = connect_client(Client, ListenPort),
    timer:sleep(150),
    {ok, Peer} = esock:register_peer(Socket, #{
        remote_addrs => [?LOOPBACK],
        remote_port => ClientPort
    }),
    receive
        {esock, Peer, {association, _Socket, _Pending, _Info}} ->
            error(expired_association_was_dispatched)
    after 100 ->
        ok
    end,
    ok = esock:unregister_peer(Socket, Peer),
    ok = gen_sctp:close(Client),
    ok = esock:release(Socket).

pending_association_limit() ->
    ListenPort = free_port(),
    ClientPort1 = free_port(),
    ClientPort2 = free_port(),
    {ok, Socket} = esock:open(#{
        local_addrs => [?LOOPBACK],
        local_port => ListenPort,
        pending_timeout => infinity,
        max_pending => 1
    }),
    {ok, Client1} = client_socket(ClientPort1),
    {ok, Client2} = client_socket(ClientPort2),
    _AssocId1 = connect_client(Client1, ListenPort),
    {ok, Peer1} = esock:register_peer(Socket, #{
        remote_addrs => [?LOOPBACK],
        remote_port => ClientPort1
    }),
    {Pending1, _Info1} = receive_pending(Peer1),
    _AssocId2 = connect_client(Client2, ListenPort),
    timer:sleep(25),
    {ok, Peer2} = esock:register_peer(Socket, #{
        remote_addrs => [?LOOPBACK],
        remote_port => ClientPort2
    }),
    receive
        {esock, Peer2, {association, _Socket, _Pending, _Info}} ->
            error(excess_pending_association_was_retained)
    after 100 ->
        ok
    end,
    {ok, Assoc1} = esock:accept(Socket, Pending1, self()),
    ok = esock:close(Assoc1),
    ok = gen_sctp:close(Client1),
    ok = gen_sctp:close(Client2),
    ok = esock:release(Socket).

infinite_pending_timeout() ->
    ListenPort = free_port(),
    ClientPort = free_port(),
    {ok, Socket} = esock:open(#{
        local_addrs => [?LOOPBACK],
        local_port => ListenPort,
        pending_timeout => infinity
    }),
    {ok, Client} = client_socket(ClientPort),
    _AssocId = connect_client(Client, ListenPort),
    timer:sleep(25),
    {ok, Peer} = esock:register_peer(Socket, #{
        remote_addrs => [?LOOPBACK],
        remote_port => ClientPort
    }),
    {Pending, _Info} = receive_pending(Peer),
    {ok, Assoc} = esock:accept(Socket, Pending, self()),
    close_all([Assoc], [Client], Socket).

overlapping_peer_registration() ->
    ListenPort = free_port(),
    {ok, Socket} = esock:open(#{local_addrs => [?LOOPBACK], local_port => ListenPort}),
    {ok, Peer} = esock:register_peer(Socket, #{
        remote_addrs => [?LOOPBACK],
        remote_port => 3868
    }),
    ?assertEqual(
        {error, {peer_conflict, Peer}},
        esock:register_peer(Socket, #{remote_addrs => any, remote_port => 3868})
    ),
    {ok, _OtherPeer} = esock:register_peer(Socket, #{
        remote_addrs => [?LOOPBACK],
        remote_port => 3869
    }),
    ok = esock:release(Socket).

dead_acceptor_is_removed() ->
    ListenPort = free_port(),
    {ok, Socket} = esock:open(#{local_addrs => [?LOOPBACK], local_port => ListenPort}),
    Acceptor = spawn(fun acceptor_loop/0),
    {ok, _Peer} = esock:register_peer(Socket, #{
        remote_addrs => [?LOOPBACK],
        remote_port => 3868,
        acceptor => Acceptor
    }),
    Monitor = erlang:monitor(process, Acceptor),
    Acceptor ! stop,
    receive
        {'DOWN', Monitor, process, Acceptor, normal} -> ok
    after ?TIMEOUT ->
        error(acceptor_did_not_stop)
    end,
    ok = wait_until(fun() ->
        case esock:register_peer(Socket, #{remote_addrs => [?LOOPBACK], remote_port => 3868}) of
            {ok, NewPeer} ->
                ok = esock:unregister_peer(Socket, NewPeer),
                true;
            {error, {peer_conflict, _}} ->
                false
        end
    end),
    ok = esock:release(Socket).

outgoing_association() ->
    ServerPort = free_port(),
    LocalPort = free_port(),
    {ok, Server} = gen_sctp:open([
        binary,
        {active, false},
        {ip, ?LOOPBACK},
        {port, ServerPort}
    ]),
    ok = gen_sctp:listen(Server, true),
    {ok, Socket} = esock:open(#{local_addrs => [?LOOPBACK], local_port => LocalPort}),
    {ok, ConnectRef} = esock:connect(Socket, #{
        remote_addrs => [?LOOPBACK],
        remote_port => ServerPort
    }),
    {ok, {_Address, _Port, _Ancillary, ServerChange}} = sctp_recv(Server, ?TIMEOUT),
    ?assertEqual(comm_up, ServerChange#sctp_assoc_change.state),
    Assoc = receive_connected(ConnectRef),
    Info = esock:association_info(Assoc),
    ?assertEqual(ServerPort, maps:get(remote_port, Info)),
    ok = esock:activate(Assoc, once),
    ServerAssocId = ServerChange#sctp_assoc_change.assoc_id,
    ok = client_send(Server, ServerAssocId, 51, <<"server-data">>),
    ?assertMatch({ok, {data, #{ppid := 51}, <<"server-data">>}}, receive_data(Assoc)),
    ok = esock:activate(Assoc, once),
    ok = client_send(Server, ServerAssocId, 52, <<"native-decode">>),
    ?assertMatch(
        {ok, {data, ?LOOPBACK, ServerPort, #sctp_sndrcvinfo{ppid = 52}, <<"native-decode">>}},
        receive_sctp_data(Assoc)
    ),
    ok = esock:send(Assoc, #{ppid => 52}, <<"client-data">>),
    {ok, {_Remote, _RemotePort, [RecvInfo], <<"client-data">>}} =
        sctp_recv(Server, ?TIMEOUT),
    ?assertEqual(52, RecvInfo#sctp_sndrcvinfo.ppid),
    ok = esock:send(Assoc, 0, 53, [], <<"fast-send">>),
    {ok, {_Remote2, _RemotePort2, [FastRecvInfo], <<"fast-send">>}} =
        sctp_recv(Server, ?TIMEOUT),
    ?assertEqual(53, FastRecvInfo#sctp_sndrcvinfo.ppid),
    close_all([Assoc], [Server], Socket).

outgoing_address_failover() ->
    AlternateLoopback = {127, 0, 0, 2},
    ServerPort = free_port(),
    LocalPort = free_port(),
    {ok, Server} = server_socket(AlternateLoopback, ServerPort),
    {ok, Socket} = esock:open(#{local_addrs => [?LOOPBACK], local_port => LocalPort}),
    {ok, ConnectRef} = esock:connect(Socket, #{
        remote_addrs => [?LOOPBACK, AlternateLoopback],
        remote_port => ServerPort
    }),
    {ok, {_Address, _Port, _Ancillary, ServerChange}} = sctp_recv(Server, ?TIMEOUT),
    ?assertEqual(comm_up, ServerChange#sctp_assoc_change.state),
    Assoc = receive_connected(ConnectRef),
    ?assert(lists:member(AlternateLoopback, maps:get(remote_addrs, esock:association_info(Assoc)))),
    close_all([Assoc], [Server], Socket).

canceled_connect_can_retry() ->
    LocalPort = free_port(),
    RemotePort = free_port(),
    {ok, Socket} = esock:open(#{local_addrs => [?LOOPBACK], local_port => LocalPort}),
    Options = #{
        remote_addrs => [?LOOPBACK],
        remote_port => RemotePort
    },
    {ok, FirstRef} = esock:connect(Socket, Options),
    case esock:cancel_connect(Socket, FirstRef) of
        ok -> ok;
        {error, not_found} -> ok
    end,
    {ok, SecondRef} = esock:connect(Socket, Options),
    ?assertNotEqual(FirstRef, SecondRef),
    case esock:cancel_connect(Socket, SecondRef) of
        ok -> ok;
        {error, not_found} -> ok
    end,
    ok = esock:release(Socket).

multiple_outgoing_associations() ->
    ServerPort1 = free_port(),
    ServerPort2 = free_port(),
    LocalPort = free_port(),
    {ok, Server1} = server_socket(ServerPort1),
    {ok, Server2} = server_socket(ServerPort2),
    {ok, Socket} = esock:open(#{local_addrs => [?LOOPBACK], local_port => LocalPort}),
    {ok, ConnectRef1} = esock:connect(Socket, #{
        remote_addrs => [?LOOPBACK],
        remote_port => ServerPort1
    }),
    {ok, ConnectRef2} = esock:connect(Socket, #{
        remote_addrs => [?LOOPBACK],
        remote_port => ServerPort2
    }),
    Assoc1 = receive_connected(ConnectRef1),
    Assoc2 = receive_connected(ConnectRef2),
    {ok, {_Address1, LocalPort, _Ancillary1, Change1}} = sctp_recv(Server1, ?TIMEOUT),
    {ok, {_Address2, LocalPort, _Ancillary2, Change2}} = sctp_recv(Server2, ?TIMEOUT),
    ?assertEqual(LocalPort, maps:get(local_port, esock:association_info(Assoc1))),
    ?assertEqual(LocalPort, maps:get(local_port, esock:association_info(Assoc2))),
    ok = esock:activate(Assoc1, once),
    ok = esock:activate(Assoc2, once),
    ok = client_send(Server2, Change2#sctp_assoc_change.assoc_id, 63, <<"server-two">>),
    ok = client_send(Server1, Change1#sctp_assoc_change.assoc_id, 62, <<"server-one">>),
    ?assertMatch({ok, {data, #{ppid := 62}, <<"server-one">>}}, receive_data(Assoc1)),
    ?assertMatch({ok, {data, #{ppid := 63}, <<"server-two">>}}, receive_data(Assoc2)),
    close_all([Assoc1, Assoc2], [Server1, Server2], Socket).

default_send_parameters() ->
    ListenPort = free_port(),
    ClientPort = free_port(),
    DefaultInfo = #sctp_sndrcvinfo{ppid = 77},
    {ok, Socket} = esock:open(#{
        local_addrs => [?LOOPBACK],
        local_port => ListenPort,
        socket_options => [{sctp_default_send_param, DefaultInfo}]
    }),
    {ok, Peer} = esock:register_peer(Socket, #{
        remote_addrs => [?LOOPBACK],
        remote_port => ClientPort
    }),
    {ok, Client} = client_socket(ClientPort),
    _AssocId = connect_client(Client, ListenPort),
    {Pending, _Info} = receive_pending(Peer),
    {ok, Assoc} = esock:accept(Socket, Pending, self()),
    ok = esock:send(Assoc, <<"default-send">>),
    {ok, {_Address, _Port, [RecvInfo], <<"default-send">>}} =
        sctp_recv(Client, ?TIMEOUT),
    ?assertEqual(77, RecvInfo#sctp_sndrcvinfo.ppid),
    close_all([Assoc], [Client], Socket).

socket_backend_incoming() ->
    ListenPort = free_port(),
    ClientPort = free_port(),
    {ok, Socket} = esock:open(#{
        backend => socket,
        local_addrs => [?LOOPBACK],
        local_port => ListenPort,
        socket_options => [{sctp_nodelay, true}]
    }),
    {ok, Peer} = esock:register_peer(Socket, #{
        remote_addrs => [?LOOPBACK],
        remote_port => ClientPort
    }),
    {ok, Client} = client_socket(ClientPort),
    AssocId = connect_client(Client, ListenPort),
    {Pending, _Info} = receive_pending(Peer),
    {ok, Assoc} = esock:accept(Socket, Pending, self()),
    ok = esock:activate(Assoc, once),
    ok = client_send(Client, AssocId, 71, <<"socket-in">>),
    ?assertMatch({ok, {data, #{ppid := 71}, <<"socket-in">>}}, receive_data(Assoc)),
    AssocInfo = esock:association_info(Assoc),
    AssocSocket = maps:get(socket, AssocInfo),
    ServerAssocId = maps:get(assoc_id, AssocInfo),
    ?assertMatch(
        {ok, {event, #sctp_remote_error{error = bad_sid, assoc_id = ServerAssocId, data = [1]}}},
        esock:decode_sctp(
            Assoc,
            {esock_socket_api, AssocSocket, recv, ServerAssocId, #{
                notification => #{
                    type => remote_error,
                    error => bad_sid,
                    assoc_id => ServerAssocId,
                    remote_causes => [1]
                }
            }}
        )
    ),
    ?assertMatch(
        {ok,
            {event, #sctp_send_failed{
                flags = true,
                info = #sctp_sndrcvinfo{stream = 2, context = 7},
                assoc_id = ServerAssocId
            }}},
        esock:decode_sctp(
            Assoc,
            {esock_socket_api, AssocSocket, recv, ServerAssocId, #{
                notification => #{
                    type => send_failed_event,
                    flags => [data_sent],
                    error => 0,
                    info => #{sid => 2, ppid => 0, context => 7, assic_id => ServerAssocId},
                    assoc_id => ServerAssocId,
                    data => <<"undelivered">>
                }
            }}
        )
    ),
    ok = esock:send(Assoc, #{ppid => 72}, <<"socket-out">>),
    {ok, {_Address, _Port, [RecvInfo], <<"socket-out">>}} = sctp_recv(Client, ?TIMEOUT),
    ?assertEqual(72, RecvInfo#sctp_sndrcvinfo.ppid),
    close_all([Assoc], [Client], Socket).

socket_backend_outgoing() ->
    ServerPort = free_port(),
    LocalPort = free_port(),
    {ok, Server} = server_socket(ServerPort),
    {ok, Socket} = esock:open(#{
        backend => socket,
        local_addrs => [?LOOPBACK],
        local_port => LocalPort
    }),
    {ok, ConnectRef} = esock:connect(Socket, #{
        remote_addrs => [?LOOPBACK],
        remote_port => ServerPort
    }),
    {ok, {_Address, _Port, _Ancillary, ServerChange}} = sctp_recv(Server, ?TIMEOUT),
    Assoc = receive_connected(ConnectRef),
    ok = esock:activate(Assoc, once),
    ok = client_send(
        Server, ServerChange#sctp_assoc_change.assoc_id, 73, <<"socket-connect">>
    ),
    ?assertMatch(
        {ok, {data, #{ppid := 73}, <<"socket-connect">>}},
        receive_data(Assoc)
    ),
    close_all([Assoc], [Server], Socket).

socket_backend_failed_connect_can_retry() ->
    ServerPort = free_port(),
    LocalPort = free_port(),
    {ok, Socket} = esock:open(#{
        backend => socket,
        local_addrs => [?LOOPBACK],
        local_port => LocalPort
    }),
    ConnectOptions = #{
        remote_addrs => [?LOOPBACK],
        remote_port => ServerPort
    },
    {ok, FirstRef} = esock:connect(Socket, ConnectOptions),
    ok = receive_connect_error(FirstRef),
    ?assert(is_process_alive(esock:socket_id(Socket))),
    {ok, Server} = server_socket(ServerPort),
    {ok, SecondRef} = esock:connect(Socket, ConnectOptions),
    {ok, {_Address, _Port, _Ancillary, ServerChange}} = sctp_recv(Server, ?TIMEOUT),
    Assoc = receive_connected(SecondRef),
    ?assertEqual(comm_up, ServerChange#sctp_assoc_change.state),
    close_all([Assoc], [Server], Socket).

socket_backend_address_failover() ->
    AlternateLoopback = {127, 0, 0, 2},
    ServerPort = free_port(),
    LocalPort = free_port(),
    {ok, Server} = server_socket(AlternateLoopback, ServerPort),
    {ok, Socket} = esock:open(#{
        backend => socket,
        local_addrs => [?LOOPBACK],
        local_port => LocalPort
    }),
    {ok, ConnectRef} = esock:connect(Socket, #{
        remote_addrs => [?LOOPBACK, AlternateLoopback],
        remote_port => ServerPort
    }),
    {ok, {_Address, _Port, _Ancillary, ServerChange}} = sctp_recv(Server, ?TIMEOUT),
    ?assertEqual(comm_up, ServerChange#sctp_assoc_change.state),
    Assoc = receive_connected(ConnectRef),
    ?assert(lists:member(AlternateLoopback, maps:get(remote_addrs, esock:association_info(Assoc)))),
    close_all([Assoc], [Server], Socket).

socket_backend_canceled_connect_can_retry() ->
    LocalPort = free_port(),
    RemotePort = free_port(),
    {ok, Socket} = esock:open(#{
        backend => socket,
        local_addrs => [?LOOPBACK],
        local_port => LocalPort
    }),
    Options = #{
        remote_addrs => [?LOOPBACK],
        remote_port => RemotePort
    },
    {ok, FirstRef} = esock:connect(Socket, Options),
    case esock:cancel_connect(Socket, FirstRef) of
        ok -> ok;
        {error, not_found} -> ok
    end,
    {ok, SecondRef} = esock:connect(Socket, Options),
    ?assertNotEqual(FirstRef, SecondRef),
    case esock:cancel_connect(Socket, SecondRef) of
        ok -> ok;
        {error, not_found} -> ok
    end,
    ok = esock:release(Socket).

socket_backend_multiple_outgoing_associations() ->
    ServerPort1 = free_port(),
    ServerPort2 = free_port(),
    LocalPort = free_port(),
    {ok, Server1} = server_socket(ServerPort1),
    {ok, Server2} = server_socket(ServerPort2),
    {ok, Socket} = esock:open(#{
        backend => socket,
        local_addrs => [?LOOPBACK],
        local_port => LocalPort
    }),
    {ok, ConnectRef1} = esock:connect(Socket, #{
        remote_addrs => [?LOOPBACK],
        remote_port => ServerPort1
    }),
    {ok, ConnectRef2} = esock:connect(Socket, #{
        remote_addrs => [?LOOPBACK],
        remote_port => ServerPort2
    }),
    Assoc1 = receive_connected(ConnectRef1),
    Assoc2 = receive_connected(ConnectRef2),
    {ok, {_Address1, LocalPort, _Ancillary1, Change1}} = sctp_recv(Server1, ?TIMEOUT),
    {ok, {_Address2, LocalPort, _Ancillary2, Change2}} = sctp_recv(Server2, ?TIMEOUT),
    ok = esock:activate(Assoc1, once),
    ok = esock:activate(Assoc2, once),
    ok = client_send(Server1, Change1#sctp_assoc_change.assoc_id, 76, <<"socket-one">>),
    ok = client_send(Server2, Change2#sctp_assoc_change.assoc_id, 77, <<"socket-two">>),
    ?assertMatch({ok, {data, #{ppid := 76}, <<"socket-one">>}}, receive_data(Assoc1)),
    ?assertMatch({ok, {data, #{ppid := 77}, <<"socket-two">>}}, receive_data(Assoc2)),
    close_all([Assoc1, Assoc2], [Server1, Server2], Socket).

socket_backend_active_n() ->
    ListenPort = free_port(),
    ClientPort = free_port(),
    {ok, Socket} = esock:open(#{
        backend => socket,
        local_addrs => [?LOOPBACK],
        local_port => ListenPort
    }),
    {ok, Peer} = esock:register_peer(Socket, #{
        remote_addrs => [?LOOPBACK],
        remote_port => ClientPort
    }),
    {ok, Client} = client_socket(ClientPort),
    AssocId = connect_client(Client, ListenPort),
    {Pending, _Info} = receive_pending(Peer),
    {ok, Assoc} = esock:accept(Socket, Pending, self()),
    ok = esock:activate(Assoc, 3),
    ok = client_send(Client, AssocId, 74, <<"one">>),
    ok = client_send(Client, AssocId, 74, <<"two">>),
    ok = client_send(Client, AssocId, 74, <<"three">>),
    ?assertMatch({ok, {data, #{ppid := 74}, <<"one">>}}, receive_data(Assoc)),
    ?assertMatch({ok, {data, #{ppid := 74}, <<"two">>}}, receive_data(Assoc)),
    ?assertMatch({ok, {data, #{ppid := 74}, <<"three">>}}, receive_data(Assoc)),
    AssocSocket = maps:get(socket, esock:association_info(Assoc)),
    receive
        {sctp_passive, AssocSocket} -> ok
    after ?TIMEOUT ->
        error(passive_timeout)
    end,
    close_all([Assoc], [Client], Socket).

socket_backend_reactivation() ->
    ListenPort = free_port(),
    ClientPort = free_port(),
    {ok, Socket} = esock:open(#{
        backend => socket,
        local_addrs => [?LOOPBACK],
        local_port => ListenPort
    }),
    {ok, Peer} = esock:register_peer(Socket, #{
        remote_addrs => [?LOOPBACK],
        remote_port => ClientPort
    }),
    {ok, Client} = client_socket(ClientPort),
    AssocId = connect_client(Client, ListenPort),
    {Pending, _Info} = receive_pending(Peer),
    {ok, Assoc} = esock:accept(Socket, Pending, self()),
    lists:foreach(
        fun(_) ->
            ok = esock:activate(Assoc, once)
        end,
        lists:seq(1, 20)
    ),
    ok = client_send(Client, AssocId, 78, <<"reactivated">>),
    ?assertMatch(
        {ok, {data, #{ppid := 78}, <<"reactivated">>}},
        receive_data(Assoc)
    ),
    close_all([Assoc], [Client], Socket).

socket_backend_default_send_parameters() ->
    ListenPort = free_port(),
    ClientPort = free_port(),
    DefaultInfo = #sctp_sndrcvinfo{ppid = 75},
    {ok, Socket} = esock:open(#{
        backend => socket,
        local_addrs => [?LOOPBACK],
        local_port => ListenPort,
        socket_options => [{sctp_default_send_param, DefaultInfo}]
    }),
    {ok, Peer} = esock:register_peer(Socket, #{
        remote_addrs => [?LOOPBACK],
        remote_port => ClientPort
    }),
    {ok, Client} = client_socket(ClientPort),
    _AssocId = connect_client(Client, ListenPort),
    {Pending, _Info} = receive_pending(Peer),
    {ok, Assoc} = esock:accept(Socket, Pending, self()),
    ok = esock:send(Assoc, <<"socket-default">>),
    {ok, {_Address, _Port, [RecvInfo], <<"socket-default">>}} =
        sctp_recv(Client, ?TIMEOUT),
    ?assertEqual(75, RecvInfo#sctp_sndrcvinfo.ppid),
    close_all([Assoc], [Client], Socket).

unsupported_socket_backend() ->
    ?assertEqual(
        {error, {unsupported_backend, socket}},
        esock:open(#{
            backend => socket,
            local_addrs => [?LOOPBACK],
            local_port => 0
        })
    ).

invalid_socket_options() ->
    ?assertMatch(
        {error, {invalid_socket_options, _}},
        invalid_esock_call(open, [invalid])
    ),
    ?assertMatch(
        {error, {invalid_socket_options, _}},
        invalid_esock_call(open, [#{socket_options => [{active, true}]}])
    ),
    ?assertMatch(
        {error, {invalid_socket_options, _}},
        invalid_esock_call(open, [#{local_addrs => []}])
    ),
    ?assertMatch(
        {error, {invalid_address, _}},
        invalid_esock_call(open, [#{local_addrs => [{256, 0, 0, 1}]}])
    ),
    ?assertMatch(
        {error, {invalid_socket_options, _}},
        invalid_esock_call(open, [#{pending_timeout => 0}])
    ),
    {ok, Socket} = esock:open(#{local_addrs => [?LOOPBACK], local_port => free_port()}),
    ?assertMatch(
        {error, {invalid_peer_options, _}},
        invalid_esock_call(register_peer, [Socket, invalid])
    ),
    ?assertMatch(
        {error, {invalid_connect_options, _}},
        invalid_esock_call(connect, [Socket, invalid])
    ),
    ?assertMatch(
        {error, {invalid_accept_options, _}},
        invalid_esock_call(accept, [Socket, make_ref(), invalid, []])
    ),
    ?assertMatch(
        {error, {invalid_peer_options, _}},
        invalid_esock_call(
            register_peer,
            [Socket, #{remote_addrs => [?LOOPBACK], remote_port => 0}]
        )
    ),
    ok = esock:release(Socket).

client_socket(Port) ->
    gen_sctp:open([binary, {active, false}, {ip, ?LOOPBACK}, {port, Port}]).

server_socket(Port) ->
    server_socket(?LOOPBACK, Port).

server_socket(Address, Port) ->
    case gen_sctp:open([binary, {active, false}, {ip, Address}, {port, Port}]) of
        {ok, Socket} ->
            ok = gen_sctp:listen(Socket, true),
            {ok, Socket};
        {error, _} = Error ->
            Error
    end.

connect_client(Socket, Port) ->
    connect_assoc_id(sctp_connect(Socket, ?LOOPBACK, Port, [])).

sctp_connect(Socket, Address, Port, Options) ->
    apply(gen_sctp, connect, [Socket, Address, Port, Options]).

connect_assoc_id({ok, #sctp_assoc_change{state = comm_up, assoc_id = AssocId}}) ->
    AssocId;
connect_assoc_id(
    {ok, {_Address, _Port, _Ancillary, #sctp_assoc_change{state = comm_up, assoc_id = AssocId}}}
) ->
    AssocId;
connect_assoc_id(Other) ->
    error({connect_failed, Other}).

client_send(Socket, AssocId, PPID, Data) ->
    gen_sctp:send(Socket, #sctp_sndrcvinfo{assoc_id = AssocId, ppid = PPID}, Data).

sctp_recv(Socket, Timeout) ->
    apply(gen_sctp, recv, [Socket, Timeout]).

invalid_esock_call(Function, Args) ->
    apply(esock, Function, Args).

receive_pending(PeerRef) ->
    receive
        {esock, PeerRef, {association, _Socket, Pending, Info}} ->
            {Pending, Info}
    after ?TIMEOUT ->
        error({association_timeout, PeerRef})
    end.

receive_connected(ConnectRef) ->
    receive
        {esock, ConnectRef, {connected, Assoc, _Info}} ->
            Assoc;
        {esock, ConnectRef, {error, Reason}} ->
            error({connect_failed, Reason})
    after ?TIMEOUT ->
        error({connect_timeout, ConnectRef})
    end.

receive_connect_error(ConnectRef) ->
    receive
        {esock, ConnectRef, {error, _Reason}} ->
            ok;
        {esock, ConnectRef, {connected, Assoc, _Info}} ->
            esock:close(Assoc),
            error(unexpected_connection)
    after ?TIMEOUT ->
        error({connect_error_timeout, ConnectRef})
    end.

receive_data(Assoc) ->
    receive_decoded(Assoc, fun esock:decode/2).

receive_sctp_data(Assoc) ->
    receive_decoded(Assoc, fun esock:decode_sctp/2).

receive_decoded(Assoc, Decoder) ->
    Info = esock:association_info(Assoc),
    receive_decoded(
        Assoc, Decoder, maps:get(backend, Info), maps:get(socket, Info)
    ).

receive_decoded(Assoc, Decoder, gen_sctp, Socket) ->
    receive
        {sctp, Socket, _RemoteAddr, _RemotePort, _Data} = Message ->
            case Decoder(Assoc, Message) of
                ignore -> receive_decoded(Assoc, Decoder, gen_sctp, Socket);
                Decoded -> Decoded
            end
    after ?TIMEOUT ->
        error(data_timeout)
    end;
receive_decoded(Assoc, Decoder, socket, Socket) ->
    receive
        {'$socket', Socket, _Tag, _Data} = Message ->
            decode_or_receive(Assoc, Decoder, socket, Socket, Message);
        {esock_socket_api, Socket, recv, _AssocId, _Result} = Message ->
            decode_or_receive(Assoc, Decoder, socket, Socket, Message)
    after ?TIMEOUT ->
        error(data_timeout)
    end.

decode_or_receive(Assoc, Decoder, Backend, Socket, Message) ->
    case Decoder(Assoc, Message) of
        ignore -> receive_decoded(Assoc, Decoder, Backend, Socket);
        Decoded -> Decoded
    end.

close_all(Assocs, Sockets, SharedSocket) ->
    lists:foreach(fun esock:close/1, Assocs),
    lists:foreach(fun gen_sctp:close/1, Sockets),
    esock:release(SharedSocket).

acceptor_loop() ->
    receive
        stop -> ok
    end.

association_owner(Parent) ->
    receive
        {association, Assoc, Parent} ->
            ok = esock:activate(Assoc, once),
            Parent ! {association_ready, self()},
            association_owner_receive(Parent, Assoc)
    end.

association_owner_receive(Parent, Assoc) ->
    receive
        Message ->
            case esock:decode(Assoc, Message) of
                ignore ->
                    association_owner_receive(Parent, Assoc);
                Decoded ->
                    Parent ! {association_data, self(), Decoded},
                    esock:close(Assoc)
            end
    end.

blocked_association_owner(Parent) ->
    receive
        {association, Assoc, Parent} ->
            ok = esock:activate(Assoc, once),
            Parent ! {association_ready, self()},
            receive
                stop -> esock:close(Assoc)
            end
    end.

wait_owner_ready(Owner) ->
    receive
        {association_ready, Owner} -> ok
    after ?TIMEOUT ->
        error({owner_ready_timeout, Owner})
    end.

free_port() ->
    {ok, Socket} = gen_sctp:open([binary, {active, false}, {ip, ?LOOPBACK}, {port, 0}]),
    {ok, Port} = inet:port(Socket),
    ok = gen_sctp:close(Socket),
    Port.

wait_until(Predicate) ->
    wait_until(Predicate, 300).

wait_until(_Predicate, 0) ->
    {error, timeout};
wait_until(Predicate, Attempts) ->
    case Predicate() of
        true ->
            ok;
        false ->
            timer:sleep(10),
            wait_until(Predicate, Attempts - 1)
    end.
