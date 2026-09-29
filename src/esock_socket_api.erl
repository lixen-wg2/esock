-module(esock_socket_api).

-include_lib("kernel/include/inet_sctp.hrl").

-export([
    supported/0,
    open/3,
    close/1,
    arm_listener/1,
    listener_message/2,
    peeloff/2,
    controlling_process/2,
    setopts/2,
    association_setopts/3,
    connect/4,
    finish_connect/1,
    cancel/2,
    abort/2,
    socknames/2,
    peernames/2,
    send/3,
    send/8,
    decode_sctp/3
]).

-define(RECV_STATE(Socket), {?MODULE, recv_state, Socket}).
-define(RECV_CTRL_SIZE, 1024).

open(LocalAddrs, LocalPort, Options) ->
    case supported() of
        true -> open_socket(LocalAddrs, LocalPort, Options);
        false -> {error, {unsupported_backend, socket}}
    end.

supported() ->
    case code:ensure_loaded(socket) of
        {module, socket} ->
            lists:all(
                fun({Function, Arity}) ->
                    erlang:function_exported(socket, Function, Arity)
                end,
                [{peeloff, 2}, {socknames, 2}, {peernames, 2}]
            );
        _ ->
            false
    end.

open_socket(LocalAddrs, LocalPort, Options) ->
    Domain = address_domain(hd(LocalAddrs)),
    case socket:open(Domain, seqpacket, sctp, #{use_registry => false}) of
        {ok, Socket} ->
            case prepare_socket(Socket, Domain, LocalAddrs, LocalPort, Options) of
                {ok, ActualAddrs, ActualPort} ->
                    {ok, Socket, ActualAddrs, ActualPort};
                {error, _} = Error ->
                    socket:close(Socket),
                    Error
            end;
        {error, Reason} ->
            {error, {open, Reason}}
    end.

prepare_socket(Socket, Domain, LocalAddrs, LocalPort, Options) ->
    case set_events(Socket) of
        ok ->
            case setopts(Socket, Options) of
                ok ->
                    case bind_addresses(Socket, Domain, LocalAddrs, LocalPort) of
                        ok ->
                            case socket:listen(Socket, true) of
                                ok -> socket_names(Socket, LocalAddrs, LocalPort);
                                {error, Reason} -> {error, {listen, Reason}}
                            end;
                        {error, Reason} ->
                            {error, {bind, Reason}}
                    end;
                {error, Reason} ->
                    {error, {setopts, Reason}}
            end;
        {error, Reason} ->
            {error, {events, Reason}}
    end.

set_events(Socket) ->
    socket:setopt(Socket, {sctp, events}, #{
        data_io => true,
        association => true,
        address => true,
        send_failure => true,
        peer_error => true,
        shutdown => true,
        partial_delivery => true
    }).

bind_addresses(Socket, Domain, [First | Rest], Port) ->
    case socket:bind(Socket, sockaddr(Domain, First, Port)) of
        ok when Rest =:= [] ->
            ok;
        ok ->
            case socket:sockname(Socket) of
                {ok, #{port := ActualPort}} ->
                    socket:bind(
                        Socket,
                        [sockaddr(Domain, Address, ActualPort) || Address <- Rest],
                        add
                    );
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

socket_names(Socket, ConfiguredAddrs, ConfiguredPort) ->
    esock_names:socket(socknames(Socket, 0), ConfiguredAddrs, ConfiguredPort).

close(Socket) ->
    erase(?RECV_STATE(Socket)),
    case socket:close(Socket) of
        ok -> ok;
        {error, closed} -> ok;
        {error, _} = Error -> Error
    end.

arm_listener(Socket) ->
    self() ! {'$socket', Socket, select, nowait},
    ok.

listener_message(Socket, {'$socket', Socket, select, Handle}) ->
    listener_recv_result(
        Socket, socket:recvmsg(Socket, 0, ?RECV_CTRL_SIZE, [], Handle)
    );
listener_message(Socket, {'$socket', Socket, completion, {_Handle, Result}}) ->
    listener_recv_result(Socket, Result);
listener_message(Socket, {'$socket', Socket, abort, {_Handle, Reason}}) ->
    {error, Reason};
listener_message(_Socket, _Message) ->
    ignore.

listener_recv_result(_Socket, {select, _SelectInfo}) ->
    pending;
listener_recv_result(_Socket, {completion, _CompletionInfo}) ->
    pending;
listener_recv_result(Socket, {select_read, {_SelectInfo, Message}}) ->
    _ = socket:setopt(Socket, {otp, select_read}, false),
    socket_message(Message, undefined);
listener_recv_result(_Socket, {ok, Message}) ->
    socket_message(Message, undefined);
listener_recv_result(_Socket, {error, Reason}) ->
    {error, Reason};
listener_recv_result(_Socket, Message) when is_map(Message) ->
    socket_message(Message, undefined).

peeloff(Socket, AssocId) ->
    case apply(socket, peeloff, [Socket, AssocId]) of
        {ok, AssocSocket} ->
            _ = socket:setopt(AssocSocket, {otp, select_read}, false),
            {ok, AssocSocket};
        {ok, AssocSocket, _InheritErrors} ->
            _ = socket:setopt(AssocSocket, {otp, select_read}, false),
            {ok, AssocSocket};
        {error, _} = Error ->
            Error
    end.

controlling_process(Socket, Owner) ->
    socket:setopt(Socket, {otp, controlling_process}, Owner).

setopts(Socket, Options) ->
    lists:foldl(
        fun
            (_Option, {error, _} = Error) -> Error;
            (Option, ok) -> setopt(Socket, Option)
        end,
        ok,
        Options
    ).

association_setopts(Socket, AssocId, Options) ->
    PassiveOptions = proplists:delete(active, Options),
    case setopts(Socket, PassiveOptions) of
        ok ->
            case [Active || {active, Active} <- Options] of
                [] -> ok;
                ActiveValues -> activate(Socket, AssocId, lists:last(ActiveValues))
            end;
        {error, _} = Error ->
            Error
    end.

setopt(_Socket, {active, _Active}) ->
    ok;
setopt(Socket, {reuseaddr, Value}) ->
    socket:setopt(Socket, {socket, reuseaddr}, Value);
setopt(Socket, {linger, {OnOff, Seconds}}) ->
    socket:setopt(Socket, {socket, linger}, #{onoff => OnOff, linger => Seconds});
setopt(Socket, {sndbuf, Value}) ->
    socket:setopt(Socket, {socket, sndbuf}, Value);
setopt(Socket, {recbuf, Value}) ->
    socket:setopt(Socket, {socket, rcvbuf}, Value);
setopt(_Socket, {buffer, _Value}) ->
    ok;
setopt(_Socket, {non_block_send, _Value}) ->
    ok;
setopt(Socket, {sctp_nodelay, Value}) ->
    socket:setopt(Socket, {sctp, nodelay}, Value);
setopt(Socket, {sctp_maxseg, Value}) ->
    socket:setopt(Socket, {sctp, maxseg}, Value);
setopt(Socket, {sctp_autoclose, Value}) ->
    socket:setopt(Socket, {sctp, autoclose}, Value);
setopt(Socket, {sctp_disable_fragments, Value}) ->
    socket:setopt(Socket, {sctp, disable_fragments}, Value);
setopt(Socket, {sctp_initmsg, Init}) when is_record(Init, sctp_initmsg) ->
    update_option(Socket, {sctp, initmsg}, initmsg_map(Init));
setopt(Socket, {sctp_associnfo, Assoc}) when is_record(Assoc, sctp_assocparams) ->
    update_option(Socket, {sctp, associnfo}, associnfo_map(Assoc));
setopt(_Socket, {sctp_rtoinfo, Rto}) when is_record(Rto, sctp_rtoinfo) ->
    %% OTP 29.0 advertises this option but rejects its documented map format.
    ok;
setopt(_Socket, {sctp_peer_addr_params, Params}) when is_record(Params, sctp_paddrparams) ->
    %% Not implemented by the OTP 29.0 socket NIF on Linux.
    ok;
setopt(Socket, {sctp_default_send_param, Info}) when is_record(Info, sctp_sndrcvinfo) ->
    update_option(Socket, {sctp, default_send_param}, sndrcvinfo_map(Info));
setopt(_Socket, Option) ->
    {error, {unsupported_socket_option, Option}}.

update_option(Socket, Option, Updates) ->
    case socket:getopt(Socket, Option) of
        {ok, Existing} when is_map(Existing) ->
            socket:setopt(Socket, Option, maps:merge(Existing, Updates));
        {error, _} = Error ->
            Error
    end.

connect(Socket, Address, Port, Options) ->
    case setopts(Socket, Options) of
        ok ->
            Domain = address_domain(Address),
            SockAddr = sockaddr(Domain, Address, Port),
            %% OTP 29 can return asynchronous results for SCTP connectx even
            %% though the socket:connect/3 type only lists them for one address.
            case apply(socket, connect, [Socket, [SockAddr], nowait]) of
                {ok, AssocId} -> {ok, AssocId, undefined};
                ok -> {ok, undefined, undefined};
                {select, SelectInfo} -> {ok, undefined, SelectInfo};
                {completion, CompletionInfo} -> {ok, undefined, CompletionInfo};
                {error, _} = Error -> Error
            end;
        {error, _} = Error ->
            Error
    end.

finish_connect(Socket) ->
    normalize_connect_result(socket:connect(Socket)).

normalize_connect_result(ok) -> {ok, undefined};
normalize_connect_result({error, _} = Error) -> Error.

cancel(Socket, SelectInfo) ->
    socket:cancel(Socket, SelectInfo).

abort(_Socket, undefined) ->
    ok;
abort(Socket, AssocId) ->
    send(Socket, AssocId, 0, 0, 0, 0, [abort], <<>>).

socknames(Socket, AssocId) ->
    names(apply(socket, socknames, [Socket, AssocId])).

peernames(Socket, AssocId) ->
    names(apply(socket, peernames, [Socket, AssocId])).

names({ok, SockAddrs}) ->
    {ok, [
        {Address, Port}
     || #{addr := Address, port := Port} <- SockAddrs
    ]};
names({error, _} = Error) ->
    Error.

activate(Socket, _AssocId, false) ->
    deactivate(Socket),
    ok;
activate(Socket, AssocId, once) ->
    start_receive(Socket, AssocId, 1, false);
activate(Socket, AssocId, true) ->
    start_receive(Socket, AssocId, infinity, false);
activate(Socket, AssocId, Active) when is_integer(Active), Active > 0 ->
    start_receive(Socket, AssocId, Active, true);
activate(_Socket, _AssocId, Active) ->
    {error, {invalid_active, Active}}.

start_receive(Socket, AssocId, Remaining, NotifyPassive) ->
    deactivate(Socket),
    case socket:setopt(Socket, {otp, select_read}, false) of
        ok ->
            put(?RECV_STATE(Socket), #{
                assoc_id => AssocId,
                remaining => Remaining,
                notify_passive => NotifyPassive,
                select_info => undefined
            }),
            case arm_receive(Socket, AssocId) of
                ok ->
                    ok;
                {error, _} = Error ->
                    deactivate(Socket),
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

arm_receive(Socket, AssocId) ->
    case socket:recvmsg(Socket, 0, ?RECV_CTRL_SIZE, [], nowait) of
        {select, SelectInfo} ->
            store_select_info(Socket, SelectInfo),
            ok;
        {completion, CompletionInfo} ->
            store_select_info(Socket, CompletionInfo),
            ok;
        {error, _} = Error ->
            Error;
        Result ->
            self() ! {?MODULE, Socket, recv, AssocId, Result},
            ok
    end.

deactivate(Socket) ->
    case erase(?RECV_STATE(Socket)) of
        #{select_info := SelectInfo} when SelectInfo =/= undefined ->
            _ = socket:cancel(Socket, SelectInfo);
        _ ->
            ok
    end,
    _ = socket:setopt(Socket, {otp, select_read}, false),
    ok.

send(Socket, _AssocId, Data) ->
    normalize_send_result(socket:send(Socket, Data)).

send(Socket, AssocId, Stream, PPID, Context, TimeToLive, Flags, Data) ->
    Info = #{
        stream => Stream,
        flags => Flags,
        ppid => PPID,
        context => Context,
        time_to_live => TimeToLive,
        assoc_id => AssocId
    },
    normalize_send_result(
        socket:sendmsg(Socket, #{
            iov => [Data],
            ctrl => [#{level => sctp, type => sndrcv, value => Info}]
        })
    ).

normalize_send_result(ok) -> ok;
normalize_send_result({ok, []}) -> ok;
normalize_send_result({ok, Rest}) -> {error, {partial_send, Rest}};
normalize_send_result({error, _} = Error) -> Error.

decode_sctp(Socket, AssocId, {'$socket', Socket, select, Handle}) ->
    case take_select_info(Socket, Handle) of
        true ->
            decode_recv_result(
                Socket, AssocId, socket:recvmsg(Socket, 0, ?RECV_CTRL_SIZE, [], Handle)
            );
        false ->
            ignore
    end;
decode_sctp(Socket, AssocId, {'$socket', Socket, completion, {Handle, Result}}) ->
    case take_select_info(Socket, Handle) of
        true -> decode_recv_result(Socket, AssocId, Result);
        false -> ignore
    end;
decode_sctp(Socket, AssocId, {'$socket', Socket, abort, {Handle, Reason}}) ->
    case take_select_info(Socket, Handle) of
        true -> receive_error(Socket, AssocId, Reason);
        false -> ignore
    end;
decode_sctp(Socket, AssocId, {?MODULE, Socket, recv, AssocId, Result}) ->
    decode_recv_result(Socket, AssocId, Result);
decode_sctp(_Socket, _AssocId, _Message) ->
    ignore.

decode_recv_result(Socket, _AssocId, {select, SelectInfo}) ->
    store_select_info(Socket, SelectInfo),
    ignore;
decode_recv_result(Socket, _AssocId, {completion, CompletionInfo}) ->
    store_select_info(Socket, CompletionInfo),
    ignore;
decode_recv_result(Socket, AssocId, {select_read, {SelectInfo, Message}}) ->
    store_select_info(Socket, SelectInfo),
    consume_message(Socket, AssocId, Message);
decode_recv_result(Socket, AssocId, {ok, Message}) ->
    consume_message(Socket, AssocId, Message);
decode_recv_result(Socket, AssocId, {error, Reason}) ->
    receive_error(Socket, AssocId, Reason);
decode_recv_result(Socket, AssocId, Message) when is_map(Message) ->
    consume_message(Socket, AssocId, Message).

consume_message(Socket, AssocId, Message) ->
    Result = association_message(Message, AssocId),
    consume_credit(Socket),
    Result.

association_message(Message, AssocId) ->
    case socket_message(Message, AssocId) of
        {ok, {event, _RemoteAddr, _RemotePort, Event}} -> {ok, {event, Event}};
        Result -> Result
    end.

consume_credit(Socket) ->
    case get(?RECV_STATE(Socket)) of
        #{remaining := infinity} = State ->
            continue_receive(Socket, State);
        #{remaining := Remaining} = State when Remaining > 1 ->
            continue_receive(Socket, State#{remaining => Remaining - 1});
        #{notify_passive := NotifyPassive} = State ->
            maybe_cancel_select(Socket, State),
            erase(?RECV_STATE(Socket)),
            _ = socket:setopt(Socket, {otp, select_read}, false),
            case NotifyPassive of
                true -> self() ! {sctp_passive, Socket};
                false -> ok
            end;
        _ ->
            ok
    end.

continue_receive(Socket, State) ->
    put(?RECV_STATE(Socket), State),
    case maps:get(select_info, State, undefined) of
        undefined ->
            case arm_receive(Socket, maps:get(assoc_id, State)) of
                ok ->
                    ok;
                {error, _} = Error ->
                    self() ! {?MODULE, Socket, recv, maps:get(assoc_id, State), Error}
            end;
        _ ->
            ok
    end.

maybe_cancel_select(Socket, #{select_info := SelectInfo}) when SelectInfo =/= undefined ->
    _ = socket:cancel(Socket, SelectInfo),
    ok;
maybe_cancel_select(_Socket, _State) ->
    ok.

store_select_info(Socket, SelectInfo) ->
    case get(?RECV_STATE(Socket)) of
        State when is_map(State) ->
            put(?RECV_STATE(Socket), State#{select_info => SelectInfo});
        _ ->
            ok
    end.

take_select_info(Socket, Handle) ->
    case get(?RECV_STATE(Socket)) of
        #{select_info := SelectInfo} = State ->
            case select_handle(SelectInfo) of
                Handle ->
                    put(?RECV_STATE(Socket), State#{select_info => undefined}),
                    true;
                _ ->
                    false
            end;
        _ ->
            false
    end.

select_handle({select_info, _Operation, Handle}) -> Handle;
select_handle({completion_info, _Operation, Handle}) -> Handle;
select_handle(_) -> undefined.

receive_error(Socket, AssocId, Reason) ->
    deactivate(Socket),
    {ok, {event, {socket_error, AssocId, Reason}}}.

socket_message(#{notification := Notification} = Message, _DefaultAssocId) ->
    {RemoteAddr, RemotePort} = remote(Message),
    {ok, {event, RemoteAddr, RemotePort, notification_record(Notification)}};
socket_message(#{iov := IOV, ctrl := Ctrl} = Message, DefaultAssocId) ->
    {RemoteAddr, RemotePort} = remote(Message),
    Info = receive_info(Ctrl, DefaultAssocId),
    {ok, {data, RemoteAddr, RemotePort, Info, iolist_to_binary(IOV)}};
socket_message(_Message, _DefaultAssocId) ->
    ignore.

remote(#{addr := #{addr := Address, port := Port}}) -> {Address, Port};
remote(_Message) -> {undefined, 0}.

receive_info(Ctrl, DefaultAssocId) ->
    case [Value || #{level := sctp, type := sndrcv, value := Value} <- Ctrl] of
        [Info | _] -> sndrcvinfo_record(Info, DefaultAssocId);
        [] -> #sctp_sndrcvinfo{assoc_id = default_assoc_id(DefaultAssocId)}
    end.

sndrcvinfo_record(Info, DefaultAssocId) ->
    #sctp_sndrcvinfo{
        stream = maps:get(stream, Info, 0),
        ssn = maps:get(ssn, Info, 0),
        flags = maps:get(flags, Info, []),
        ppid = network_to_host_32(maps:get(ppid, Info, 0)),
        context = maps:get(context, Info, 0),
        timetolive = maps:get(time_to_live, Info, 0),
        tsn = maps:get(tsn, Info, 0),
        cumtsn = maps:get(cum_tsn, Info, 0),
        assoc_id = maps:get(assoc_id, Info, default_assoc_id(DefaultAssocId))
    }.

notification_record(#{type := assoc_change} = Notification) ->
    #sctp_assoc_change{
        state = assoc_state(maps:get(state, Notification)),
        error = maps:get(error, Notification, 0),
        outbound_streams = maps:get(outbound_streams, Notification, 0),
        inbound_streams = maps:get(inbound_streams, Notification, 0),
        assoc_id = maps:get(assoc_id, Notification, 0)
    };
notification_record(#{type := peer_addr_change} = Notification) ->
    #sctp_paddr_change{
        addr = sockaddr_tuple(maps:get(addr, Notification, #{})),
        state = maps:get(state, Notification, addr_unreachable),
        error = maps:get(error, Notification, 0),
        assoc_id = maps:get(assoc_id, Notification, 0)
    };
notification_record(#{type := send_failed} = Notification) ->
    send_failed_record(Notification);
notification_record(#{type := send_failed_event} = Notification) ->
    send_failed_record(Notification);
notification_record(#{type := remote_error} = Notification) ->
    #sctp_remote_error{
        error = maps:get(error, Notification, 0),
        assoc_id = maps:get(assoc_id, Notification, 0),
        data = maps:get(remote_causes, Notification, [])
    };
notification_record(#{type := shutdown_event} = Notification) ->
    #sctp_shutdown_event{assoc_id = maps:get(assoc_id, Notification, 0)};
notification_record(#{type := adaptation_event} = Notification) ->
    #sctp_adaptation_event{
        adaptation_ind = maps:get(adaption_ind, Notification, 0),
        assoc_id = maps:get(assoc_id, Notification, 0)
    };
notification_record(#{type := partial_delivery_event} = Notification) ->
    #sctp_pdapi_event{
        indication = maps:get(indication, Notification, partial_delivery_aborted),
        assoc_id = maps:get(assoc_id, Notification, 0)
    };
notification_record(Notification) ->
    Notification.

send_failed_record(Notification) ->
    AssocId = maps:get(assoc_id, Notification, 0),
    #sctp_send_failed{
        flags = send_failed_sent(maps:get(flags, Notification, [])),
        error = maps:get(error, Notification, 0),
        info = send_failed_info(maps:get(info, Notification, #{}), AssocId),
        assoc_id = AssocId,
        data = maps:get(data, Notification, <<>>)
    }.

send_failed_sent(Flags) when is_list(Flags) -> lists:member(data_sent, Flags);
send_failed_sent(Flags) when is_integer(Flags) -> Flags =/= 0;
send_failed_sent(Flag) -> Flag =:= data_sent.

send_failed_info(#{sid := Stream} = Info, DefaultAssocId) ->
    #sctp_sndrcvinfo{
        stream = Stream,
        flags = maps:get(flags, Info, []),
        ppid = network_to_host_32(maps:get(ppid, Info, 0)),
        context = maps:get(context, Info, 0),
        assoc_id = maps:get(assoc_id, Info, maps:get(assic_id, Info, DefaultAssocId))
    };
send_failed_info(Info, DefaultAssocId) ->
    sndrcvinfo_record(Info, DefaultAssocId).

assoc_state(cant_str_assoc) -> cant_assoc;
assoc_state(State) -> State.

sockaddr_tuple(#{addr := Address, port := Port}) -> {Address, Port};
sockaddr_tuple(Other) -> Other.

address_domain(Address) when tuple_size(Address) =:= 4 -> inet;
address_domain(Address) when tuple_size(Address) =:= 8 -> inet6.

sockaddr(Domain, Address, Port) ->
    #{family => Domain, addr => Address, port => Port}.

default_assoc_id(undefined) -> 0;
default_assoc_id(AssocId) -> AssocId.

initmsg_map(Init) ->
    compact(#{
        num_outstreams => Init#sctp_initmsg.num_ostreams,
        max_instreams => Init#sctp_initmsg.max_instreams,
        max_attempts => Init#sctp_initmsg.max_attempts,
        max_init_timeo => Init#sctp_initmsg.max_init_timeo
    }).

associnfo_map(Assoc) ->
    compact(#{
        assoc_id => default_assoc_id(Assoc#sctp_assocparams.assoc_id),
        asocmaxrxt => Assoc#sctp_assocparams.asocmaxrxt,
        number_peer_destinations => Assoc#sctp_assocparams.number_peer_destinations,
        peer_rwnd => Assoc#sctp_assocparams.peer_rwnd,
        local_rwnd => Assoc#sctp_assocparams.local_rwnd,
        cookie_life => Assoc#sctp_assocparams.cookie_life
    }).

sndrcvinfo_map(Info) ->
    compact(#{
        stream => Info#sctp_sndrcvinfo.stream,
        flags => Info#sctp_sndrcvinfo.flags,
        ppid => host_to_network_32(default_integer(Info#sctp_sndrcvinfo.ppid)),
        context => Info#sctp_sndrcvinfo.context,
        time_to_live => Info#sctp_sndrcvinfo.timetolive,
        assoc_id => default_assoc_id(Info#sctp_sndrcvinfo.assoc_id)
    }).

compact(Map) ->
    maps:filter(fun(_Key, Value) -> Value =/= undefined end, Map).

default_integer(undefined) -> 0;
default_integer(Value) -> Value.

host_to_network_32(Value) ->
    <<Network:32/native>> = <<Value:32/big>>,
    Network.

network_to_host_32(Value) ->
    <<Host:32/big>> = <<Value:32/native>>,
    Host.
