-module(esock_socket_api).

-include_lib("kernel/include/inet_sctp.hrl").

-export([
    supported/0,
    open/4,
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

-export_type([async_info/0, connect_result/0]).

%% socket:sctp_assoc_id/0 is only exported in OTP 29. Keep older OTPs
%% compilable while supported/0 gates the SCTP socket capabilities at runtime.
-type assoc_id() :: integer().
-type async_info() :: socket:select_info() | socket:completion_info().
-type result() :: ok | {error, term()}.
-type open_result() ::
    {ok, socket:socket(), [inet:ip_address()], inet:port_number()} | {error, term()}.
-type connect_result() ::
    {ok, assoc_id(), undefined}
    | {ok, undefined, async_info() | undefined}
    | {error, term()}.
-type names_result() ::
    {ok, [{inet:ip_address(), inet:port_number()}]} | {error, term()}.
-type recv_result() ::
    {select, socket:select_info()}
    | {completion, socket:completion_info()}
    | {select_read, {socket:select_info(), socket:msg_recv()}}
    | {ok, socket:msg_recv()}
    | {error, term()}
    | socket:msg_recv().
-type listener_result() ::
    {ok, {event, inet:ip_address() | undefined, inet:port_number(), term()}}
    | {ok, {data, inet:ip_address() | undefined, inet:port_number(), #sctp_sndrcvinfo{}, binary()}}
    | pending
    | ignore
    | {error, term()}.
-type decode_result() :: {ok, esock:decoded_sctp_message()} | ignore.
-type recv_state() :: #{
    assoc_id := assoc_id(),
    generation := reference(),
    remaining := pos_integer() | infinity,
    notify_passive := boolean(),
    select_info := async_info() | undefined
}.

-define(RECV_STATE(Socket), {?MODULE, recv_state, Socket}).
%% Reassembly survives a temporary switch to passive mode.
-define(RECV_PARTIAL(Socket), {?MODULE, recv_partial, Socket}).
-define(RECV_CTRL_SIZE, 1024).
-define(MAX_MESSAGE_SIZE, 16 * 1024 * 1024).

-spec open(
    nonempty_list(inet:ip_address()),
    inet:port_number(),
    [esock:socket_option()],
    boolean()
) -> open_result().
open(LocalAddrs, LocalPort, Options, StartListening) ->
    case supported() of
        true -> open_socket(LocalAddrs, LocalPort, Options, StartListening);
        false -> {error, {unsupported_backend, socket}}
    end.

-spec supported() -> boolean().
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

-spec open_socket(
    nonempty_list(inet:ip_address()),
    inet:port_number(),
    [esock:socket_option()],
    boolean()
) -> open_result().
open_socket(LocalAddrs, LocalPort, Options, StartListening) ->
    Domain = address_domain(hd(LocalAddrs)),
    case socket:open(Domain, seqpacket, sctp, #{use_registry => false}) of
        {ok, Socket} ->
            case prepare_socket(Socket, Domain, LocalAddrs, LocalPort, Options, StartListening) of
                {ok, ActualAddrs, ActualPort} ->
                    {ok, Socket, ActualAddrs, ActualPort};
                {error, _} = Error ->
                    socket:close(Socket),
                    Error
            end;
        {error, Reason} ->
            {error, {open, Reason}}
    end.

-spec prepare_socket(
    socket:socket(),
    inet | inet6,
    nonempty_list(inet:ip_address()),
    inet:port_number(),
    [esock:socket_option()],
    boolean()
) ->
    {ok, [inet:ip_address()], inet:port_number()} | {error, term()}.
prepare_socket(Socket, Domain, LocalAddrs, LocalPort, Options, StartListening) ->
    case set_events(Socket) of
        ok ->
            case setopts(Socket, Options) of
                ok ->
                    case bind_addresses(Socket, Domain, LocalAddrs, LocalPort) of
                        ok ->
                            ListenResult =
                                case StartListening of
                                    true -> socket:listen(Socket, true);
                                    false -> ok
                                end,
                            case ListenResult of
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

-spec set_events(socket:socket()) -> result().
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

-spec bind_addresses(
    socket:socket(),
    inet | inet6,
    nonempty_list(inet:ip_address()),
    inet:port_number()
) -> result().
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

-spec socket_names(socket:socket(), [inet:ip_address()], inet:port_number()) ->
    {ok, [inet:ip_address()], inet:port_number()} | {error, term()}.
socket_names(Socket, ConfiguredAddrs, ConfiguredPort) ->
    esock_names:socket(socknames(Socket, 0), ConfiguredAddrs, ConfiguredPort).

-spec close(socket:socket()) -> result().
close(Socket) ->
    erase(?RECV_STATE(Socket)),
    erase(?RECV_PARTIAL(Socket)),
    case socket:close(Socket) of
        ok -> ok;
        {error, closed} -> ok;
        {error, _} = Error -> Error
    end.

-spec arm_listener(socket:socket()) -> ok.
arm_listener(Socket) ->
    self() ! {'$socket', Socket, select, nowait},
    ok.

-spec listener_message(socket:socket(), term()) -> listener_result().
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

-spec listener_recv_result(socket:socket(), recv_result()) -> listener_result().
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

-spec peeloff(socket:socket(), assoc_id()) -> {ok, socket:socket()} | {error, term()}.
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

-spec controlling_process(socket:socket(), pid()) -> result().
controlling_process(Socket, Owner) ->
    socket:setopt(Socket, {otp, controlling_process}, Owner).

-spec setopts(socket:socket(), [esock:socket_option()]) -> result().
setopts(Socket, Options) ->
    lists:foldl(
        fun
            (_Option, {error, _} = Error) -> Error;
            (Option, ok) -> setopt(Socket, Option)
        end,
        ok,
        Options
    ).

-spec association_setopts(socket:socket(), assoc_id(), [esock:socket_option()]) -> result().
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

-spec setopt(socket:socket(), esock:socket_option()) -> result().
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

-spec connect(
    socket:socket(),
    inet:ip_address(),
    inet:port_number(),
    [esock:socket_option()]
) -> connect_result().
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

-spec finish_connect(socket:socket()) -> {ok, undefined} | {error, term()}.
finish_connect(Socket) ->
    normalize_connect_result(socket:connect(Socket)).

-spec normalize_connect_result(result()) -> {ok, undefined} | {error, term()}.
normalize_connect_result(ok) -> {ok, undefined};
normalize_connect_result({error, _} = Error) -> Error.

-spec cancel(socket:socket(), async_info()) -> result().
cancel(Socket, SelectInfo) ->
    socket:cancel(Socket, SelectInfo).

-spec abort(socket:socket(), assoc_id() | undefined) -> result().
abort(_Socket, undefined) ->
    ok;
abort(Socket, AssocId) ->
    send(Socket, AssocId, 0, 0, 0, 0, [abort], <<>>).

-spec socknames(socket:socket(), assoc_id()) -> names_result().
socknames(Socket, AssocId) ->
    names(apply(socket, socknames, [Socket, AssocId])).

-spec peernames(socket:socket(), assoc_id()) -> names_result().
peernames(Socket, AssocId) ->
    names(apply(socket, peernames, [Socket, AssocId])).

-spec names({ok, [socket:sockaddr_in() | socket:sockaddr_in6()]} | {error, term()}) ->
    names_result().
names({ok, SockAddrs}) ->
    {ok, [
        {Address, Port}
     || #{addr := Address, port := Port} <- SockAddrs
    ]};
names({error, _} = Error) ->
    Error.

-spec activate(socket:socket(), assoc_id(), esock:active_mode() | -32768..0) -> result().
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

-spec start_receive(socket:socket(), assoc_id(), pos_integer() | infinity, boolean()) -> result().
start_receive(Socket, AssocId, Remaining, NotifyPassive) ->
    deactivate(Socket),
    case socket:setopt(Socket, {otp, select_read}, false) of
        ok ->
            put(?RECV_STATE(Socket), #{
                assoc_id => AssocId,
                generation => make_ref(),
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

-spec arm_receive(socket:socket(), assoc_id()) -> result().
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
            %% A queued result must not consume credit from a later activation.
            self() ! {?MODULE, Socket, recv, AssocId, {receive_generation(Socket), Result}},
            ok
    end.

-spec deactivate(socket:socket()) -> ok.
deactivate(Socket) ->
    case erase(?RECV_STATE(Socket)) of
        #{select_info := SelectInfo} when SelectInfo =/= undefined ->
            _ = socket:cancel(Socket, SelectInfo);
        _ ->
            ok
    end,
    _ = socket:setopt(Socket, {otp, select_read}, false),
    ok.

-spec send(socket:socket(), assoc_id(), iodata()) -> result().
send(Socket, _AssocId, Data) ->
    normalize_send_result(socket:send(Socket, Data)).

-spec send(
    socket:socket(),
    assoc_id(),
    non_neg_integer(),
    non_neg_integer(),
    non_neg_integer(),
    non_neg_integer(),
    [atom()],
    binary()
) -> result().
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

-spec normalize_send_result(ok | {ok, binary() | erlang:iovec()} | {error, term()}) -> result().
normalize_send_result(ok) -> ok;
normalize_send_result({ok, []}) -> ok;
normalize_send_result({ok, Rest}) -> {error, {partial_send, Rest}};
normalize_send_result({error, _} = Error) -> Error.

-spec decode_sctp(socket:socket(), assoc_id(), term()) -> decode_result().
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
decode_sctp(Socket, AssocId, {?MODULE, Socket, recv, AssocId, {Generation, Result}}) when
    is_reference(Generation)
->
    decode_recv_result(Socket, AssocId, Generation, Result);
decode_sctp(Socket, AssocId, {?MODULE, Socket, recv, AssocId, Result}) ->
    decode_recv_result(Socket, AssocId, Result);
decode_sctp(_Socket, _AssocId, _Message) ->
    ignore.

-spec decode_recv_result(socket:socket(), assoc_id(), recv_result()) -> decode_result().
decode_recv_result(Socket, AssocId, Result) ->
    decode_recv_result(Socket, AssocId, receive_generation(Socket), Result).

decode_recv_result(Socket, _AssocId, _Generation, {select, SelectInfo}) ->
    store_select_info(Socket, SelectInfo),
    ignore;
decode_recv_result(Socket, _AssocId, _Generation, {completion, CompletionInfo}) ->
    store_select_info(Socket, CompletionInfo),
    ignore;
decode_recv_result(Socket, AssocId, Generation, {select_read, {SelectInfo, Message}}) ->
    store_select_info(Socket, SelectInfo),
    consume_message(Socket, AssocId, Generation, Message);
decode_recv_result(Socket, AssocId, Generation, {ok, Message}) ->
    consume_message(Socket, AssocId, Generation, Message);
decode_recv_result(Socket, AssocId, Generation, {error, Reason}) ->
    case receive_generation(Socket) of
        Generation -> receive_error(Socket, AssocId, Reason);
        _ -> ignore
    end;
decode_recv_result(Socket, AssocId, Generation, Message) when is_map(Message) ->
    consume_message(Socket, AssocId, Generation, Message).

-spec consume_message(socket:socket(), assoc_id(), reference() | undefined, socket:msg_recv()) ->
    decode_result().
consume_message(Socket, AssocId, Generation, Message) ->
    case assemble_message(Socket, AssocId, Message) of
        pending ->
            case get(?RECV_STATE(Socket)) of
                #{generation := Generation} = State -> continue_receive(Socket, State);
                _ -> ok
            end,
            ignore;
        {error, Reason} ->
            receive_error(Socket, AssocId, Reason);
        Result ->
            consume_credit(Socket, Generation),
            Result
    end.

assemble_message(Socket, AssocId, #{notification := Notification} = Message) ->
    case Notification of
        #{type := partial_delivery} ->
            erase(?RECV_PARTIAL(Socket));
        #{type := partial_delivery_event} ->
            erase(?RECV_PARTIAL(Socket));
        #{type := shutdown_event} ->
            erase(?RECV_PARTIAL(Socket));
        #{type := assoc_change, state := State} when State =/= comm_up ->
            erase(?RECV_PARTIAL(Socket));
        _ ->
            ok
    end,
    association_message(Message, AssocId);
assemble_message(Socket, AssocId, #{iov := _, ctrl := _} = Message) ->
    {ok, {data, RemoteAddr, RemotePort, Info, Data}} = association_message(Message, AssocId),
    EOR = lists:member(eor, maps:get(flags, Message, [])),
    case get(?RECV_PARTIAL(Socket)) of
        undefined when byte_size(Data) > ?MAX_MESSAGE_SIZE ->
            {error, {message_too_large, ?MAX_MESSAGE_SIZE}};
        undefined when EOR ->
            {ok, {data, RemoteAddr, RemotePort, Info, Data}};
        undefined ->
            put(?RECV_PARTIAL(Socket), {RemoteAddr, RemotePort, Info, [Data], byte_size(Data)}),
            pending;
        {FirstAddr, FirstPort, FirstInfo, Chunks, Size} ->
            TotalSize = Size + byte_size(Data),
            case TotalSize > ?MAX_MESSAGE_SIZE of
                true ->
                    erase(?RECV_PARTIAL(Socket)),
                    {error, {message_too_large, ?MAX_MESSAGE_SIZE}};
                false when EOR ->
                    erase(?RECV_PARTIAL(Socket)),
                    Payload = iolist_to_binary(lists:reverse([Data | Chunks])),
                    {ok, {data, FirstAddr, FirstPort, FirstInfo, Payload}};
                false ->
                    put(?RECV_PARTIAL(Socket), {
                        FirstAddr, FirstPort, FirstInfo, [Data | Chunks], TotalSize
                    }),
                    pending
            end
    end;
assemble_message(_Socket, AssocId, Message) ->
    association_message(Message, AssocId).

-spec association_message(socket:msg_recv(), assoc_id()) -> decode_result().
association_message(Message, AssocId) ->
    case socket_message(Message, AssocId) of
        {ok, {event, _RemoteAddr, _RemotePort, Event}} -> {ok, {event, Event}};
        Result -> Result
    end.

consume_credit(Socket, Generation) ->
    case get(?RECV_STATE(Socket)) of
        #{generation := Generation, remaining := infinity} = State ->
            continue_receive(Socket, State);
        #{generation := Generation, remaining := Remaining} = State when Remaining > 1 ->
            continue_receive(Socket, State#{remaining => Remaining - 1});
        #{generation := Generation, notify_passive := NotifyPassive} = State ->
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

-spec continue_receive(socket:socket(), recv_state()) -> ok.
continue_receive(Socket, State) ->
    put(?RECV_STATE(Socket), State),
    case maps:get(select_info, State, undefined) of
        undefined ->
            case arm_receive(Socket, maps:get(assoc_id, State)) of
                ok ->
                    ok;
                {error, _} = Error ->
                    self() !
                        {?MODULE, Socket, recv, maps:get(assoc_id, State), {
                            maps:get(generation, State), Error
                        }},
                    ok
            end;
        _ ->
            ok
    end.

receive_generation(Socket) ->
    case get(?RECV_STATE(Socket)) of
        #{generation := Generation} -> Generation;
        _ -> undefined
    end.

-spec maybe_cancel_select(socket:socket(), recv_state()) -> ok.
maybe_cancel_select(Socket, #{select_info := SelectInfo}) when SelectInfo =/= undefined ->
    _ = socket:cancel(Socket, SelectInfo),
    ok;
maybe_cancel_select(_Socket, _State) ->
    ok.

-spec store_select_info(socket:socket(), async_info()) -> recv_state() | ok.
store_select_info(Socket, SelectInfo) ->
    case get(?RECV_STATE(Socket)) of
        State when is_map(State) ->
            put(?RECV_STATE(Socket), State#{select_info => SelectInfo});
        _ ->
            ok
    end.

-spec take_select_info(socket:socket(), reference()) -> boolean().
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

-spec select_handle(async_info() | undefined) -> reference() | undefined.
select_handle({select_info, _Operation, Handle}) -> Handle;
select_handle({completion_info, _Operation, Handle}) -> Handle;
select_handle(_) -> undefined.

-spec receive_error(socket:socket(), assoc_id(), term()) -> {ok, {event, term()}}.
receive_error(Socket, AssocId, Reason) ->
    deactivate(Socket),
    erase(?RECV_PARTIAL(Socket)),
    {ok, {event, {socket_error, AssocId, Reason}}}.

-spec socket_message(socket:msg_recv(), assoc_id() | undefined) ->
    {ok, {event, inet:ip_address() | undefined, inet:port_number(), term()}}
    | {ok, {data, inet:ip_address() | undefined, inet:port_number(), #sctp_sndrcvinfo{}, binary()}}
    | ignore.
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
notification_record(#{type := partial_delivery} = Notification) ->
    notification_record(Notification#{type => partial_delivery_event});
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

-spec address_domain(inet:ip_address()) -> inet | inet6.
address_domain(Address) when tuple_size(Address) =:= 4 -> inet;
address_domain(Address) when tuple_size(Address) =:= 8 -> inet6.

-spec sockaddr(inet | inet6, inet:ip_address(), inet:port_number()) ->
    socket:sockaddr_in() | socket:sockaddr_in6().
sockaddr(Domain, Address, Port) ->
    #{family => Domain, addr => Address, port => Port}.

-spec default_assoc_id(assoc_id() | undefined) -> assoc_id().
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
