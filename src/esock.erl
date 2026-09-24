-module(esock).

-moduledoc """
Shared SCTP sockets with peer routing and direct association ownership.

A socket is shared by callers that use the same local address set and port.
Incoming associations are matched once against registered peers. After an
association is accepted, its peeled-off SCTP socket is controlled directly by
the association owner; payload does not pass through the shared socket process.
""".

-include("esock.hrl").
-include_lib("kernel/include/inet_sctp.hrl").

-export([
    open/1,
    release/1,
    socknames/1,
    socket_id/1,
    register_peer/2,
    unregister_peer/2,
    connect/2,
    cancel_connect/2,
    accept/3,
    accept/4,
    activate/2,
    setopts/2,
    send/2,
    send/3,
    send/5,
    close/1,
    association_info/1,
    decode/2,
    decode_sctp/2
]).

-export_type([
    address/0,
    port_number/0,
    socket_ref/0,
    association/0,
    socket_options/0,
    peer_options/0,
    connect_options/0,
    send_options/0,
    peer_info/0,
    association_info/0,
    decoded_message/0,
    decoded_sctp_message/0
]).

-type address() :: inet:ip_address() | loopback | any.
-type port_number() :: inet:port_number().
-opaque socket_ref() :: #esock_socket_ref{}.
-opaque association() :: #esock_assoc_ref{}.

-type socket_options() :: #{
    backend => gen_sctp | socket,
    local_addrs => [address()],
    local_port => port_number(),
    socket_options => [gen_sctp:option()],
    pending_timeout => pos_integer() | infinity,
    max_pending => pos_integer() | infinity
}.

-type peer_options() :: #{
    remote_addrs => [address()] | any,
    remote_port => port_number() | any,
    acceptor => pid()
}.

-type connect_options() :: #{
    remote_addrs := [address()],
    remote_port := port_number(),
    owner => pid(),
    connect_options => [gen_sctp:option()]
}.

-type send_options() :: #{
    stream => non_neg_integer(),
    ppid => non_neg_integer(),
    context => non_neg_integer(),
    timetolive => non_neg_integer(),
    flags => [atom()]
}.

-type peer_info() :: #{
    local_addrs := [inet:ip_address()],
    local_port := port_number(),
    remote_addrs := [inet:ip_address()],
    remote_port := port_number(),
    inbound_streams := non_neg_integer(),
    outbound_streams := non_neg_integer()
}.

-type association_info() :: #{
    backend := gen_sctp | socket,
    assoc_id := non_neg_integer(),
    socket := term(),
    local_addrs := [inet:ip_address()],
    local_port := port_number(),
    remote_addrs := [inet:ip_address()],
    remote_port := port_number(),
    inbound_streams := non_neg_integer(),
    outbound_streams := non_neg_integer()
}.

-type decoded_message() ::
    {data, map(), binary()}
    | {event, term()}.

-type decoded_sctp_message() ::
    {data, inet:ip_address(), inet:port_number(), #sctp_sndrcvinfo{}, binary()}
    | {event, term()}.

-doc """
Acquire a shared SCTP socket.

Calls using the same non-zero local port and normalized local address set
return leases on the same socket. Port zero always creates a new socket.
""".
-spec open(socket_options()) -> {ok, socket_ref()} | {error, term()}.
open(Options) ->
    esock_registry:acquire(Options, self()).

-doc "Release this caller's lease on a shared socket.".
-spec release(socket_ref()) -> ok | {error, term()}.
release(#esock_socket_ref{pid = Pid, lease = Lease}) ->
    esock_socket:release(Pid, Lease).

-doc "Return all local addresses and their common port.".
-spec socknames(socket_ref()) -> {ok, {[inet:ip_address()], port_number()}} | {error, term()}.
socknames(#esock_socket_ref{pid = Pid}) ->
    esock_socket:socknames(Pid).

-doc "Return the identity of the shared socket process.".
-spec socket_id(socket_ref()) -> pid().
socket_id(#esock_socket_ref{pid = Pid}) ->
    Pid.

-doc """
Register an acceptor for a remote SCTP peer.

The acceptor receives
`{esock, PeerRef, {association, SocketRef, PendingRef, PeerInfo}}`.
Registration is one-shot and is consumed by the next matching association.
""".
-spec register_peer(socket_ref(), peer_options()) ->
    {ok, reference()} | {error, term()}.
register_peer(#esock_socket_ref{pid = Pid, lease = Lease}, Options) ->
    esock_socket:register_peer(Pid, Lease, Options, self()).

-doc "Remove a peer registration that has not yet been matched.".
-spec unregister_peer(socket_ref(), reference()) -> ok | {error, term()}.
unregister_peer(#esock_socket_ref{pid = Pid, lease = Lease}, PeerRef) ->
    esock_socket:unregister_peer(Pid, Lease, PeerRef).

-doc "Start an asynchronous outgoing association.".
-spec connect(socket_ref(), connect_options()) -> {ok, reference()} | {error, term()}.
connect(#esock_socket_ref{pid = Pid, lease = Lease}, Options) ->
    esock_socket:connect(Pid, Lease, Options, self()).

-doc "Cancel an outgoing connection attempt.".
-spec cancel_connect(socket_ref(), reference()) -> ok | {error, term()}.
cancel_connect(#esock_socket_ref{pid = Pid, lease = Lease}, ConnectRef) ->
    esock_socket:cancel_connect(Pid, Lease, ConnectRef).

-doc "Transfer a pending association to an owner process.".
-spec accept(socket_ref(), reference(), pid()) ->
    {ok, association()} | {error, term()}.
accept(Socket, PendingRef, Owner) ->
    accept(Socket, PendingRef, Owner, []).

-doc "Transfer a pending association and set options before handoff.".
-spec accept(socket_ref(), reference(), pid(), [gen_sctp:option()]) ->
    {ok, association()} | {error, term()}.
accept(#esock_socket_ref{pid = Pid, lease = Lease}, PendingRef, Owner, Options) when
    is_pid(Owner), is_list(Options)
->
    esock_socket:accept(Pid, Lease, PendingRef, Owner, Options);
accept(#esock_socket_ref{}, PendingRef, Owner, Options) ->
    {error,
        {invalid_accept_options, #{
            pending_ref => PendingRef,
            owner => Owner,
            options => Options
        }}}.

-doc "Configure active delivery on an association owned by the caller.".
-spec activate(association(), false | true | once | pos_integer()) -> ok | {error, term()}.
activate(Assoc, Active) ->
    setopts(Assoc, [{active, Active}]).

-doc "Set options on a peeled association socket.".
-spec setopts(association(), [gen_sctp:option()]) -> ok | {error, term()}.
setopts(#esock_assoc_ref{backend = gen_sctp, socket = Socket}, Options) when is_list(Options) ->
    inet:setopts(Socket, Options);
setopts(#esock_assoc_ref{backend = socket, socket = Socket, assoc_id = AssocId}, Options) when
    is_list(Options)
->
    esock_socket_api:association_setopts(Socket, AssocId, Options);
setopts(#esock_assoc_ref{}, Options) ->
    {error, {invalid_socket_options, Options}}.

-doc "Send on stream zero using the association's default send parameters.".
-spec send(association(), binary()) -> ok | {error, term()}.
send(Assoc, Data) when is_binary(Data) ->
    #esock_assoc_ref{backend = Backend, socket = Socket, assoc_id = AssocId} = Assoc,
    case Backend of
        gen_sctp -> gen_sctp:send(Socket, AssocId, 0, Data);
        socket -> esock_socket_api:send(Socket, AssocId, Data)
    end.

-doc "Send SCTP data with per-message metadata.".
-spec send(association(), send_options(), binary()) -> ok | {error, term()}.
send(#esock_assoc_ref{backend = Backend, socket = Socket, assoc_id = AssocId}, Options, Data) when
    is_map(Options), is_binary(Data)
->
    Stream = maps:get(stream, Options, 0),
    PPID = maps:get(ppid, Options, 0),
    Context = maps:get(context, Options, 0),
    TimeToLive = maps:get(timetolive, Options, 0),
    Flags = maps:get(flags, Options, []),
    case Backend of
        gen_sctp ->
            Info = #sctp_sndrcvinfo{
                assoc_id = AssocId,
                stream = Stream,
                ppid = PPID,
                context = Context,
                timetolive = TimeToLive,
                flags = Flags
            },
            gen_sctp:send(Socket, Info, Data);
        socket ->
            esock_socket_api:send(
                Socket, AssocId, Stream, PPID, Context, TimeToLive, Flags, Data
            )
    end.

-doc "Send SCTP data with the common stream, PPID, and flags metadata.".
-spec send(
    association(),
    non_neg_integer(),
    non_neg_integer(),
    [atom()],
    binary()
) -> ok | {error, term()}.
send(
    #esock_assoc_ref{backend = Backend, socket = Socket, assoc_id = AssocId},
    Stream,
    PPID,
    Flags,
    Data
) when
    is_binary(Data)
->
    case Backend of
        gen_sctp ->
            Info = #sctp_sndrcvinfo{
                assoc_id = AssocId,
                stream = Stream,
                ppid = PPID,
                flags = Flags
            },
            gen_sctp:send(Socket, Info, Data);
        socket ->
            esock_socket_api:send(Socket, AssocId, Stream, PPID, 0, 0, Flags, Data)
    end.

-doc "Close a peeled association.".
-spec close(association()) -> ok | {error, term()}.
close(#esock_assoc_ref{backend = gen_sctp, socket = Socket}) ->
    gen_sctp:close(Socket);
close(#esock_assoc_ref{backend = socket, socket = Socket}) ->
    esock_socket_api:close(Socket).

-doc "Return association metadata and the peeled socket.".
-spec association_info(association()) -> association_info().
association_info(#esock_assoc_ref{} = Assoc) ->
    #{
        backend => Assoc#esock_assoc_ref.backend,
        socket => Assoc#esock_assoc_ref.socket,
        assoc_id => Assoc#esock_assoc_ref.assoc_id,
        local_addrs => Assoc#esock_assoc_ref.local_addrs,
        local_port => Assoc#esock_assoc_ref.local_port,
        remote_addrs => Assoc#esock_assoc_ref.remote_addrs,
        remote_port => Assoc#esock_assoc_ref.remote_port,
        inbound_streams => Assoc#esock_assoc_ref.inbound_streams,
        outbound_streams => Assoc#esock_assoc_ref.outbound_streams
    }.

-doc "Decode a raw active-mode `gen_sctp` message for an association.".
-spec decode(association(), term()) -> {ok, decoded_message()} | ignore.
decode(Assoc, Message) ->
    case decode_sctp(Assoc, Message) of
        {ok, {data, RemoteAddr, RemotePort, Info, Data}} ->
            decode_data(RemoteAddr, RemotePort, Info, Data);
        Other ->
            Other
    end.

decode_data(RemoteAddr, RemotePort, Info, Data) ->
    Metadata = #{
        remote_addr => RemoteAddr,
        remote_port => RemotePort,
        stream => Info#sctp_sndrcvinfo.stream,
        ppid => Info#sctp_sndrcvinfo.ppid,
        context => Info#sctp_sndrcvinfo.context,
        flags => Info#sctp_sndrcvinfo.flags,
        assoc_id => Info#sctp_sndrcvinfo.assoc_id
    },
    {ok, {data, Metadata, Data}}.

-doc "Decode an active-mode message without allocating a metadata map.".
-spec decode_sctp(association(), term()) -> {ok, decoded_sctp_message()} | ignore.
decode_sctp(
    #esock_assoc_ref{backend = gen_sctp, socket = Socket},
    {sctp, Socket, RemoteAddr, RemotePort, {[#sctp_sndrcvinfo{} = Info], Data}}
) when is_binary(Data) ->
    {ok, {data, RemoteAddr, RemotePort, Info, Data}};
decode_sctp(
    #esock_assoc_ref{backend = gen_sctp, socket = Socket},
    {sctp, Socket, _RemoteAddr, _RemotePort, {_Ancillary, Event}}
) ->
    {ok, {event, Event}};
decode_sctp(
    #esock_assoc_ref{backend = gen_sctp, socket = Socket},
    {sctp, Socket, _RemoteAddr, _RemotePort, Event}
) ->
    {ok, {event, Event}};
decode_sctp(
    #esock_assoc_ref{backend = socket, socket = Socket, assoc_id = AssocId},
    Message
) ->
    esock_socket_api:decode_sctp(Socket, AssocId, Message);
decode_sctp(_Assoc, _Message) ->
    ignore.
