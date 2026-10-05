-record(esock_socket_ref, {
    pid :: pid(),
    lease :: reference()
}).

-record(esock_assoc_ref, {
    backend = gen_sctp :: gen_sctp | socket,
    socket :: gen_sctp:sctp_socket() | socket:socket(),
    assoc_id :: gen_sctp:assoc_id(),
    local_addrs = [] :: [inet:ip_address()],
    local_port :: inet:port_number(),
    remote_addrs = [] :: [inet:ip_address()],
    remote_port :: inet:port_number(),
    inbound_streams = 0 :: non_neg_integer(),
    outbound_streams = 0 :: non_neg_integer()
}).

%% Refining both fields preserves the backend/socket relationship.
-type esock_assoc() ::
    #esock_assoc_ref{backend :: gen_sctp, socket :: gen_sctp:sctp_socket()}
    | #esock_assoc_ref{backend :: socket, socket :: socket:socket(), assoc_id :: integer()}.
