-record(esock_socket_ref, {
    pid :: pid(),
    lease :: reference()
}).

-record(esock_assoc_ref, {
    backend = gen_sctp :: gen_sctp | socket,
    socket :: term(),
    assoc_id :: non_neg_integer(),
    local_addrs = [] :: [inet:ip_address()],
    local_port :: inet:port_number(),
    remote_addrs = [] :: [inet:ip_address()],
    remote_port :: inet:port_number(),
    inbound_streams = 0 :: non_neg_integer(),
    outbound_streams = 0 :: non_neg_integer()
}).
