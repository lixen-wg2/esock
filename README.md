# esock

`esock` shares an SCTP listening socket between independent protocol users and
routes each new association by the remote address set and remote port.

The shared process only handles socket acquisition and association setup. As
soon as an association is accepted, its peeled SCTP socket is transferred to
the caller-selected owner process. Data does not pass through a central Erlang
process.

This is useful for telco systems where several logical peers must use the same
local address and port, while the remote peers use known source ports.

## Requirements

- Erlang/OTP 27 or later with `gen_sctp` support
- SCTP support in the operating system

The default backend is `gen_sctp`. Erlang/OTP 29 or later can instead use the
experimental `socket` backend:

```erlang
{ok, Socket} = esock:open(#{
    backend => socket,
    local_addrs => [{127, 0, 0, 1}],
    local_port => 3868
}).
```

The backend is part of a shared socket's identity, so leases using different
backends never share a listener. The public association API and decoded SCTP
records are the same for both backends.

OTP 29.0.x has two SCTP socket-option limitations on Linux: its `socket` NIF
rejects the documented `rtoinfo` map and reports `peer_addr_params` as
unsupported. The `socket` backend accepts the corresponding `gen_sctp`
options for configuration compatibility but leaves those two settings at the
kernel defaults. Do not use it where custom RTO or heartbeat settings are a
deployment requirement.

The `gen_sctp`-specific `buffer` and `non_block_send` options are also
accepted for configuration compatibility but have no direct `socket` API
equivalent. Use `sndbuf` and `recbuf` when kernel buffer sizing is required.

The backend also avoids OTP's `select_read` optimization because OTP 29.0.x
drops SCTP ancillary metadata on that path. Active-N credit is implemented
with asynchronous `recvmsg` operations instead, preserving stream and PPID
metadata. The `socket` backend accepts positive Active-N values; the negative
counter adjustments supported by `gen_sctp` are not part of esock's portable
API.

## Basic use

Start the application and acquire a listening socket:

```erlang
{ok, _} = application:ensure_all_started(esock),

{ok, Socket} = esock:open(#{
    local_addrs => [{127, 0, 0, 1}],
    local_port => 3868
}).
```

Calls with the same non-zero local address set and port acquire separate leases
on the same socket. Port `0` always creates a distinct socket.

Unmatched incoming associations are retained only until `pending_timeout`, which
defaults to 10 seconds. The unmatched pending backlog is also capped by
`max_pending`, which defaults to 1024; associations above the cap are closed.
Set a higher limit only when the listener can tolerate the corresponding
file-descriptor and memory use. `pending_timeout => infinity` disables
time-based eviction but does not disable the cap. `max_pending => infinity`
restores an unbounded pending backlog and should be used only with an external
admission or rate limit.

For example:

```erlang
{ok, Socket} = esock:open(#{
    local_addrs => [{127, 0, 0, 1}],
    local_port => 3868,
    pending_timeout => infinity,
    max_pending => 256
}).
```

Register a fixed peer before it connects:

```erlang
{ok, PeerRef} = esock:register_peer(Socket, #{
    remote_addrs => [{127, 0, 0, 2}],
    remote_port => 13868,
    acceptor => self()
}).
```

The registration is one-shot. The acceptor receives a setup message for the
next matching association:

```erlang
receive
    {esock, PeerRef, {association, Socket, PendingRef, PeerInfo}} ->
        {ok, Association} = esock:accept(Socket, PendingRef, self()),
        ok = esock:activate(Association, once)
end.
```

Active SCTP messages are delivered directly to the association owner. Decode
them without exposing the internal association representation:

```erlang
receive
    Message ->
        case esock:decode(Association, Message) of
            {ok, {data, Metadata, Payload}} ->
                io:format("stream=~p ppid=~p data=~p~n", [
                    maps:get(stream, Metadata),
                    maps:get(ppid, Metadata),
                    Payload
                ]),
                ok = esock:activate(Association, once);
            {ok, {event, Event}} ->
                io:format("SCTP event: ~p~n", [Event]);
            ignore ->
                ok
        end
end.
```

Code on a latency-sensitive data path can use `esock:decode_sctp/2` to receive
the native `#sctp_sndrcvinfo{}` record without allocating the metadata map used
by `esock:decode/2`.

Send and close the association:

```erlang
ok = esock:send(Association, #{stream => 1, ppid => 46}, Data),
ok = esock:close(Association).
```

The common stream, PPID, and flags fields also have an allocation-light send
form:

```erlang
ok = esock:send(Association, 1, 46, [], Data).
```

Release the listening socket lease when the user no longer needs to accept or
create associations:

```erlang
ok = esock:release(Socket).
```

The listening socket remains alive while another lease exists. Leases and peer
registrations are also removed automatically when their owner processes die.

## Multiple peers on one local port

Distinct fixed remote ports can be registered on the same socket:

```erlang
{ok, PeerA} = esock:register_peer(Socket, #{
    remote_addrs => [{10, 0, 0, 20}],
    remote_port => 2905
}),
{ok, PeerB} = esock:register_peer(Socket, #{
    remote_addrs => [{10, 0, 0, 20}],
    remote_port => 2906
}).
```

Overlapping registrations are rejected. For example, a wildcard registration
cannot coexist with a fixed registration that it could also match. This keeps
association assignment deterministic.

## Outgoing associations

Outgoing setup is asynchronous:

```erlang
{ok, ConnectRef} = esock:connect(Socket, #{
    remote_addrs => [{10, 0, 0, 30}],
    remote_port => 3868,
    owner => self()
}),

receive
    {esock, ConnectRef, {connected, Association, PeerInfo}} ->
        ok = esock:activate(Association, once);
    {esock, ConnectRef, {error, Reason}} ->
        error(Reason)
end.
```

Canceling an outgoing setup permits a new attempt to the same endpoint. The
replacement gets its `ConnectRef` immediately. When the kernel has not assigned
an association ID yet, esock finishes the in-flight socket operation before it
starts another and retains the canceled attempt until its terminal association
event arrives. This prevents a late event from completing the replacement
attempt or redirecting a different pending connection.

## Concurrency model

- `esock_registry` is used only when acquiring a socket. It is not in the data
  path.
- The supervisor owns process lifecycle and is never searched as a registry.
- One process owns each shared one-to-many listening socket.
- Each accepted association is peeled off and transferred to its own owner.
- Payload handling, decoding, and protocol serialization are therefore local
  to the association owner and cannot serialize traffic for unrelated peers.

## Tests

```shell
rebar3 eunit
```

Format the Erlang sources and configuration:

```shell
rebar3 fmt
```

Check formatting without changing files:

```shell
rebar3 fmt --check
```
