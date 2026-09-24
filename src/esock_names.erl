-module(esock_names).

-moduledoc false.

-export([socket/3, association/3]).

-type names_result() ::
    {ok, [{inet:ip_address(), inet:port_number()}]} | {error, term()}.

-spec socket(names_result(), [inet:ip_address()], inet:port_number()) ->
    {ok, [inet:ip_address()], inet:port_number()} | {error, term()}.
socket({ok, Names}, _ConfiguredAddrs, _ConfiguredPort) when Names =/= [] ->
    Ports = lists:usort([Port || {_Address, Port} <- Names]),
    case Ports of
        [Port] -> {ok, lists:usort([Address || {Address, _} <- Names]), Port};
        _ -> {error, {inconsistent_local_ports, Names}}
    end;
socket({ok, []}, ConfiguredAddrs, ConfiguredPort) ->
    {ok, ConfiguredAddrs, ConfiguredPort};
socket({error, Reason}, _ConfiguredAddrs, _ConfiguredPort) ->
    {error, {socknames, Reason}}.

-spec association(names_result(), [inet:ip_address()], inet:port_number()) ->
    {[inet:ip_address()], inet:port_number()}.
association({ok, [{_Address, Port} | _] = Values}, _DefaultAddrs, _DefaultPort) ->
    {[Address || {Address, ValuePort} <- Values, ValuePort =:= Port], Port};
association(_Result, DefaultAddrs, DefaultPort) ->
    {DefaultAddrs, DefaultPort}.
