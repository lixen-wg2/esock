-module(esock_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Children = [
        #{
            id => esock_registry,
            start => {esock_registry, start_link, []},
            type => worker
        },
        #{
            id => esock_socket_sup,
            start => {esock_socket_sup, start_link, []},
            type => supervisor
        }
    ],
    {ok, {#{strategy => rest_for_one, intensity => 5, period => 10}, Children}}.
