-module(esock_socket_sup).
-behaviour(supervisor).

-export([start_link/0, start_socket/1]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_socket(Config) ->
    ChildSpec = #{
        id => make_ref(),
        start => {esock_socket, start_link, [Config]},
        restart => temporary,
        shutdown => 5000,
        type => worker,
        modules => [esock_socket]
    },
    supervisor:start_child(?MODULE, ChildSpec).

init([]) ->
    {ok, {#{strategy => one_for_one, intensity => 5, period => 10}, []}}.
