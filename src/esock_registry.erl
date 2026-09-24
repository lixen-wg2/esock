-module(esock_registry).
-behaviour(gen_server).

-export([start_link/0, acquire/2, socket_down/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {
    sockets = #{} :: #{term() => {pid(), reference()}},
    monitors = #{} :: #{reference() => term()}
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

acquire(Options, Owner) ->
    try gen_server:call(?MODULE, {acquire, Options, Owner}, infinity) of
        Reply -> Reply
    catch
        exit:{noproc, _} -> {error, socket_stopping};
        exit:{normal, _} -> {error, socket_stopping};
        exit:{shutdown, _} -> {error, socket_stopping}
    end.

socket_down(Pid) ->
    gen_server:cast(?MODULE, {socket_down, Pid}).

init([]) ->
    {ok, #state{}}.

handle_call({acquire, Options, Owner}, _From, State0) ->
    case esock_socket:normalize_config(Options) of
        {ok, Key, Config} ->
            acquire_socket(Key, Config, Owner, State0);
        {error, _} = Error ->
            {reply, Error, State0}
    end;
handle_call(_Request, _From, State) ->
    {reply, {error, bad_request}, State}.

handle_cast({socket_down, Pid}, State0) ->
    {noreply, remove_pid(Pid, State0)};
handle_cast(_Message, State) ->
    {noreply, State}.

handle_info({'DOWN', Monitor, process, _Pid, _Reason}, #state{monitors = Monitors} = State0) ->
    case maps:take(Monitor, Monitors) of
        {Key, NewMonitors} ->
            Sockets = maps:remove(Key, State0#state.sockets),
            {noreply, State0#state{sockets = Sockets, monitors = NewMonitors}};
        error ->
            {noreply, State0}
    end;
handle_info(_Message, State) ->
    {noreply, State}.

acquire_socket(Key, Config, Owner, #state{sockets = Sockets} = State0) ->
    case maps:get(Key, Sockets, undefined) of
        {Pid, _Monitor} ->
            case esock_socket:acquire(Pid, Owner, Config) of
                {ok, _} = Reply ->
                    {reply, Reply, State0};
                {error, socket_stopping} ->
                    start_socket(Key, Config, Owner, remove_key(Key, State0));
                {error, _} = Error ->
                    {reply, Error, State0}
            end;
        undefined ->
            start_socket(Key, Config, Owner, State0)
    end.

start_socket(Key, Config, Owner, State0) ->
    case esock_socket_sup:start_socket(Config) of
        {ok, Pid} ->
            Monitor = erlang:monitor(process, Pid),
            Sockets = (State0#state.sockets)#{Key => {Pid, Monitor}},
            Monitors = (State0#state.monitors)#{Monitor => Key},
            State = State0#state{sockets = Sockets, monitors = Monitors},
            case esock_socket:acquire(Pid, Owner, Config) of
                {ok, _} = Reply ->
                    {reply, Reply, State};
                {error, _} = Error ->
                    {reply, Error, remove_key(Key, State)}
            end;
        {error, Reason} ->
            {reply, {error, Reason}, State0}
    end.

remove_pid(Pid, #state{sockets = Sockets} = State) ->
    Keys = [Key || {Key, {SocketPid, _}} <- maps:to_list(Sockets), SocketPid =:= Pid],
    lists:foldl(fun remove_key/2, State, Keys).

remove_key(Key, #state{sockets = Sockets, monitors = Monitors} = State) ->
    case maps:take(Key, Sockets) of
        {{_Pid, Monitor}, NewSockets} ->
            erlang:demonitor(Monitor, [flush]),
            State#state{
                sockets = NewSockets,
                monitors = maps:remove(Monitor, Monitors)
            };
        error ->
            State
    end.
