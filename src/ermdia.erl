%%%-------------------------------------------------------------------
%%% File    : ermdia.erl
%%% Author  : Yuuki Takano <ytakano@jaist.ac.jp>
%%% Description : 
%%%
%%% Created : 22 Dec 2008 by Yuuki Takano <ytakano@jaist.ac.jp>
%%%-------------------------------------------------------------------
-module(ermdia).

-behaviour(gen_server).

%% API
-export([start/2, start/3, start_link/2, start_link/3, stop/1]).
-export([join/3]).

-export([get_id/1]).
-export([put_data/3, get_data/4, find_value/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {udp}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Server, Port) ->
    start_link(Server, Port, false).
start_link(Server, Port, IsGlobal) ->
    gen_server:start_link({local, Server}, ?MODULE,
                          [Server, Port, IsGlobal], []).

start(Server, Port) ->
    start(Server, Port, false).
start(Server, Port, IsGlobal) ->
    gen_server:start({local, Server}, ?MODULE, [Server, Port, IsGlobal], []).

stop(Server) ->
    gen_server:cast(Server, stop).

%% self() ! {join, false | true}
join(Server, Host, Port) ->
    gen_server:call(Server, {join, Host, Port}).

%% self() ! {put, Tag, true | false}
put_data(Server, Key, Value) ->
    UDPServer = to_udp(Server),
    ermudp:dht_put(UDPServer, Key, Value).

%% self() ! {find_value, Tag,
%%           false | {Index, #Total, Value, Elapsed_Time},
%%           false | {IP, Port}}
find_value(Server, Key) ->
    UDPServer = to_udp(Server),
    ermudp:dht_find_value(UDPServer, Key).

%% self() ! {get, Tag,
%%           false | {Index, #Total, Value, Elapsed_Time},
%%           {IP, Port}}
get_data(Server, Key, Index, {IP, Port}) ->
    UDPServer = to_udp(Server),
    ermudp:dht_index_get(UDPServer, Key, Index, IP, Port).


get_id(Server) ->
    UDPServer = to_udp(Server),
    ermudp:get_id(UDPServer).


%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([Server, Port, IsGlobal | _]) ->
    ermlibs:init(),
    ermlogger:start(),

    UDPServer = list_to_atom(atom_to_list(Server) ++ ".udp"),
    ermudp:start_link(UDPServer, Port),
    case IsGlobal of
        true ->
            ermudp:set_nat_state(UDPServer, global);
        _ ->
            ok
    end,

    F = fun() -> loop(UDPServer) end,
    spawn_link(F),

    {ok, #state{udp = UDPServer}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({join, Host, Port}, {PID, Tag}, State) ->
    %% join to dht
    F1 = fun() ->
                 io:format("join: joining dht...~n"),
                 Tag1 = ermudp:dht_find_node(State#state.udp,
                                             Host, Port),
                 receive
                     {find_node, Tag1, Nodes} ->
                         if
                             length(Nodes) > 1 ->
                                 catch PID ! {join, true};
                             true ->
                                 catch PID ! {join, false}
                         end
                 after 10000 ->
                         catch PID ! {join, false}
                 end
         end,

    %% join to dtun
    F2 = fun() ->
                 io:format("join: joining dtun...~n"),
                 Tag2 = ermudp:dtun_find_node(State#state.udp, Host, Port),
                 receive
                     {find_node, Tag2, Nodes} ->
                         Nodes0 = [X || {_, {_, IP0, Port0}} = X <- Nodes,
                                        IP0 =/= localhost, Port0 =/= 0],
                         if
                             length(Nodes0) > 0 ->
                                 ermudp:dtun_register(State#state.udp),
                                 F1();
                             true ->
                                 catch PID ! {join, false}
                         end
                 after 10000 ->
                         catch PID ! {join, false}
                 end
         end,

    %% detect nat
    F3 = fun() ->
                 io:format("join: detecting nat...~n"),
                 case ermudp:detect_nat(State#state.udp, Host, Port) of
                     false ->
                         catch PID ! {join, false};
                     Tag3 ->
                         receive
                             {detect_nat, Tag3, error} ->
                                 io:format("join: detecting nat, error~n"),
                                 catch PID ! {join, false};
                             {detect_nat, Tag3, _} ->
                                 F2()
                         after 10000 ->
                                 io:format("join: detecting nat, timed out~n"),
                                 catch PID ! {join, false}
                         end
                 end
         end,

    % nat -> dtun -> dht
    case ermudp:get_nat_state(State#state.udp) of
        undefined ->
            spawn_link(F3);
        global ->
            spawn_link(F2);
        nat ->
            spawn_link(F2);
        cone ->
            spawn_link(F2);
        _ ->
            catch PID ! {join, false}
    end,

    Reply = Tag,
    {reply, Reply, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(stop, State) ->
    {stop, terminated, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------


loop(UDPServer) ->
    Rnd = random:uniform(60),
    ermlibs:sleep((120 + Rnd) * 1000),

    %% detect types of NAT
    case ermudp:get_nat_state(UDPServer) of
        nat ->
            Peers = list_to_atom(atom_to_list(UDPServer) ++ ".peers"),
            Nodes0 = ermpeers:get_global(Peers),
            Nodes = lists:reverse(Nodes0),
            if
                length(Nodes) > 1 ->
                    [{IP1, Port1}, {IP2, Port2} | _] = Nodes,
                    Tag0 = ermudp:detect_nat_type(UDPServer,
                                                  IP1, Port1, IP2, Port2),
                    receive
                        {detect_nat_type, Tag0, _} ->
                            ok
                    after 30 * 1000 ->
                            ok
                    end;
                true ->
                    ok
            end;
        _ ->
            ok
    end,

    %% expire DBs
    ermudp:expire(UDPServer),

    %% register to dtun
    Tag1 = ermudp:dtun_register(UDPServer),
    receive
        {register, Tag1, _} ->
            ok
    after 30 * 1000 ->
            ok
    end,
    
    %% maintain routing table
    Tag2 = ermudp:dht_find_node(UDPServer, ermudp:get_id(UDPServer)),
    receive
        {find_node, Tag2, _} ->
            ok
    after 30 * 1000 ->
            ok
    end,

    loop(UDPServer).


to_udp(Server) ->
    list_to_atom(atom_to_list(Server) ++ ".udp").
