%%%-------------------------------------------------------------------
%%% File    : ermudp.erl
%%% Author  : Yuuki Takano <ytakano@jaist.ac.jp>
%%% Description : 
%%%
%%% Created : 17 Dec 2008 by Yuuki Takano <ytakano@jaist.ac.jp>
%%%-------------------------------------------------------------------
-module(ermudp).

-behaviour(gen_server).

%% API
-export([start_link/2, stop/1]).
-export([detect_nat/3, detect_nat_type/5]).

-export([dtun_find_node/3, dtun_find_node/2, dtun_find_value/2]).
-export([dtun_ping/3]).
-export([dtun_register/1, dtun_request/2, dtun_expiration/1]).

-export([get_id/1]).
-export([print_state/1]).
-export([run_test1/0, run_test2/0, stop_test2/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {server, socket, id, nat_state = undefined,
                dict_nonce, dtun_state, peers}).


%% 1. Socket1 ---------> {Host, Port}: echo, Nonce1
%% 2. Socket1 <--------- {Host, Port}: echo_reply,
%%                                     Host(Socket1), Port(Socket1),Nonce1
%% 3. Socket1 ---------> {Host, Port}: echo_redirect, Port(Socket2), Nonce2
%% 4.         Socket2 <- {Host, Port}: echo_redirect_reply,
%%                                     Host(Socket1), Port(Socket1), Nonce2
%% 5. recv echo_redirect_reply ? yes -> global, timeout -> nat
%%
%%       (send echo)   (recv reply)     (recv reply)
%% undefined -> nat_echo -> echo_redirect -> global
%%           <-                           -> nat
%%        (timeout)                     (timeout)
-define(STATE_NAT_ECHO, nat_echo).
-define(STATE_NAT_ECHO_REDIRECT, nat_echo_redirect).
-define(STATE_GLOBAL, global).
-define(STATE_NAT, nat).


%% 1. Socket -> {Host1, Port1}: echo, Nonce1
%% 2. Socket ----------------> {Host2, Port2}: echo, Nonce2
%% 3. Socket <- {Host1, Port1}: echo_reply,
%%                              Host(Socket), P1 = Port(Socket), Nonce1
%% 4. Socket <---------------- {Host2, Port2}: echo_reply,
%%                                             Host(Socket), P2 = Port(Socket),
%%                                             Nonce2
%% 5. P1 == P2 -> cone, P1 != P2 -> symmetric
%%
%%   (send echo)     (recv reply)      (recv reply)
%% nat -> nat_type_echo1 -> nat_type_echo2 -> symmetric (P1 != P2)
%%     <-                                  -> cone      (P1 == P2)
%%   (timeout)
-define(STATE_NAT_TYPE_ECHO1, nat_type_echo1).
-define(STATE_NAT_TYPE_ECHO2, nat_type_echo2).
-define(STATE_SYMMETRIC, symmetric).
-define(STATE_CONE, cone).


%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Server, Port) ->
    gen_server:start_link({local, Server}, ?MODULE, [Server, Port], []).

stop(Server) ->
    gen_server:cast(Server, stop).

detect_nat(Server, Host, Port) ->
    gen_server:call(Server, {detect_nat, Host, Port}).

detect_nat_type(Server, Host1, Port1, Host2, Port2) ->
    gen_server:call(Server, {detect_nat_type, Host1, Port1, Host2, Port2}).

set_nat_state(Server, NATState) ->
    gen_server:call(Server, {set_nat_state, NATState}).

get_id(Server) ->
    gen_server:call(Server, get_id).

print_state(Server) ->
    gen_server:call(Server, print_state).


dtun_find_node(Server, Host, Port) ->
    gen_server:call(Server, {dtun_find_node, Host, Port}).
    
dtun_find_node(Server, ID) ->
    gen_server:call(Server, {dtun_find_node, ID}).

dtun_find_value(Server, ID) ->
    gen_server:call(Server, {dtun_find_value, ID}).

dtun_ping(Server, Host, Port) ->
    gen_server:call(Server, {dtun_ping, Host, Port}).

dtun_request(Server, ID) ->
    gen_server:call(Server, {dtun_request, ID}).

dtun_register(Server) ->
    gen_server:call(Server, dtun_register).

dtun_expiration(Server) ->
    gen_server:call(Server, dtun_expiration).


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
init([Server, Port | _]) ->
    case open_udp(Port) of
        error ->
            {stop, {error, ?MODULE, ?LINE, Port, "cannot open port"}};
        Socket ->
            PeersServer = list_to_atom(atom_to_list(Server) ++ ".peers"),
            ermpeers:start_link(PeersServer),

            <<ID:160>> = crypto:rand_bytes(20),
            Dict = ets:new(Server, [public]),
            DTUNState = ermdtun:init(Server, PeersServer, ID),
            State = #state{server = Server, socket = Socket, id = ID,
                           dict_nonce = Dict, dtun_state = DTUNState,
                           peers = PeersServer},
            {ok, State}
    end.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(dtun_expiration, _From, State) ->
    ermdtun:expiration(State#state.dtun_state),

    Reply = ok,
    {reply, Reply, State};
handle_call(dtun_register, _From, State) ->
    ermdtun:register_node(State#state.server, State#state.socket,
                          State#state.dtun_state),

    Reply = ok,
    {reply, Reply, State};
handle_call({dtun_request, ID}, {PID, Tag}, State) ->
    ermdtun:request(State#state.server, State#state.socket,
                    State#state.dtun_state, ID, PID, Tag),
    Reply = Tag,
    {reply, Reply, State};
handle_call({dtun_ping, Host, Port}, {PID, Tag}, State) ->
    Reply = case State#state.nat_state of
                ?STATE_GLOBAL ->
                    ermdtun:ping(State#state.socket, State#state.dtun_state,
                                 Host, Port, PID, Tag),
                    Tag;
                _ ->
                    error
            end,
    {reply, Reply, State};
handle_call({dtun_find_node, Host, Port}, {PID, Tag}, State) ->
    NAT = case State#state.nat_state of
              ?STATE_GLOBAL ->
                  global;
              _ ->
                  nat
          end,

    ermdtun:find_node(State#state.socket, State#state.dtun_state,
                      NAT, Host, Port, PID, Tag),

    {reply, Tag, State};
handle_call({dtun_find_node, ID}, {PID, Tag}, State) ->
    NAT = case State#state.nat_state of
              ?STATE_GLOBAL ->
                  global;
              _ ->
                  nat
          end,

    ermdtun:find_node(State#state.socket, State#state.dtun_state,
                      NAT, ID, PID, Tag),

    {reply, Tag, State};
handle_call({dtun_find_value, ID}, {PID, Tag}, State) ->
    NAT = case State#state.nat_state of
              ?STATE_GLOBAL ->
                  global;
              _ ->
                  nat
          end,

    ermdtun:find_value(State#state.socket, State#state.dtun_state,
                       NAT, ID, PID, Tag),

    {reply, Tag, State};
handle_call({detect_nat_type, Host1, Port1, Host2, Port2}, {PID, Tag}, State) ->
    case State#state.nat_state of
        ?STATE_NAT ->
            Nonce1 = ermlibs:gen_nonce(),
            
            gen_udp:send(State#state.socket, Host1, Port1,
                         term_to_binary({echo, Nonce1})),

            F= fun() ->
                       receive
                           terminate ->
                               ok
                       after 1000 ->
                               ets:delete(State#state.dict_nonce, Nonce1),
                               set_nat_state(State#state.server, ?STATE_NAT),
                               catch PID ! {detect_nat_type, Tag, error}
                       end
               end,

            PID2 = spawn_link(F),

            ets:insert(State#state.dict_nonce,
                       {Nonce1, ?STATE_NAT_TYPE_ECHO1,
                        PID, Tag, PID2, Host2, Port2}),

            {reply, Tag, State#state{nat_state = ?STATE_NAT_TYPE_ECHO1}};
        _ ->
            {reply, false, State}
    end;
handle_call({detect_nat, Host, Port}, {PID, Tag}, State) ->
    case State#state.nat_state of
        undefined ->
            Nonce1 = ermlibs:gen_nonce(),

            gen_udp:send(State#state.socket, Host, Port,
                         term_to_binary({echo, Nonce1})),

            F = fun() ->
                        receive
                            terminate ->
                                ok
                        after 1000 ->
                                %% timed out
                                ets:delete(State#state.dict_nonce, Nonce1),

                                set_nat_state(State#state.server, undefined),
                                catch PID ! {detect_nat, Tag, error}
                        end
                end,

            PID2 = spawn_link(F),

            ets:insert(State#state.dict_nonce, 
                       {Nonce1, ?STATE_NAT_ECHO, PID, Tag, PID2}),

            NewState = State#state{nat_state = ?STATE_NAT_ECHO},

            Reply = Tag,
            {reply, Reply, NewState};
        _ ->
            {reply, false, State}
    end;
handle_call({set_nat_state, NATState}, _From, State) ->
    Reply = ok,
    {reply, Reply, State#state{nat_state = NATState}};
handle_call(get_id, _From, State) ->
    Reply = State#state.id,
    {reply, Reply, State};
handle_call(print_state, _From, State) ->
    case inet:sockname(State#state.socket) of
        {ok, {IP, Port}} ->
            io:format("bind: IP = ~p, Port = ~p~n", [IP, Port]);
        _ ->
            ok
    end,

    io:format("nat_state: ~p~n~n", [State#state.nat_state]),

    ermdtun:print_rttable(State#state.dtun_state),

    Reply = ok,
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
    ermdtun:stop(State#state.dtun_state),
    ermpeers:stop(State#state.peers),
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info({udp, Socket, IP, Port, Bin}, State) ->
    Term = binary_to_term(Bin),
    io:format("recv udp: ID = ~p~n          Term = ~p~n",
              [State#state.id, Term]),

    ermpeers:add_contacted(State#state.peers, IP, Port),

    {noreply, dispatcher(State, Socket, IP, Port, Term)};
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

open_udp(Port) ->
    try
        {ok, Socket} = gen_udp:open(Port, [binary]),
        Socket
    catch
        _:Why ->
            ermlogger:append({erlang:localtime(), ?MODULE, ?LINE, Why}),
            error
    end.

dispatcher(State, _, IP, Port, {echo, Nonce}) ->
    gen_udp:send(State#state.socket, IP, Port,
                 term_to_binary({echo_reply, IP, Port, Nonce})),
    State;
dispatcher(State, _, IP, Port, {echo_redirect, ToPort, Nonce}) ->
    gen_udp:send(State#state.socket, IP, ToPort,
                 term_to_binary({echo_redirect_reply, IP, Port, Nonce})),
    State;
dispatcher(State, _, IP, Port, {echo_reply, _, _, _} = Msg) ->
    case State#state.nat_state of
        ?STATE_NAT_ECHO ->
            recv_nat_echo_reply(State, IP, Port, Msg);
        ?STATE_NAT_TYPE_ECHO1->
            recv_nat_type_echo_reply1(State, Msg);
        ?STATE_NAT_TYPE_ECHO2->
            recv_nat_type_echo_reply2(State, Msg);
        _ ->
            State
    end;
dispatcher(State, Socket, _, _, {echo_redirect_reply, _, _, _} = Msg) ->
    case State#state.nat_state of
        ?STATE_NAT_ECHO_REDIRECT ->
            recv_nat_echo_redirect_reply(State, Socket, Msg);
        _ ->
            State
    end;
dispatcher(State, Socket, IP, Port, {dtun, Msg}) ->
    DTUNState = ermdtun:dispatcher(State#state.server, State#state.dtun_state,
                                   Socket, IP, Port, Msg),
    State#state{dtun_state = DTUNState};
dispatcher(State, _, _IP, _Port, _Data) ->
    State.


recv_nat_type_echo_reply1(State, {echo_reply, _, MyPort1, Nonce1}) ->
    case ets:lookup(State#state.dict_nonce, Nonce1) of
        [] ->
            State;
        [{Nonce1, ?STATE_NAT_TYPE_ECHO1, PID, Tag, PID2, Host2, Port2} | _] ->
            catch PID2 ! terminate,

            Nonce2 = ermlibs:gen_nonce(),

            gen_udp:send(State#state.socket, Host2, Port2,
                         term_to_binary({echo, Nonce2})),

            F= fun() ->
                       receive
                           terminate ->
                               ok
                       after 1000 ->
                               ets:delete(State#state.dict_nonce, Nonce2),
                               set_nat_state(State#state.server, ?STATE_NAT),
                               catch PID ! {detect_nat_type, Tag, error}
                       end
               end,

            PID3 = spawn_link(F),

            ets:delete(State#state.dict_nonce, Nonce1),
            ets:insert(State#state.dict_nonce,
                       {Nonce2, ?STATE_NAT_TYPE_ECHO2,
                        PID, Tag, PID3, MyPort1}),

            State#state{nat_state = ?STATE_NAT_TYPE_ECHO2};
        _ ->
            State
    end.


recv_nat_type_echo_reply2(State, {echo_reply, _, MyPort2, Nonce2}) ->
    case ets:lookup(State#state.dict_nonce, Nonce2) of
        [] ->
            State;
        [{Nonce2, ?STATE_NAT_TYPE_ECHO2, PID, Tag, PID3, MyPort1} | _] ->
            catch PID3 ! terminate,

            ets:delete(State#state.dict_nonce, Nonce2),
            if
                MyPort1 =:= MyPort2 ->
                    catch PID ! {detect_nat_type, Tag, cone},
                    State#state{nat_state = ?STATE_CONE};
                true ->
                    catch PID ! {detect_nat_type, Tag, symmetric},
                    State#state{nat_state = ?STATE_SYMMETRIC}
            end;
        _ ->
            State
    end.


recv_nat_echo_reply(State, FromIP, FromPort, {echo_reply, _, _, Nonce1}) ->
    case ets:lookup(State#state.dict_nonce, Nonce1) of
        [] ->
            State;
        [{Nonce1, ?STATE_NAT_ECHO, PID, Tag, PID2} | _] ->
            catch PID2 ! terminate,

            Socket2 = open_udp(0),

            case inet:sockname(Socket2) of
                {error, Why} ->
                    ermlogger:append({erlang:localtime(), ?MODULE, ?LINE, Why}),
                    gen_udp:close(Socket2),
                    ets:delete(State#state.dict_nonce, Nonce1),

                    State#state{nat_state = undefined};
                {ok, {_, Port2}} ->
                    Nonce2 = ermlibs:gen_nonce(),
                    Data = term_to_binary({echo_redirect, Port2, Nonce2}),

                    F = fun() ->
                                receive
                                    terminate ->
                                        gen_udp:close(Socket2),
                                        ok
                                after 1000 ->
                                        ets:delete(State#state.dict_nonce,
                                                   Nonce2),
                                        gen_udp:close(Socket2),
                                        set_nat_state(State#state.server,
                                                      ?STATE_NAT),
                                        catch PID ! {detect_nat, Tag, nat}
                                end
                        end,

                    %% send echo_redirect
                    gen_udp:send(State#state.socket, FromIP, FromPort, Data),

                    PID3 = spawn_link(F),

                    ets:delete(State#state.dict_nonce, Nonce1),
                    ets:insert(State#state.dict_nonce,
                               {Nonce2, ?STATE_NAT_ECHO_REDIRECT, PID, Tag,
                                PID3, Socket2}),

                    State#state{nat_state = ?STATE_NAT_ECHO_REDIRECT}
            end;
        _ ->
            State
    end.


recv_nat_echo_redirect_reply(State, Socket,
                             {echo_redirect_reply, _, _, Nonce2}) ->
    case ets:lookup(State#state.dict_nonce, Nonce2) of
        [] ->
            State;
        [{Nonce2, ?STATE_NAT_ECHO_REDIRECT, PID, Tag, PID3, Socket} | _] ->
            catch PID3 ! terminate,
            catch PID ! {detect_nat, Tag, global},

            ets:delete(State#state.dict_nonce, Nonce2),

            State#state{nat_state = ?STATE_GLOBAL};
        _ ->
            State
    end.



run_test1() ->
    start_link(test1, 10000),
    start_link(test2, 10001),

    print_state(test1),

    case detect_nat(test1, "localhost", 10001) of
        false ->
            ok;
        Ref1 ->
            receive
                %% Ret1 = error | nat | global
                {detect_nat, Ref1, Ret1} ->
                    io:format("~nRet1 = ~p~n~n", [Ret1])
            end
    end,

    print_state(test1),

    set_nat_state(test1, ?STATE_NAT),

    case detect_nat_type(test1, "localhost", 10001, "localhost", 10001) of
        false ->
            ok;
        Ref2 ->
            receive
                %% Ret2 = error | nat | global
                {detect_nat_type, Ref2, Ret2} ->
                    io:format("~nRet2 = ~p~n~n", [Ret2])
            end
    end,

    stop(test1),
    stop(test2).

    
run_test2() ->
    N = 100,

    start_link(test, 10000),
    set_nat_state(test, ?STATE_GLOBAL),

    start_nodes(N),

    dtun_register(test),
    dtun_register(test1),

    ermlibs:sleep(200),

    Tag = dtun_request(test, get_id(test1)),
    receive
        {request, Tag, Ret} ->
            io:format("request: ~p~n", [Ret])
    after 1000 ->
            io:format("request: timed out~n")
    end,

    print_state(test).

stop_test2() ->
    N = 100,

    stop(test),
    stop_nodes(N).


start_nodes(N)
  when N > 0 ->
    S0 = "test" ++ integer_to_list(N),
    S = list_to_atom(S0),
    start_link(S, 10000 + N),
    set_nat_state(S, ?STATE_GLOBAL),

    dtun_find_node(S, "localhost", 10000),

    io:format("N = ~p~n", [N]),
    receive
        {find_node, _Tag, _Nodes} ->
            ok
    end,

    start_nodes(N - 1);
start_nodes(_) ->
    ok.

stop_nodes(N)
  when N > 0 ->
    S0 = "test" ++ integer_to_list(N),
    S = list_to_atom(S0),
    stop(S),
    stop_nodes(N - 1);
stop_nodes(_) ->
    ok.
