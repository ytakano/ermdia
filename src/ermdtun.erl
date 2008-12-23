-module(ermdtun).

-define(MAX_FINDNODE, 6).
-define(MAX_QUERY, 3).
-define(MAX_REGISTER, 3).

-define(DB_TIMEDOUT_TTL, 300).
-define(DB_TTL, 240).


-export([init/3, stop/1]).
-export([dispatcher/6]).
-export([find_node/6, find_node/7, find_value/6]).
-export([register_node/5, request/6]).
-export([ping/6]).

-export([expire/1]).

-export([print_rttable/1]).

-export([filter_nodes/7, merge_nodes/3]).

-record(dtun_state, {id, table, timed_out, dict_nonce, peers, contacted}).


send_msg(Socket, Host, Port, Msg) ->
    gen_udp:send(Socket, Host, Port, term_to_binary({dtun, Msg})).


%% 1. p1 -------> p3: ping, ID(p1), Nonce0
%% 2. p1 -> p2      : request, ID(p1), ID(p3), Nonce
%% 3.       p2 -> p3: request_by, ID(p2), IP(p1), Port(p1), Nonce
%% 4. p1 <------- p3: request_reply, ID(p3), Nonce
request(UDPServer, Socket, State, ID, PID, Tag) ->
    Nonce = ermlibs:gen_nonce(),

    F = fun() ->
                try
                    Tag1 = ermudp:dtun_find_value(UDPServer, ID),
                    receive
                        {find_value, Tag1, false, false} ->
                            PID ! {request, Tag, false};
                        {find_value, Tag1, {IP, Port, _Sec},
                         {FromIP, FromPort}} ->
                            ping(Socket, State, IP, Port, self(), make_ref()),

                            Msg = {request, State#dtun_state.id,
                                   ID, Nonce},
                            send_msg(Socket, FromIP, FromPort, Msg)
                    after 30000 ->
                            ets:delete(State#dtun_state.dict_nonce,
                                       {request, Nonce}),
                            PID ! {request, Tag, false}
                    end
                catch
                    _:_ ->
                        ok
                end
        end,

    ets:insert(State#dtun_state.dict_nonce, {{request, Nonce}, PID, Tag}),
    
    spawn_link(F).


%% 1.  p1 -> p2: register, ID(p1)
register_node(UDPServer, Socket, State, PID, Tag) ->
    F = fun() ->
                Tag1 = ermudp:dtun_find_node(UDPServer, State#dtun_state.id),
                receive
                    {find_node, Tag1, Nodes} ->
                        register2nodes(Socket, State#dtun_state.id, Nodes,
                                       ?MAX_REGISTER),
                        catch PID ! {register, Tag, true}
                after 30000 ->
                        catch PID ! {register, Tag, false}
                end
        end,
    spawn_link(F).

register2nodes(_, _, _, N)
  when N < 1 ->
    ok;
register2nodes(_, _, [], _) ->
    ok;
register2nodes(Socket, MyID, [{_, {MyID, _, _}} | T],  N) ->
    register2nodes(Socket, MyID, T, N);
register2nodes(Socket, MyID, [{_, {_, IP, Port}} | T], N) ->
    Msg = {register, MyID},
    %%io:format("register2nodes: IP = ~p, Port = ~p~n", [IP, Port]),
    send_msg(Socket, IP, Port, Msg),
    register2nodes(Socket, MyID, T, N - 1).


%% 1. p1 -> p2: ping, ID(p1), Nonce
%% 2. p1 <- p2: ping_reply, ID(P2), Nonce
ping(Socket, State, Host, Port, PID, Tag) ->
    Nonce = ermlibs:gen_nonce(),
    ets:insert(State#dtun_state.dict_nonce, {{ping, Nonce}, PID, Tag}),

    Msg = {ping, State#dtun_state.id, Nonce},
    send_msg(Socket, Host, Port, Msg).


%% 1. p1 -> p2: find_node, false | true, ID(p1), global | nat, DestID, Nonce
%% 2. p1 <- P2: find_node_reply, false | true, ID(p2), DestID,
%%              Nonce, Nodes | Value
find_node(Socket, State, NAT, Host, Port, PID, Tag) ->
    Nonce = ermlibs:gen_nonce(),

    ID = State#dtun_state.id,
    F = fun() ->
                put(State#dtun_state.id ,true),
                catch find_node(Socket, State, ID, Nonce, [], 0, NAT, false,
                                PID, Tag)
        end,
    PID1 = spawn_link(F),
    
    ets:insert(State#dtun_state.dict_nonce, {{find_node, Nonce, ID}, PID1}),

    Msg = {find_node, false, State#dtun_state.id, NAT, ID, Nonce},
    send_msg(Socket, Host, Port, Msg).


find_node(Socket, State, NAT, ID, PID, Tag) ->
    find_node_value(Socket, State, NAT, ID, PID, Tag, false).


find_value(Socket, State, NAT, ID, PID, Tag) ->
    case ets:lookup(State#dtun_state.contacted, ID) of
        [] ->
            find_node_value(Socket, State, NAT, ID, PID, Tag, true);
        [{ID, IP, Port, Sec} | _] ->
            catch PID ! {find_value, Tag, {IP, Port, ermlibs:get_sec() - Sec}}
    end.


find_node_value(Socket, State, NAT, ID, PID, Tag, IsValue) ->
    Nonce = ermlibs:gen_nonce(),

    F = fun() ->
                put(State#dtun_state.id ,true),
                catch find_node(Socket, State, ID, Nonce, [], 1, NAT, IsValue,
                                PID, Tag)
        end,
    PID1 = spawn_link(F),

    ets:insert(State#dtun_state.dict_nonce, {{find_node, Nonce, ID}, PID1}),

    Nodes = ermrttable:lookup(State#dtun_state.table, ID, ?MAX_FINDNODE),

    catch PID1 ! {find_node, false, Nodes, State#dtun_state.id, localhost, 0}.


find_node(Socket, State, ID, Nonce, Nodes, N, NAT, IsValue, PID, Tag) ->
    MyID      = State#dtun_state.id,
    TimedOut  = State#dtun_state.timed_out,
    Table     = State#dtun_state.table,
    DicNonce  = State#dtun_state.dict_nonce,
    Contacted = State#dtun_state.contacted,

    receive
        {find_node, true, Value, _FromID, IP, Port} ->
            case {IsValue, Value} of
                {true, {IP0, Port0, ETime}} ->
                    if
                        ETime > ?DB_TTL ->
                            ok;
                        ETime < 0 ->
                            ok;
                        true ->
                            Sec = ermlibs:get_sec() - ETime,
                            ets:insert(Contacted, {ID, IP0, Port0, Sec})
                    end,

                    ets:delete(DicNonce, {find_node, ID, Nonce}),

                    catch PID ! {find_value, Tag, Value, {IP, Port}};
                _ ->
                    find_node(Socket, State, ID, Nonce, Nodes, N - 1,
                              NAT, IsValue, PID, Tag)
            end;
        {find_node, false, Nodes0, FromID, IP, Port} ->
            Nodes2 = try
                         Nodes1 = filter_nodes(MyID, ID, FromID, IP, Port,
                                               TimedOut, Nodes0),
                         lists:sort(Nodes1)
                     catch
                         _:_ ->
                             []
                     end,
            Nodes3 = merge_nodes(Nodes, Nodes2, ?MAX_FINDNODE),

            PID1 = get(FromID),
            if
                is_port(PID1) ->
                    catch PID1 ! terminate;
                true ->
                    ok
            end,

            put(FromID, true),
            %% io:format("send find_node: ID = ~p~n~p~n", [ID, Nodes3]),
            N0 = send_find_node(Socket, NAT, MyID, ID, Nonce, Nodes3,
                                IsValue, ?MAX_QUERY, N - 1),

            %% io:format("N0 = ~p~n", [N0]),

            if
                N0 > 0 ->
                    find_node(Socket, State, ID, Nonce, Nodes3, N0,
                              NAT, IsValue, PID, Tag);
                true ->
                    ets:delete(DicNonce, {find_node, ID, Nonce}),
                    case IsValue of
                        true ->
                            catch PID ! {find_value, Tag, false, false};
                        _ ->
                            catch PID ! {find_node, Tag, Nodes3}
                    end
            end;
        {timeout, ToID, _IP, _Port} ->
            %% io:format("timeout: MyID = ~p, ToID = ~p~n", [MyID, ToID]),
            ets:insert(TimedOut, {ToID, ermlibs:get_sec()}),
            ermrttable:remove(Table, ToID),

            put(ToID, true),
            Nodes1 = [X || {_, {ID0, _, _}} = X <- Nodes, ID0 =/= ToID],
            N0 = send_find_node(Socket, NAT, MyID, ID, Nonce, Nodes1,
                                IsValue, ?MAX_QUERY, N - 1),

            if
                N0 > 0 ->
                    find_node(Socket, State, ID, Nonce, Nodes1, N0,
                              NAT, IsValue, PID, Tag);
                true ->
                    ets:delete(DicNonce, {find_node, ID, Nonce}),
                    case IsValue of
                        true ->
                            catch PID ! {find_value, Tag, false, false};
                        _ ->
                            catch PID ! {find_node, Tag, Nodes}
                    end
            end
    after 10000 ->
            ets:delete(DicNonce, {find_node, ID, Nonce}),

            case IsValue of
                true ->
                    catch PID ! {find_value, Tag, false, false};
                _ ->
                    catch PID ! {find_node, Tag, Nodes}
            end,
            
            catch PID ! {find_node, Tag, Nodes}
    end.


send_find_node(_, _, _, _, _, _, _, Max, N)
  when Max =:= N ->
    N;
send_find_node(_, _, _, _, _, [], _, _, N) ->
    N;
send_find_node(Socket, NAT, MyID, ID, Nonce, [Node | T], IsValue, Max, N) ->
    {_, {ID0, IP, Port}} = Node,

    case get(ID0) of
        undefined ->
            %% io:format("send: MyID = ~p~n, ID0 = ~p~n", [MyID, ID0]),

            Msg = {find_node, IsValue, MyID, NAT, ID, Nonce},
            send_msg(Socket, IP, Port, Msg),

            PID = self(),
            F = fun() ->
                        receive
                            terminate ->
                                ok
                        after 1000 ->
                                catch PID ! {timeout, ID0, IP, Port}
                        end
                end,
    
            PID0 = spawn_link(F),

            put(ID0, PID0),
            send_find_node(Socket, NAT, MyID, ID, Nonce, T, IsValue,
                           Max, N + 1);
        _ ->
            send_find_node(Socket, NAT, MyID, ID, Nonce, T, IsValue, Max, N)
    end.


filter_nodes(MyID, Dest, ID, IP, Port, TimedOut, Nodes) ->
    filter_nodes(MyID, Dest, ID, IP, Port, TimedOut, Nodes, []).
filter_nodes(MyID, Dest, ID, IP, Port, TimedOut, [{_, {ID0, _, _}} | T] , Ret)
  when MyID =:= ID0 ->
    filter_nodes(MyID, Dest, ID, IP, Port, TimedOut, T,
                 [{0, {MyID, localhost, 0}} | Ret]);
filter_nodes(MyID, Dest, ID, IP, Port, TimedOut,
             [{_, {ID0, localhost, 0}} | T] , Ret)
  when ID =:= ID0 ->
    filter_nodes(MyID, Dest, ID, IP, Port, TimedOut, T,
                 [{Dest bxor ID0, {ID, IP, Port}} | Ret]);
filter_nodes(MyID, Dest, ID, IP, Port, TimedOut,
             [{_, {ID0, _, _} = Node} | T], Ret) ->
    case ets:lookup(TimedOut, Node) of
        [] ->
            filter_nodes(MyID, Dest, ID, IP, Port, TimedOut, T,
                         [{Dest bxor ID0, Node} | Ret]);
        _ ->
            filter_nodes(MyID, Dest, ID, IP, Port, TimedOut, T, Ret)
    end;
filter_nodes(MyID, Dest, ID, IP, Port, TimedOut, [_H | T], Ret) ->
    filter_nodes(MyID, Dest, ID, IP, Port, TimedOut, T, Ret);
filter_nodes(_, _, _, _, _, _, [], Ret) ->
    Ret.


merge_nodes(Nodes1, Nodes2, Max) ->
    merge_nodes(Nodes1, Nodes2, Max, 0, []).
merge_nodes(_, _, Max, N, Ret)
  when N =:= Max ->
    lists:reverse(Ret);
merge_nodes([Node1 | T1] = Nodes1, [Node2 | T2] = Nodes2, Max, N, Ret) ->
    {Dist1, _} = Node1,
    {Dist2, _} = Node2,
    if
        Dist1 =:= Dist2 ->
            merge_nodes(T1, T2, Max, N + 1, [Node1 | Ret]);
        Dist1 < Dist2 ->
            merge_nodes(T1, Nodes2, Max, N + 1, [Node1 | Ret]);
        true ->
            merge_nodes(Nodes1, T2, Max, N + 1, [Node2 | Ret])
    end;
merge_nodes([], [Node | T], Max, N, Ret) ->
    merge_nodes([], T, Max, N + 1, [Node | Ret]);
merge_nodes([Node | T], [], Max, N, Ret) ->
    merge_nodes(T, [], Max, N + 1, [Node | Ret]);
merge_nodes([], [], _, _, Ret) ->
    lists:reverse(Ret).


init(UDPServer, PeersServer, ID) ->
    Table = list_to_atom(atom_to_list(UDPServer) ++ ".dtun"),
    TID1  = list_to_atom(atom_to_list(UDPServer) ++ ".dtun.tout"),
    TID2  = list_to_atom(atom_to_list(UDPServer) ++ ".dtun.nonce"),
    TID3  = list_to_atom(atom_to_list(UDPServer) ++ ".dtun.contact"),

    ermrttable:start_link(Table, ID),

    #dtun_state{id         = ID,
                table      = Table,
                peers      = PeersServer,
                timed_out  = ets:new(TID1, [public]),
                dict_nonce = ets:new(TID2, [public]),
                contacted  = ets:new(TID3, [public])}.


dispatcher(_UDPServer, State, _Socket, IP, Port, {register, FromID}) ->
    MyID = State#dtun_state.id,
    case FromID of
        MyID ->
            State;
        _ ->
            case ermrttable:lookup(State#dtun_state.table,
                                   FromID, ?MAX_REGISTER * 2) of
                error ->
                    State;
                Nodes ->
                    N0 = [X || {_, {ID, _, _}} = X <- Nodes,
                               ID =:= State#dtun_state.id],
                    if
                        length(N0) > 0 ->
                            ets:insert(State#dtun_state.contacted,
                                       {FromID, IP, Port, ermlibs:get_sec()});
                        true ->
                            ok
                    end,
                    State
            end
    end;
dispatcher(_UDPServer, State, Socket, IP, Port, {ping, _FromID, Nonce}) ->
    Msg = {ping_reply, State#dtun_state.id, Nonce},
    send_msg(Socket, IP, Port, Msg),
    State;
dispatcher(_UDPServer, State, _Socket, IP, Port,
           {ping_reply, FromID, Nonce}) ->
    case ets:lookup(State#dtun_state.dict_nonce, {ping, Nonce}) of
        [{{ping, Nonce}, PID, Tag} | _] ->
            ets:delete(State#dtun_state.dict_nonce, {ping, Nonce}),
            catch PID ! {ping_reply, Tag, FromID, IP, Port};
        _ ->
            ok
    end,
    State;
dispatcher(UDPServer, State, Socket, IP, Port,
           {find_node, IsValue, FromID, NAT, Dest, Nonce}) ->
    %% io:format("recv find_node: FromID = ~p~n", [FromID]),

    F = fun() ->
                Nodes = ermrttable:lookup(State#dtun_state.table, Dest,
                                          ?MAX_FINDNODE),
                Msg = {find_node_reply, false, State#dtun_state.id, Dest,
                       Nonce, Nodes},
                
                send_msg(Socket, IP, Port, Msg)
        end,
    
    case IsValue of
        true ->
            case ets:lookup(State#dtun_state.contacted, Dest) of
                [{Dest, IP1, Port1, Sec} | _] ->
                    Msg = {find_node_reply, true, State#dtun_state.id, Dest,
                           Nonce, {IP1, Port1, ermlibs:get_sec() - Sec}},
                    send_msg(Socket, IP, Port, Msg);
                _ ->
                    F()
            end;
        false ->
            F()
    end,

    case NAT of
        global ->
            ermpeers:add_global(State#dtun_state.peers, IP, Port),
            add2rttable(UDPServer, State#dtun_state.table, FromID, IP, Port);
        _ ->
            ok
    end,

    State;
dispatcher(UDPServer, State, _Socket, IP, Port,
           {find_node_reply, IsValue, FromID, Dest, Nonce, Value}) ->
    %% io:format("recv find_node_reply: FromID = ~p~n", [FromID]),
    case ets:lookup(State#dtun_state.dict_nonce, {find_node, Nonce, Dest}) of
        [{{find_node, Nonce, Dest}, PID} | _] ->
            catch PID ! {find_node, IsValue, Value, FromID, IP, Port};
        _ ->
            ok
    end,

    add2rttable(UDPServer, State#dtun_state.table, FromID, IP, Port),

    State;
dispatcher(_UDPServer, State, Socket, IP, Port, {request, _, Dest, Nonce}) ->
    if
        Dest =:= State#dtun_state.id ->
            Msg = {request_reply, State#dtun_state.id, Nonce},
            send_msg(Socket, IP, Port, Msg);
        true ->
            case ets:lookup(State#dtun_state.contacted, Dest) of
                [] ->
                    ok;
                [{Dest, DestIP, DestPort, _} | _] ->
                    Msg = {request_by, State#dtun_state.id, IP, Port, Nonce},
                    send_msg(Socket, DestIP, DestPort, Msg)
            end
    end,
    State;
dispatcher(_UDPServer, State, Socket, _IP, _Port,
           {request_by, _, IP, Port, Nonce}) ->
    Msg = {request_reply, State#dtun_state.id, Nonce},
    send_msg(Socket, IP, Port, Msg),
    State;
dispatcher(_UDPServer, State, _Socket, _IP, _Port,
           {request_reply, _ID, Nonce}) ->
    case ets:lookup(State#dtun_state.dict_nonce, {request, Nonce}) of
        [{{request, Nonce}, PID, Tag} | _] ->
            ets:delete(State#dtun_state.dict_nonce, {request, Nonce}),
            catch PID ! {request, Tag, true};
        _ ->
            ok
    end,
    State;
dispatcher(_UDPServer, State, _Socket, _IP, _Port, _Msg) ->
    State.


add2rttable(UDPServer, Table, ID, IP, Port) ->
    F1 = fun(PingID) ->
                 %% send_ping
                 Tag = ermudp:dtun_ping(UDPServer, IP, Port),
                 receive
                     {ping_reply, Tag, PingID, FromIP, FromPort} ->
                         ermrttable:add(Table, UDPServer, PingID,
                                        FromIP, FromPort)
                 after 1000 ->
                         ermrttable:replace(Table, PingID, ID, IP, Port)
                 end,
                 
                 ermrttable:unset_ping(Table, PingID)
         end,

    F2 = fun() ->
                 ermrttable:add(Table, ID, IP, Port, F1)
         end,
    spawn_link(F2).


print_rttable(State) ->
    ermrttable:print_state(State#dtun_state.table).


stop(State) ->
    ermrttable:stop(State#dtun_state.table).


expire(State) ->
    F = fun() ->
                expire_contacted(State),
                expire_timed_out(State)
        end,
    spawn_link(F).


expire_timed_out(State) ->
    Dict = State#dtun_state.timed_out,
    expire_timed_out(ets:first(Dict), Dict, ermlibs:get_sec()).
expire_timed_out(Key, _, _)
  when Key =:= '$end_of_table' ->
    ok;
expire_timed_out(Key, Dict, Now) ->
    Next = ets:next(Dict, Key),
    
    case ets:lookup(Dict, Key) of
        [{_, Sec} | _] ->
            if
                Now - Sec > ?DB_TIMEDOUT_TTL ->
                    ets:delete(Dict, Key);
                true ->
                    ok
            end;
        _ ->
            ok
    end,
    
    expire_timed_out(Next, Dict, Now).


expire_contacted(State) ->
    Dict = State#dtun_state.contacted,
    expire_contacted(ets:first(Dict), Dict, ermlibs:get_sec()).
expire_contacted(Key, _, _)
  when Key =:= '$end_of_table' ->
    ok;
expire_contacted(Key, Dict, Now) ->
    Next = ets:next(Dict, Key),

    case ets:lookup(Dict, Key) of
        [{_, _, _, Sec} | _] ->
            if
                Now - Sec > ?DB_TTL ->
                    ets:delete(Dict, Key);
                true ->
                    ok
            end;
        _ ->
            ok
    end,
    
    expire_contacted(Next, Dict, Now).
