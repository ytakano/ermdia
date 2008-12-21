-module(ermdht).

-export([init/3, stop/1]).
-export([dispatcher/6]).
-export([ping/8]).
-export([find_value/6, find_node/6, find_node/7]).
-export([print_rttable/1]).

-define(MAX_FINDNODE, 6).
-define(MAX_QUERY, 3).
-define(MAX_STORE, 3).


-record(dht_state, {id, table, timed_out, dict_nonce, peers, db}).


send_msg(Socket, Host, Port, Msg) ->
    gen_udp:send(Socket, Host, Port, term_to_binary({dht, Msg})).


init(UDPServer, Peers, ID) ->
    Table = list_to_atom(atom_to_list(UDPServer) ++ ".dht"),
    TID1  = list_to_atom(atom_to_list(UDPServer) ++ ".dht.tout"),
    TID2  = list_to_atom(atom_to_list(UDPServer) ++ ".dht.nonce"),
    TID3  = list_to_atom(atom_to_list(UDPServer) ++ ".dht.db"),

    ermrttable:start_link(Table, ID),

    #dht_state{id         = ID,
               table      = Table,
               peers      = Peers,
               timed_out  = ets:new(TID1, [public]),
               dict_nonce = ets:new(TID2, [public]),
               db         = ets:new(TID3, [public])}.


stop(State) ->
    ermrttable:stop(State#dht_state.table).


%% 1. p1 -> p2: ping, ID(p1), Nonce
%% 2. p1 <- p2: ping_reply, ID(P2), Nonce
ping(UDPServer, Socket, State, ID, Host, Port, PID, Tag) ->
    F1 = fun() ->
                 Nonce = ermlibs:gen_nonce(),
                 ets:insert(State#dht_state.dict_nonce,
                            {{ping, Nonce}, PID, Tag}),

                 Msg = {ping, State#dht_state.id, Nonce},
                 send_msg(Socket, Host, Port, Msg)
         end,

    F2 = fun() ->
                 Tag = ermudp:dtun_request(UDPServer, ID),
                 receive
                     {request, Tag, true} ->
                         F1()
                 after 1000 ->
                         ok
                 end
         end,

    case ermpeers:is_contacted(State#dht_state.peers, Host, Port) of
        true ->
            F1();
        _ ->
            spawn_link(F2)
    end.


find_value(UDPServer, Socket, State, ID, PID, Tag) ->
    case ets:lookup(State#dht_state.db, ID) of
        [] ->
            find_node_value(UDPServer, Socket, State, ID, PID, Tag, true);
        [{ID, IP, Port, Sec} | _] ->
            catch PID ! {find_value, Tag, {IP, Port, Sec}}
    end.


find_node(UDPServer, Socket, State, Host, Port, PID, Tag) ->
    Nonce = ermlibs:gen_nonce(),

    ID = State#dht_state.id,
    F = fun() ->
                put(State#dht_state.id ,true),
                find_node(UDPServer, Socket, State, ID, Nonce, [], 0, false,
                          PID, Tag)
        end,
    PID1 = spawn_link(F),
    
    ets:insert(State#dht_state.dict_nonce, {{find_node, Nonce, ID}, PID1}),

    Msg = {find_node, false, State#dht_state.id, ID, Nonce},
    send_msg(Socket, Host, Port, Msg).


find_node(UDPServer, Socket, State, ID, PID, Tag) ->
    find_node_value(UDPServer, Socket, State, ID, PID, Tag, false).


%% 1. p1 -> p2: find_node, true | false, ID(p1), DestID, Nonce
%% 2. p1 <- p2: find_node_reply, true | false, ID(p2), DestID,
%%              Nonce, Nodes | Value
%%
%% 2-1. p1 -------> p3: request (ermdtun)
%% 2-1.       p2 <- p3: request_by (ermdtun)
%% 2-2. p1 <- p2      : request_reply (ermdtun)
find_node_value(UDPServer, Socket, State, ID, PID, Tag, IsValue) ->
    Nonce = ermlibs:gen_nonce(),

    F = fun() ->
                put(State#dht_state.id ,true),
                find_node(UDPServer, Socket, State, ID, Nonce, [], 1, IsValue,
                          PID, Tag)
        end,
    PID1 = spawn_link(F),

    ets:insert(State#dht_state.dict_nonce, {{find_node, Nonce, ID}, PID1}),

    Nodes = ermrttable:lookup(State#dht_state.table, ID, ?MAX_FINDNODE),

    catch PID1 ! {find_node, false, Nodes, State#dht_state.id, localhost, 0}.


find_node(UDPServer, Socket, State, ID, Nonce, Nodes, N, IsValue, PID, Tag) ->
    MyID     = State#dht_state.id,
    TimedOut = State#dht_state.timed_out,
    Table    = State#dht_state.table,
    DicNonce = State#dht_state.dict_nonce,
    Peers    = State#dht_state.peers,

    receive
        {find_node, true, Value, _FromID, IP, Port} ->
            case IsValue of
                true ->
                    ets:delete(DicNonce, {find_node, ID, Nonce}),
                    catch PID ! {find_value, Tag, Value, {IP, Port}};
                _ ->
                    find_node(UDPServer, Socket, State, ID, Nonce, Nodes, N - 1,
                              IsValue, PID, Tag)
            end;
        {find_node, false, Nodes0, FromID, IP, Port} ->
            Nodes2 = try
                         Nodes1 = ermdtun:filter_nodes(MyID, ID, FromID, IP,
                                                       Port, TimedOut, Nodes0),
                         lists:sort(Nodes1)
                     catch
                         _:_ ->
                             []
                     end,
            Nodes3 = ermdtun:merge_nodes(Nodes, Nodes2, ?MAX_FINDNODE),

            PID1 = get(FromID),
            if
                is_port(PID1) ->
                    catch PID1 ! terminate;
                true ->
                    ok
            end,

            put(FromID, true),

            N0 = send_find_node(Socket, UDPServer, Peers, MyID, ID, Nonce,
                                Nodes3, IsValue, ?MAX_QUERY, N - 1),

            if
                N0 > 0 ->
                    find_node(UDPServer, Socket, State, ID, Nonce, Nodes3, N0,
                              IsValue, PID, Tag);
                true ->
                    ets:delete(DicNonce, {find_node, ID, Nonce}),

                    case IsValue of
                        true ->
                            catch PID ! {find_value, Tag, false, false};
                        _ ->
                            catch PID ! {find_node, Tag, Nodes3}
                    end
            end;
        {request, FromID, IP, Port} ->
            Msg = {find_node, IsValue, MyID, ID, Nonce},
            send_msg(Socket, IP, Port, Msg),

            PID0 = self(),
            F = fun() ->
                        receive
                            terminate ->
                                ok
                        after 1000 ->
                                catch PID0 ! {timeout, FromID, IP, Port}
                        end
                end,
    
            PID1 = spawn_link(F),
            put(FromID, PID1),

            find_node(UDPServer, Socket, State, ID, Nonce, Nodes, N,
                      IsValue, PID, Tag);
        {timeout, ToID, _IP, _Port} ->
            ets:insert(TimedOut, {ToID, ermlibs:get_sec()}),
            ermrttable:remove(Table, ToID),

            put(ToID, true),
            Nodes1 = [X || {_, {ID0, _, _}} = X <- Nodes, ID0 =/= ToID],
            N0 = send_find_node(Socket, UDPServer, Peers, MyID, ID, Nonce,
                                Nodes1, IsValue, ?MAX_QUERY, N - 1),

            if
                N0 > 0 ->
                    find_node(UDPServer, Socket, State, ID, Nonce, Nodes1, N0,
                              IsValue, PID, Tag);
                true ->
                    ets:delete(DicNonce, {find_node, ID, Nonce}),
                    case IsValue of
                        true ->
                            catch PID ! {find_value, Tag, false, false};
                        _ ->
                            catch PID ! {find_node, Tag, Nodes1}
                    end
            end
    after 30000 ->
            ok
    end.


send_find_node(_, _, _, _, _, _, _, _, Max, N)
  when Max =:= N ->
    N;
send_find_node(_, _, _, _, _, _, [], _, _, N) ->
    N;
send_find_node(Socket, UDPServer, Peers, MyID, ID, Nonce, [Node | T],
               IsValue, Max, N) ->
    {_, {ID0, IP, Port}} = Node,

    case get(ID0) of
        undefined ->
            %% io:format("send: ID0 = ~p~n", [ID0]),
            case ermpeers:is_contacted(Peers, IP, Port) of
                true ->
                    Msg = {find_node, IsValue, MyID, ID, Nonce},
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
                    send_find_node(Socket, UDPServer, Peers, MyID, ID, Nonce,
                                   T, IsValue, Max, N + 1);
                _ ->
                    PID = self(),
                    F = fun() ->
                                Tag = ermudp:dtun_request(UDPServer, ID0),
                                receive
                                    {request, Tag, true} ->
                                        PID ! {request, ID0, IP, Port};
                                    {request, Tag, false} ->
                                        PID ! {timeout, ID0, IP, Port}
                                after 2000 ->
                                        catch PID ! {timeout, ID0, IP, Port}
                                end
                        end,

                    spawn_link(F),
                    put(ID0, true),

                    send_find_node(Socket, UDPServer, Peers, MyID, ID, Nonce,
                                   T, IsValue, Max, N + 1)
            end;
        _ ->
            send_find_node(Socket, UDPServer, Peers, MyID, ID, Nonce, T,
                           IsValue, Max, N)
    end.


dispatcher(_UDPServer, State, Socket, IP, Port, {ping, _FromID, Nonce}) ->
    Msg = {ping_reply, State#dht_state.id, Nonce},
    send_msg(Socket, IP, Port, Msg),
    State;
dispatcher(_UDPServer, State, _Socket, IP, Port,
           {ping_reply, FromID, Nonce}) ->
    case ets:lookup(State#dht_state.dict_nonce, {ping, Nonce}) of
        [{{ping, Nonce}, PID, Tag} | _] ->
            ets:delete(State#dht_state.dict_nonce, {ping, Nonce}),
            catch PID ! {ping_reply, Tag, FromID, IP, Port};
        _ ->
            ok
    end,
    State;
dispatcher(UDPServer, State, _Socket, IP, Port,
           {find_node_reply, IsValue, FromID, Dest, Nonce, Value}) ->
    case ets:lookup(State#dht_state.dict_nonce, {find_node, Nonce, Dest}) of
        [{{find_node, Nonce, Dest}, PID} | _] ->
            catch PID ! {find_node, IsValue, Value, FromID, IP, Port};
        _ ->
            ok
    end,

    add2rttable(UDPServer, State#dht_state.table, FromID, IP, Port),

    State;
dispatcher(UDPServer, State, Socket, IP, Port,
           {find_node, IsValue, FromID, Dest, Nonce}) ->
    F = fun() ->
                Nodes = ermrttable:lookup(State#dht_state.table, Dest,
                                          ?MAX_FINDNODE),
                Msg = {find_node_reply, false, State#dht_state.id, Dest,
                       Nonce, Nodes},
                
                send_msg(Socket, IP, Port, Msg)
        end,
    
    case IsValue of
        true ->
            case ets:lookup(State#dht_state.db, Dest) of
                [{Dest, IP1, Port1, Sec} | _] ->
                    Msg = {find_node_reply, true, State#dht_state.id, Dest,
                           Nonce, {IP1, Port1, Sec}},
                    send_msg(Socket, IP, Port, Msg);
                _ ->
                    F()
            end;
        false ->
            F()
    end,

    add2rttable(UDPServer, State#dht_state.table, FromID, IP, Port),

    State;
dispatcher(_UDPServer, State, _Socket, _IP, _Port, _Msg) ->
    State.


add2rttable(UDPServer, Table, ID, IP, Port) ->
    F1 = fun(PingID) ->
                 %% send_ping
                 Tag = ermudp:dht_ping(UDPServer, ID, IP, Port),
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
    ermrttable:print_state(State#dht_state.table).
