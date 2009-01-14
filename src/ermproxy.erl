-module(ermproxy).

-export([proxy_register/3]).
-export([dispatcher/6]).
-export([send_dgram/5]).
-export([put_data/7]).
-export([init/2]).
-export([set_server/3]).
-export([find_value/5]).
-export([set_registering/2]).
-export([expire/1]).

-define(MAX_QUEUE, 1024 * 4).
-define(DIRECT_TTL, 300).

-record(proxy_state, {id, proxy_server = {}, db_nonce, direct, queue,
                      registering = false}).


set_server(State, IP, Port) ->
    State#proxy_state{proxy_server = {IP, Port}}.


send_msg(Socket, Host, Port, Msg) ->
    gen_udp:send(Socket, Host, Port, term_to_binary({proxy, Msg})).


set_registering(State, Register) ->
    State#proxy_state{registering = Register}.


send_all_queue(Socket, State, IP, Port) ->
    ID = ets:first(State#proxy_state.queue),
    send_all_queue(Socket, State, ID, IP, Port).
send_all_queue(_, _, '$end_of_table', _, _) ->
    ok;
send_all_queue(Socket, State, ID, IP, Port) ->
    Next = ets:next(State#proxy_state.queue, ID),

    send_queue(Socket, State, ID, IP, Port),

    send_all_queue(Socket, State, Next, IP, Port).


send_queue(Socket, State, ID, IP, Port) ->
    case ets:lookup(State#proxy_state.queue, ID) of
        [{ID, Queue} | _] ->
            send_bufs(Socket, State#proxy_state.id, ID, IP, Port, Queue),
            ets:delete(State#proxy_state.queue, ID);
        _ ->
            ok
    end.

send_bufs(_Socket, _Src, _ID, _IP, _Port, []) ->
    ok;
send_bufs(Socket, Src, ID, IP, Port, [Data | T]) ->
    Msg = {dgram, Src, ID, Data},
    send_msg(Socket, IP, Port, Msg),
    send_bufs(Socket, Src, ID, IP, Port, T).


add2queue(State, ID, Data) ->
    Queue = case ets:lookup(State#proxy_state.queue, ID) of
                [{ID, Q} | _] ->
                    if
                        length(Q) < ?MAX_QUEUE ->
                            Q0 = lists:reverse(Q),
                            Q1 = [Data | Q0],
                            lists:reverse(Q1);
                        true ->
                            Q
                    end;
                _ ->
                    [Data]
            end,

    ets:insert(State#proxy_state.queue, {ID, Queue}).



%% p1 -> p2: dgram, ID(p1), Dest, Data
send_dgram(UDPServer, Socket, State, ID, Data) ->
    F = fun() ->
                ermudp:proxy_register(UDPServer)
        end,

    case ets:lookup(State#proxy_state.direct, ID) of
        [{ID, true, _} | _] ->
            case State#proxy_state.proxy_server of
                {IP, Port} ->
                    send_queue(Socket, State, ID, IP, Port);
                _ ->
                    ok
            end,

            F = fun() ->
                        ermudp:dgram_send(UDPServer, ID, Data)
                end,
            spawn_link(F);
        [{ID, false, _} | _] ->
            add2queue(State, ID, Data),

            case State#proxy_state.proxy_server of
                {IP, Port} ->
                    send_queue(Socket, State, ID, IP, Port);
                _ ->
                    spawn_link(F)
            end;
        _ ->
            add2queue(State, ID, Data),

            case State#proxy_state.proxy_server of
                {IP, Port} ->
                    F = fun() ->
                                Tag = ermudp:dgram_advertise(UDPServer,
                                                             ID, IP, Port),
                                receive
                                    {advertise, Tag, false} ->
                                        ets:insert(State#proxy_state.direct,
                                                   {ID, false,
                                                    ermlibs:get_sec()});
                                    {advertise, Tag, _} ->
                                        ets:insert(State#proxy_state.direct,
                                                   {ID, true,
                                                    ermlibs:get_sec()})
                                end
                        end,
                    
                    spawn_link(F),

                    Msg = {dgram, State#proxy_state.id, ID, Data},
                    send_msg(Socket, IP, Port, Msg);
                _ ->
                    spawn_link(F)
            end
    end.


%% p1 -> p2: find_value, ID(p1), ID, Nonce
%% p1 <- p2: find_value_reply, ID(p2), ID, Value, Nonce
find_value(Socket, State, Key, PID, Tag) ->
    case State#proxy_state.proxy_server of
        {IP, Port} ->
            Nonce = ermlibs:gen_nonce(),
            F = fun() ->
                        receive
                            terminate ->
                                ok
                        after 30000 ->
                                ets:delete(State#proxy_state.db_nonce, Nonce),
                                catch PID ! {find_value, Tag, false, false}
                        end
                end,

            Msg = {find_value, State#proxy_state.id, Key, Nonce},

            PID0 = spawn_link(F),
            ets:insert(State#proxy_state.db_nonce, {Nonce, PID, Tag, PID0}),

            send_msg(Socket, IP, Port, Msg);
        _ ->
            catch PID ! {find_value, Tag, false, false}
    end.


%% p1 -> p2: put, ID(p1), Key, Value, TTL
put_data(Socket, State, Key, Value, TTL, PID, Tag) ->
    case State#proxy_state.proxy_server of
        {IP, Port} ->
            Msg = {put, State#proxy_state.id, Key, Value, TTL},
            send_msg(Socket, IP, Port, Msg),
            catch PID ! {put, Tag, true};
        _ ->
            catch PID ! {put, Tag, false}
    end.


%% p1 -> p2: register, ID(p1), Nonce
%% p1 <- p2: register_reply, ID(p2), Nonce
proxy_register(UDPServer, State, Socket) ->
    Nonce = ermlibs:gen_nonce(),

    F0 = fun() ->
                 receive
                     terminat ->
                         ermudp:proxy_set_registering(UDPServer, false)
                 after 30000 ->
                         ermudp:proxy_set_registering(UDPServer, false),
                         ets:delete(State#proxy_state.db_nonce, Nonce)
                 end
         end,

    F1 = fun() ->
                 Tag = ermudp:dtun_find_node(UDPServer, 
                                             State#proxy_state.id),
                 
                 receive
                     {find_node, Tag, Nodes0} ->
                         Nodes = [N || N = {_, {_, _, P}} <- Nodes0,
                                       P =/= 0],

                         case Nodes of
                             [{_, {_, IP, Port}} | _] ->
                                 PID = spawn_link(F0),

                                 ets:insert(State#proxy_state.db_nonce,
                                            {Nonce, undefined, undefined, PID}),

                                 Msg = {register, State#proxy_state.id, Nonce},
                                 send_msg(Socket, IP, Port, Msg);
                             _ ->
                                 ermudp:proxy_set_registering(UDPServer, false)
                         end
                 after 30000 ->
                         ermudp:proxy_set_registering(UDPServer, false)
                 end
        end,
    
    case State#proxy_state.registering of
        false ->
            spawn_link(F1);
        _ ->
            ok
    end,
    
    State#proxy_state{registering = true}.
        


dispatcher(UDPServer, State, Socket, IP, Port, {register, ID, Nonce}) ->
    %% io:format("recv register: Port = ~p~n", [Port]),
    F = fun() ->
                ermudp:dtun_register(UDPServer, ID)
        end,
    
    spawn_link(F),

    Msg = {register_reply, State#proxy_state.id, Nonce},
    send_msg(Socket, IP, Port, Msg),

    State;
dispatcher(UDPServer, State, Socket, IP, Port,
           {register_reply, _ID, Nonce}) ->
    %% io:format("recv register reply: Port = ~p~n", [Port]),

    case ets:lookup(State#proxy_state.db_nonce, Nonce) of
        [{Nonce, _, _, PID} | _] ->
            PID ! terminate,
            ets:delete(State#proxy_state.db_nonce, Nonce),

            send_all_queue(Socket, State, IP, Port),

            F = fun() ->
                        ermudp:proxy_set_server(UDPServer, IP, Port)
                end,
            spawn_link(F);
        _ ->
            ok
    end,
    State;
dispatcher(UDPServer, State, _Socket, _IP, _Port,
           {dgram, FromID, DestID, Data}) ->
    %% io:format("recv put: Port = ~p~n", [Port]),

    F = fun() ->
                ermudp:dgram_send(UDPServer, DestID, FromID, Data)
        end,
    
    spawn_link(F),
    State;
dispatcher(UDPServer, State, _Socket, _IP, _Port,
           {put, _FromID, Key, Value, TTL})
  when is_integer(TTL) ->
    %% io:format("recv put: Port = ~p~n", [Port]),
    F = fun() ->
                ermudp:dht_put(UDPServer, Key, Value, TTL)
        end,

    spawn_link(F),
    State;
dispatcher(_UDPServer, State, _Socket, IP, Port,
           {find_value_reply, _FromID, Val, Nonce}) ->
    %% io:format("recv find_value_reply: Port = ~p~n", [Port]),
    case ets:lookup(State#proxy_state.db_nonce, Nonce) of
        [{Nonce, PID, Tag, PID0} | _] ->
            PID0 ! terminate,
            PID ! {find_value, Tag, Val, {IP, Port}};
        _ ->
            ok
    end,
    State;
dispatcher(UDPServer, State, Socket, IP, Port,
           {find_value, _FromID, Key, Nonce}) ->
    %% io:format("recv find_value: Port = ~p~n", [Port]),
    F = fun() ->
                Tag = ermudp:dht_find_value(UDPServer, Key),
                receive
                    {find_value, Tag, false, _} ->
                        Msg = {find_value_reply, State#proxy_state.id,
                               false, Nonce},
                        send_msg(Socket, IP, Port, Msg);
                    {find_value, Tag, Val, _} ->
                        Msg = {find_value_reply, State#proxy_state.id,
                               Val, Nonce},
                        send_msg(Socket, IP, Port, Msg)
                after 30000 ->
                        ok
                end
        end,

    spawn_link(F),

    State;
dispatcher(_, State, _, _, _, _) ->
    State.


expire(State) ->
    F = fun() ->
                expire_direct(State)
        end,
    spawn_link(F).


expire_direct(State) ->
    Dict = State#proxy_state.direct,
    expire_direct(ets:first(Dict), Dict, ermlibs:get_sec()).
expire_direct('$end_of_table', _, _) ->
    ok;
expire_direct(Key, Dict, Now) ->
    Next = ets:next(Dict, Key),
    
    case ets:lookup(Dict, Key) of
        [{_, _, Sec} | _] ->
            if
                Now - Sec > ?DIRECT_TTL ->
                    ets:delete(Dict, Key);
                true ->
                    ok
            end;
        _ ->
            ok
    end,
    
    expire_direct(Next, Dict, Now).


init(UDPServer, ID) ->
    TID1 = list_to_atom(atom_to_list(UDPServer) ++ ".ermproxy.nonce"),
    TID2 = list_to_atom(atom_to_list(UDPServer) ++ ".ermproxy.direct"),
    TID3 = list_to_atom(atom_to_list(UDPServer) ++ ".ermproxy.queue"),

    #proxy_state{id       = ID,
                 db_nonce = ets:new(TID1, [public]),
                 direct   = ets:new(TID2, [public]),
                 queue    = ets:new(TID3, [public])}.
