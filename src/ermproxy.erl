-module(ermproxy).

-export([proxy_register/3]).
-export([dispatcher/6]).
-export([send_dgram/4]).
-export([put_data/7]).
-export([init/2]).
-export([set_server/3]).
-export([find_value/5]).

-record(proxy_state, {id, proxy_server = {}, db_nonce}).


set_server(State, IP, Port) ->
    State#proxy_state{proxy_server = {IP, Port}}.

send_msg(Socket, Host, Port, Msg) ->
    gen_udp:send(Socket, Host, Port, term_to_binary({proxy, Msg})).


%% p1 -> p2: dgram, ID(p1), Dest, Data
send_dgram(Socket, State, ID, Data) ->
    case State#proxy_state.proxy_server of
        {IP, Port} ->
            Msg = {dgram, State#proxy_state.id, ID, Data},
            send_msg(Socket, IP, Port, Msg);
        _ ->
            ok
    end.


%% p1 -> p2: find_value, ID(p1), ID, Nonce
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
            ok
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


%% p1 -> p2: register, ID(p1)
proxy_register(UDPServer, State, Socket) ->
    F = fun() ->
                Tag = ermudp:dtun_find_node(UDPServer, 
                                            State#proxy_state.id),

                receive
                    {find_node, Tag, Nodes0} ->
                        Nodes = [N || N = {_, {_, _, P}} <- Nodes0,
                                      P =/= 0],

                        case Nodes of
                            [{_, {_, IP, Port}} | _] ->
                                Msg = {register, State#proxy_state.id},
                                send_msg(Socket, IP, Port, Msg),
                                ermudp:proxy_set_server(UDPServer, IP, Port);
                            _ ->
                                ok
                        end
                after 30000 ->
                        ok
                end
        end,
    
    spawn_link(F).


dispatcher(UDPServer, State, _Socket, _IP, _Port, {register, ID}) ->
    %% io:format("recv register: Port = ~p~n", [Port]),
    F = fun() ->
                ermudp:dtun_register(UDPServer, ID)
        end,
    
    spawn_link(F),
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


init(UDPServer, ID) ->
    TID = list_to_atom(atom_to_list(UDPServer) ++ ".ermproxy.nonce"),
    #proxy_state{id = ID, db_nonce = ets:new(TID, [public])}.
