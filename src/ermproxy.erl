-module(ermproxy).

-export([proxy_register/3]).
-export([dispatcher/6]).
-export([send_dgram/4]).
-export([put_data/7]).
-export([init/1]).
-export([set_server/3]).

-record(proxy_state, {id, proxy_server = {}}).


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
dispatcher(_, State, _, _, _, _) ->
    State.


init(ID) ->
    #proxy_state{id = ID}.
