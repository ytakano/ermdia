-module(ermdgram).

-define(MAX_QUEUE, 1024 * 4).
-define(DB_TTL, 240).
-define(FORWARD_TTL, 300).

-export([init/2, send_dgram/6, set_recv_func/2, dispatcher/5, expire/1,
         add_forward/4]).

-record(dgram_state, {id, requested, queue, recv, forward}).


add_forward(State, ID, IP, Port) ->
    ets:insert(State#dgram_state.forward, {ID, IP, Port, ermlibs:get_sec()}).


send_msg(Socket, Host, Port, Msg) ->
    gen_udp:send(Socket, Host, Port, term_to_binary({dgram, Msg})).


set_recv_func(State, Func) ->
    State#dgram_state{recv = Func}.


send_dgram(UDPServer, Socket, State, ID, Src, Dgram) ->
    F = fun() ->
                Tag = ermudp:dtun_request(UDPServer, ID),
                receive
                    {request, Tag, {IP, Port}} ->
                        ets:insert(State#dgram_state.requested,
                                   {ID, IP, Port, ermlibs:get_sec()}),
                        send_queue(Socket, State, ID, IP, Port);
                    {request, Tag, false} ->
                        ets:delete(State#dgram_state.requested, ID)
                after 3000 ->
                        ets:delete(State#dgram_state.requested, ID)
                end
        end,

    case ets:lookup(State#dgram_state.requested, ID) of
        [{ID, IP, Port, _Sec} | _] ->
            Msg = {Src, ID, Dgram},
            send_msg(Socket, IP, Port, Msg);
        _ ->
            Queue = case ets:lookup(State#dgram_state.queue, ID) of
                        [{ID, Q} | _] ->
                            if
                                length(Q) < ?MAX_QUEUE ->
                                    Q0 = lists:reverse(Q),
                                    Q1 = [{Src, Dgram} | Q0],
                                    lists:reverse(Q1);
                                true ->
                                    Q
                            end;
                        _ ->
                            [{Src, Dgram}]
                    end,
            ets:insert(State#dgram_state.queue, {ID, Queue}),
            spawn_link(F)
    end.


init(UDPServer, ID) ->
    S1 = list_to_atom(atom_to_list(UDPServer) ++ ".dgram"),
    S2 = list_to_atom(atom_to_list(UDPServer) ++ ".dgram.queue"),
    S3 = list_to_atom(atom_to_list(UDPServer) ++ ".dgram.forward"),

    #dgram_state{id        = ID,
                 requested = ets:new(S1, [public]),
                 queue     = ets:new(S2, [public]),
                 forward   = ets:new(S3, [public]),
                 recv      = fun recv_func/2}.


send_queue(Socket, State, ID, IP, Port) ->
    case ets:lookup(State#dgram_state.queue, ID) of
        [{ID, Queue} | _] ->
            ets:delete(State#dgram_state.queue, ID),
            send_queue(Socket, State, ID, IP, Port, Queue);
        _ ->
            ok
    end.

send_queue(Socket, State, ID, IP, Port, [{Src, Dgram} | T]) ->
    Msg = {Src, ID, Dgram},
    send_msg(Socket, IP, Port, Msg),
    send_queue(Socket, State, ID, IP, Port, T);
send_queue(_, _, _, _, _, []) ->
    ok.


recv_func(_ID, _Dgram) ->
    ok.


dispatcher(State, Socket, IP, Port, {FromID, DestID, Dgram} = Msg) ->
    ets:insert(State#dgram_state.requested,
               {FromID, IP, Port, ermlibs:get_sec()}),

    MyID = State#dgram_state.id,
    case DestID of
        MyID ->
            Recv = State#dgram_state.recv,
            Recv(FromID, Dgram);
        _ ->
            case ets:lookup(State#dgram_state.forward, DestID) of
                [{DestID, DestIP, DestPort, _} | _] ->
                    send_msg(Socket, DestIP, DestPort, Msg);
                _ ->
                    ok
            end
    end,
    State;
dispatcher(State, _, _, _, _) ->
    State.


expire(State) ->
    F = fun() ->
                expire_requested(State),
                expire_forward(State)
        end,
    spawn_link(F).


expire_forward(State) ->
    Dict = State#dgram_state.forward,
    expire_forward(ets:first(Dict), Dict, ermlibs:get_sec()).
expire_forward('$end_of_table', _, _) ->
    ok;
expire_forward(Key, Dict, Now) ->
    Next = ets:next(Dict, Key),
    
    case ets:lookup(Dict, Key) of
        [{_, _, _, Sec} | _] ->
            if
                Now - Sec > ?FORWARD_TTL ->
                    ets:delete(Dict, Key);
                true ->
                    ok
            end;
        _ ->
            ok
    end,
    
    expire_forward(Next, Dict, Now).


expire_requested(State) ->
    Dict = State#dgram_state.requested,
    expire_requested(ets:first(Dict), Dict, ermlibs:get_sec()).
expire_requested(Key, _, _)
  when Key =:= '$end_of_table' ->
    ok;
expire_requested(Key, Dict, Now) ->
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
    
    expire_requested(Next, Dict, Now).
