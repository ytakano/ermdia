-module(ermdgram).

-define(MAX_QUEUE, 1024 * 4).
-define(DB_TTL, 240).
-define(FORWARD_TTL, 300).

-export([init/2, send_dgram/6, set_recv_func/2, dispatcher/5, expire/1,
         add_forward/4, send_ping/7]).

-record(dgram_state, {id, requested, queue, recv, forward, db_nonce}).


add_forward(State, ID, IP, Port) ->
    ets:insert(State#dgram_state.forward, {ID, IP, Port, ermlibs:get_sec()}).


send_msg(Socket, Host, Port, Msg) ->
    gen_udp:send(Socket, Host, Port, term_to_binary({dgram, Msg})).


set_recv_func(State, Func) ->
    State#dgram_state{recv = Func}.


%% p1 -> p2: ping, ID(p1), ID(p2), Nonce
%% p1 <- p2: ping_reply, ID(p2), ID(p1), Nonce
send_ping(Socket, State, ID, IP, Port, PID, Tag) ->
    Nonce = ermlibs:gen_nonce(),
    Msg = {ping, State#dgram_state.id, ID, Nonce},

    F = fun() ->
                receive
                    terminate ->
                        ok
                after 30000 ->
                        ets:delete(State#dgram_state.db_nonce, Nonce),
                        catch PID ! {ping, Tag, false}
                end
        end,
    
    PID0 = spawn_link(F),
    
    ets:insert(State#dgram_state.db_nonce, {Nonce, PID, Tag, PID0}),

    send_msg(Socket, IP, Port,Msg).


%% see ermdtun.erl
%% 0.1. p1 -------> p3: ping, ID(p1), Nonce0
%% 0.2. p1 -> p2      : request, ID(p1), ID(p3), Nonce
%% 0.3.       p2 -> p3: request_by, ID(p2), IP(p1), Port(p1), Nonce
%% 0.4. p1 <------- p3: request_reply, ID(p3), Nonce
%%
%% 1. p1 -> p2: msg, ID(p1), ID(p2), Msg
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
            Msg = {msg, Src, ID, Dgram},
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
    S4 = list_to_atom(atom_to_list(UDPServer) ++ ".dgram.db_nonce"),

    #dgram_state{id        = ID,
                 requested = ets:new(S1, [public]),
                 queue     = ets:new(S2, [public]),
                 forward   = ets:new(S3, [public]),
                 db_nonce  = ets:new(S4, [public]),
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
    Msg = {msg, Src, ID, Dgram},
    send_msg(Socket, IP, Port, Msg),
    send_queue(Socket, State, ID, IP, Port, T);
send_queue(_, _, _, _, _, []) ->
    ok.


recv_func(_ID, _Dgram) ->
    ok.


dispatcher(State, Socket, IP, Port, {msg, FromID, DestID, Dgram} = Msg) ->
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
dispatcher(State, Socket, IP, Port, {ping, FromID, ID, Nonce})
  when ID =:= State#dgram_state.id ->
    Msg = {ping_reply, ID, FromID, Nonce},
    send_msg(Socket, IP, Port, Msg),

    ets:insert(State#dgram_state.requested,
               {FromID, IP, Port, ermlibs:get_sec()}),

    State;
dispatcher(State, _Socket, IP, Port, {ping_reply, FromID, ID, Nonce})
  when ID =:= State#dgram_state.id ->
    case ets:lookup(State#dgram_state.db_nonce, Nonce) of
        [{Nonce, PID, Tag, PID0} | _] ->
            PID0 ! terminate,
            catch PID ! {ping, Tag, {ID, IP, Port}},
            ets:delete(State#dgram_state.db_nonce, Nonce),

            ets:insert(State#dgram_state.requested,
                       {FromID, IP, Port, ermlibs:get_sec()});
        _ ->
            ok
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
