-module(ermdgram).

-define(MAX_QUEUE, 1024 * 4).

-export([init/2, send_dgram/4, set_recv_func/2, dispatcher/5]).

-record(dgram_state, {id, requested, queue, recv}).


send_msg(Socket, Host, Port, Msg) ->
    gen_udp:send(Socket, Host, Port, term_to_binary({dgram, Msg})).


set_recv_func(State, Func) ->
    State#dgram_state{recv = Func}.


send_dgram(Socket, State, ID, Dgram) ->
    F = fun() ->
                Tag = ermudp:dtun_request(),
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

    case ets:lookup(requested, ID) of
        [{ID, IP, Port, _Sec} | _] ->
            Msg = {State#dgram_state.id, Dgram},
            send_msg(Socket, IP, Port, Msg);
        _ ->
            Queue = case ets:lookup(State#dgram_state.queue, ID) of
                        [{ID, Q} | _] ->
                            if
                                length(Q) < ?MAX_QUEUE ->
                                    Q0 = lists:reverse(Q),
                                    Q1 = [Dgram | Q0],
                                    lists:reverse(Q1);
                                true ->
                                    Q
                            end;
                        _ ->
                            []
                    end,
            ets:insert(State#dgram_state.queue, {ID, Queue}),
            spawn_link(F)
    end,
    State.


init(UDPServer, ID) ->
    S1 = list_to_atom(atom_to_list(UDPServer) ++ ".dgram"),
    S2 = list_to_atom(atom_to_list(UDPServer) ++ ".dgram.queue"),
    #dgram_state{id        = ID,
                 requested = ets:new(S1, [public]),
                 queue     = ets:new(S2, [public]),
                 recv      = fun recv_func/5}.


send_queue(Socket, State, ID, IP, Port) ->
    case ets:lookup(State#dgram_state.queue, ID) of
        [{ID, Queue} | _] ->
            ets:delete(State#dgram_state.queue, ID),
            send_queue(Socket, State, ID, IP, Port, Queue);
        _ ->
            ok
    end.

send_queue(Socket, State, ID, IP, Port, [Dgram | T]) ->
    Msg = {State#dgram_state.id, Dgram},
    send_msg(Socket, IP, Port, Msg),
    send_queue(Socket, State, ID, IP, Port, T);
send_queue(_, _, _, _, _, []) ->
    ok.


recv_func(_Socket, _IP, _Port, _ID, _Dgram) ->
    ok.


dispatcher(State, Socket, IP, Port, {FromID, Dgram}) ->
    ets:insert(State#dgram_state.requested,
               {FromID, IP, Port, ermlibs:get_sec()}),

    Recv = State#dgram_state.recv,
    Data = binary_to_term(Dgram),
    Recv(Socket, IP, Port, FromID, Data),
    State;
dispatcher(State, _, _, _, _) ->
    State.

