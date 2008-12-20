%%%-------------------------------------------------------------------
%%% File    : ermrttable.erl
%%% Author  : Yuuki Takano <ytakano@jaist.ac.jp>
%%% Description : 
%%%
%%% Created : 17 Dec 2008 by Yuuki Takano <ytakano@jaist.ac.jp>
%%%-------------------------------------------------------------------
-module(ermrttable).

-behaviour(gen_server).

%% API
-export([start_link/2, stop/1]).
-export([add/5, remove/2, lookup/3]).
-export([replace/5, unset_ping/2]).
-export([print_state/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {server, id, table, keys}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link(Table, ID) -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Server, ID) ->
    gen_server:start_link({local, Server}, ?MODULE, [Server, ID], []).

stop(Server) ->
    gen_server:cast(Server, stop).

add(Server, ID, Host, Port, PingFunc)
  when is_integer(ID) ->
    gen_server:call(Server, {add, ID, Host, Port, PingFunc});
add(_, _, _, _, _) ->
    error.

remove(Server, ID)
  when is_integer(ID) ->
    gen_server:call(Server, {remove, ID});
remove(_, _) ->
    error.

lookup(Server, ID, Num)
  when is_integer(ID) ->
    gen_server:call(Server, {lookup, ID, Num});
lookup(_, _, _) ->
    error.

print_state(Server) ->
    gen_server:call(Server, print_state).


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
init([Server, ID | _]) ->
    Table = list_to_atom("rttable" ++ integer_to_list(ID)),
    {ok, #state{server = Server,
                id     = ID,
                table  = ets:new(Table, [public]),
                keys   = sets:new()}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({unset_ping, ID}, _From, State) ->
    put({ping, ID}, undefined),
    Reply = ok,
    {reply, Reply, State};
handle_call({replace, FromID, ID, IP, Port}, _From, State) ->
    I1 = id2i(State#state.id, FromID),
    I2 = id2i(State#state.id, ID),

    if
        I1 =:= I2 ->
            case ets:lookup(State#state.table, I1) of
                [{I1, Row} | _] ->
                    case [X || {ID0, _, _} = X <- Row, ID0 =:= FromID] of
                        [] ->
                            {reply, error, State};
                        _ ->
                            Row1 = [X || {ID0, _, _} = X <- Row,
                                         ID0 =/= FromID, ID0 =/= ID],
                            Row2 = ermlibs:add2last({ID, IP, Port}, Row1),

                            ets:insert(State#state.table, {I1, Row2}),
                            {reply, ok, State}
                    end;
                _ ->
                    {reply, error, State}
            end;
        true ->
            {reply, error, State}
    end;
handle_call({add, ID, Host, Port, PingFunc}, _From, State) ->
    I = id2i(State#state.id, ID),

    Row = case ets:lookup(State#state.table, I) of
              [] ->
                  [{ID, Host, Port}];
              [{I, R} | _] ->
                  if
                      length(R) < 20 ->
                          R0 = [X || X = {ID0, _, _} <- R, ID0 =/= ID],
                          ermlibs:add2last({ID, Host, Port}, R0);
                      true ->
                          R0 = [X || X = {ID0, _, _} <- R, ID0 =:= ID],
                          if
                              length(R0) > 0 ->
                                  R1 = [X || X = {ID0, _, _} <- R, ID0 =/= ID],
                                  ermlibs:add2last({ID, Host, Port}, R1);
                              true ->
                                  case find_ping_node(R) of
                                      false ->
                                          R;
                                      {ID1, _, _} ->
                                          F = fun() -> PingFunc(ID1) end,

                                          put({ping, ID1}, true),
                                          spawn_link(F),

                                          R
                                  end
                          end
                  end
          end,

    ets:insert(State#state.table, {I, Row}),
    Sets = sets:add_element(I, State#state.keys),

    Reply = ok,
    {reply, Reply, State#state{keys = Sets}};
handle_call({remove, ID}, _From, State) ->
    I = id2i(State#state.id, ID),
    
    Row = case ets:lookup(State#state.table, I) of
              [] ->
                  [];
              [{I, R} | _] ->
                  [X || X = {ID0, _, _} <- R, ID0 =/= ID]
          end,

    Sets = case Row of
               [] ->
                   ets:delete(State#state.table, I),
                   sets:del_element(I, State#state.keys);
               _ ->
                   ets:insert(State#state.table, {I, Row}),
                   sets:add_element(I, State#state.keys)
           end,
    
    Reply = ok,
    {reply, Reply, State#state{keys = Sets}};
handle_call({lookup, ID, Num}, _From, State) ->
    Nodes = id2nodes(State#state.id, ID,
                     State#state.table, State#state.keys, Num),
    {reply, Nodes, State};
handle_call(print_state, _From, State) ->
    io:format("ID = ~.16b~n~n", [State#state.id]),

    Keys = lists:sort(sets:to_list(State#state.keys)),
    print_table(Keys, State#state.table),

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
    {stop, normal, State};
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

find_ping_node([{ID, _, _} = Node| T]) ->
    case get({ping, ID}) of
        undefined ->
            Node;
        _ ->
            find_ping_node(T)
    end;
find_ping_node([]) ->
    false.

id2i(MyID, ID) -> id2i(MyID bxor ID, 1 bsl 159, 159).
id2i(_, _, I)    when I < 0           -> -1;
id2i(D, Mask, I) when D band Mask > 0 -> I;
id2i(D, Mask, I)                      -> id2i(D, Mask bsr 1, I - 1).


id2nodes(MyID, ID, Table, Keys, Max) ->
    Is = id2is(MyID, ID, Table, Keys, Max),

    Nodes1 = is2nodes(MyID, Is, Table),
    Nodes2 = [{ID bxor ID0, X} || X = {ID0, _, _} <- Nodes1],
    Nodes3 = lists:sort(Nodes2),
    
    lists:sublist(Nodes3, Max).


is2nodes(MyID, Is, Table) ->
    is2nodes(MyID, Is, Table, []).
is2nodes(_, [], _, Nodes) ->
    Nodes;
is2nodes(MyID, [-1 | T], Table, Nodes) ->
    is2nodes(MyID, T, Table, [{MyID, localhost, 0} | Nodes]);
is2nodes(MyID, [I | T], Table, Nodes) ->
    case ets:lookup(Table, I) of
        [] ->
            is2nodes(MyID, T, Table, Nodes);
        [{I, NS} | _] ->
            is2nodes(MyID, T, Table, lists:append(NS, Nodes))
    end.


id2is(MyID, ID, Table, Keys, Max) ->
    {N1, Is1} = id2i4lookup(MyID, ID, Table, Max),
    
    if
        N1 < Max ->
            {_, Is2} = id2i4lookupR(MyID, ID, Table, Keys, Max - N1),
            lists:append(Is1, Is2);
        true ->
            Is1
    end.


id2i4lookup(MyID, ID, Table, Max) ->
    id2i4lookup(MyID, ID, Table, Max, 0, []).
id2i4lookup(MyID, ID, _, _, N, Ret)
  when (MyID bxor ID) =:= 0 ->
    {N + 1, [-1 | Ret]};
id2i4lookup(_, _, _, Max, N, Ret)
  when N >= Max ->
    {N, Ret};
id2i4lookup(MyID, ID, Table, Max, N, Ret) ->
    I = id2i(MyID, ID),
    
    case ets:lookup(Table, I) of
        [] ->
            id2i4lookup(MyID, ID bxor (1 bsl I), Table, Max, N, Ret);
        [{I, Row} | _] ->
            id2i4lookup(MyID, ID bxor (1 bsl I),
                        Table, Max, N + length(Row), [I | Ret])
    end.


id2i4lookupR(MyID, ID, Table, Keys, Max) ->
    Keys0 = lists:sort(sets:to_list(Keys)),
    id2i4lookupR(MyID, bnot (MyID bxor ID), Table, Max, 0, Keys0, []).
id2i4lookupR(_, _, _, Max, N, _, Is)
  when N >= Max ->
    {N, lists:reverse(Is)};
id2i4lookupR(_, _, _, _, N, [], Is) ->
    {N, lists:reverse(Is)};
id2i4lookupR(MyID, ID, Table, Max, N, [Key | T], Is) ->
    Bits = 1 bsl Key,

    if
        (Bits band ID) =/= 0 ->
            case ets:lookup(Table, Key) of
                [{Key, Nodes} | _] ->
                    id2i4lookupR(MyID, ID, Table, Max, N + length(Nodes), T,
                                 [Key | Is]);
                _ ->
                    id2i4lookupR(MyID, ID, Table, Max, N, T, Is)
            end;
        true ->
            id2i4lookupR(MyID, ID, Table, Max, N, T, Is)
    end.


print_table([Key | T], Table) ->
    [{Key, Row} | _] = ets:lookup(Table, Key),
    io:format("k = ~p~n", [Key]),
    lists:foreach(fun({ID, Host, Port}) ->
                          io:format("  ID = ~.16b~n", [ID]),
                          io:format("  Host = ~p, Port = ~p~n", [Host, Port])
                  end,
                  Row),
    print_table(T, Table);
print_table([], _) ->
    ok.


replace(Server, FromID, ID, IP, Port) ->
    gen_server:call(Server, {replace, FromID, ID, IP, Port}).


unset_ping(Server, ID) ->
    gen_server:call(Server, {unset_ping, ID}).
