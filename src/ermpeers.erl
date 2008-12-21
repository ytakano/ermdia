%%%-------------------------------------------------------------------
%%% File    : ermpeers.erl
%%% Author  : Yuuki Takano <ytakano@jaist.ac.jp>
%%% Description : 
%%%
%%% Created : 20 Dec 2008 by Yuuki Takano <ytakano@jaist.ac.jp>
%%%-------------------------------------------------------------------
-module(ermpeers).

-behaviour(gen_server).

-define(MAX_GLOBAL, 100).
-define(DB_CONTACTED_TTL, 240).

%% API
-export([start_link/1, stop/1]).
-export([add_global/3, add_contacted/3, is_contacted/3]).
-export([expire/1]).


%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {global, contacted}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Server) ->
    gen_server:start_link({local, Server}, ?MODULE, [Server], []).

add_global(Server, IP, Port) ->
    gen_server:call(Server, {add_global, IP, Port}).

add_contacted(Server, IP, Port) ->
    gen_server:call(Server, {add_contacted, IP, Port}).

is_contacted(Server, IP, Port) ->
    gen_server:call(Server, {is_contacted, IP, Port}).

expire(Server) ->
    gen_server:call(Server, expire).

stop(Server) ->
    gen_server:cast(Server, stop).

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
init([Server]) ->
    TID = list_to_atom(atom_to_list(Server) ++ ".contacted"),

    {ok, #state{global = [], contacted = ets:new(TID, [])}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({add_global, IP, Port}, _From, State) ->
    G1 = ermlibs:add2last({IP, Port}, State#state.global),
    G2 = if
             length(G1) > ?MAX_GLOBAL ->
                 [_ | T] = G1,
                 T;
             true ->
                 G1
         end,
    
    Reply = ok,
    {reply, Reply, State#state{global = G2}};
handle_call({add_contacted, IP, Port}, _From, State) ->
    ets:insert(State#state.contacted, {{IP, Port}, ermlibs:get_sec()}),

    Reply = ok,
    {reply, Reply, State};
handle_call({is_contacted, IP, Port}, _From, State) ->
    Reply = case ets:lookup(State#state.contacted, {IP, Port}) of
                [] ->
                    false;
                _ ->
                    true
            end,

    {reply, Reply, State};
handle_call(expire, _From, State) ->
    expire_contacted(State),
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

expire_contacted(State) ->
    Dict = State#state.contacted,
    F = fun() ->
                expire_contacted(ets:first(Dict), Dict, ermlibs:get_sec())
        end,
    spawn_link(F).
expire_contacted(Key, _, _)
  when Key =:= '$end_of_state' ->
    ok;
expire_contacted(Key, Dict, Now) ->
    Next = ets:next(Dict, Key),

    case ets:lookup(Dict, Key) of
        [{_, Sec}] ->
            if
                Now - Sec > ?DB_CONTACTED_TTL ->
                    ets:delete(Dict, Key);
                true ->
                    ok
            end;
        _ ->
            ok
    end,
    
    expire_contacted(Next, Dict, Now).
    
