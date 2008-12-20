-module(ermlibs).

-export([init/0]).
-export([sleep/1]).
-export([gen_nonce/0]).
-export([add2last/2]).
-export([get_sec/0]).


init() ->
    crypto:start().


sleep(MSec) ->
    receive
    after MSec ->
            ok
    end.

add2last(Data, List) ->
    L0 = lists:reverse(List),
    L1 = [Data | L0],
    lists:reverse(L1).


gen_nonce() ->
    <<Nonce:32>> = crypto:rand_bytes(4),
    Nonce.


get_sec() ->
    calendar:datetime_to_gregorian_seconds({date(), time()}).
