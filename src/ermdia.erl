-module(ermdia).

-export([init/0]).

-export([test_nat_server/0, test_nat_client/2]).

init() ->
    ermlibs:init(),
    ermlogger:start().


test_nat_server() ->
    init(),
    ermudp:test_nat_detector_server().


test_nat_client(Host, Port) ->
    init(),
    ermudp:test_nat_detector_client(Host, Port).

