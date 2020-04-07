-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(init_bh(C), ct_utils:init_bh((C))).
-define(end_bh(C), ct_utils:end_bh((C))).
-define(json_request(P), ct_utils:json_request((P))).
-define(request(P), ct_utils:reqest((P))).
