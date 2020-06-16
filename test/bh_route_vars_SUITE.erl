-module(bh_route_vars_SUITE).
-compile([nowarn_export_all, export_all]).

-include("ct_utils.hrl").

all() -> [
          name_test,
          list_test,
          activity_list_test
         ].

init_per_suite(Config) ->
    ?init_bh(Config).

end_per_suite(Config) ->
    ?end_bh(Config).

name_test(_Config) ->
    Names = ["poc_version", % integer
             "securities_percent", % float
             "predicate_callback_mod", % atom
             "price_oracle_public_keys" % oracle keys
            ],
    lists:foreach(fun(Name) ->
                          {ok, {_, _, Json}} = ?json_request(["/v1/vars/", Name]),
                          ?assertMatch(#{ <<"data">> := _ }, Json)
                  end, Names),
    ok.

list_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/vars"),
    #{ <<"data">> := Data } = Json,
    ?assert(map_size(Data) >= 0),

    ok.

activity_list_test(_Config) ->
    {ok, {_, _, AllJson}} = ?json_request("/v1/vars/activity"),
    #{ <<"data">> := AllData } = AllJson,
    ?assert(length(AllData) >= 0),

    ok.
