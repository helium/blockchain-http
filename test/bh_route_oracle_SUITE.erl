-module(bh_route_oracle_SUITE).
-compile([nowarn_export_all, export_all]).

-include("ct_utils.hrl").

all() -> [
          price_test,
          list_test,
          activity_list_test,
          price_predictions_test
         ].

init_per_suite(Config) ->
    ?init_bh(Config).

end_per_suite(Config) ->
    ?end_bh(Config).

price_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/oracle/prices/current"),
    ?assertMatch(#{ <<"data">> :=
                        #{ <<"block">> :=  _,
                           <<"price">> := _
                         }
                  }, Json),

    ok.

list_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/oracle/prices"),
    #{ <<"data">> := Data } = Json,
    ?assert(length(Data) >= 0),

    ok.

activity_list_test(_Config) ->
    {ok, {_, _, AllJson}} = ?json_request("/v1/oracle/activity"),
    #{ <<"data">> := AllData } = AllJson,
    ?assert(length(AllData) >= 0),

    {ok, {_, _, OneJson}} = ?json_request("/v1/oracle/13CFFcmPtMvNQCpWQRXCTqXPnXtcsibDWVwiQRKpUCt4nqtF7RE/activity"),
    #{ <<"data">> := OneData } = OneJson,
    ?assert(length(OneData) >= 0),

    ok.

price_predictions_test(_Config) ->
    {ok, {_, _, AllJson}} = ?json_request("/v1/oracle/predictions"),
    #{ <<"data">> := AllData } = AllJson,
    ?assert(length(AllData) >= 0),

    ok.
