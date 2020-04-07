-module(bh_route_accounts_SUITE).

-compile([nowarn_export_all, export_all]).

-include("ct_utils.hrl").

all() -> [
          activity_filter_no_result_test,
          activity_filter_no_account_test
         ].

init_per_suite(Config) ->
    ?init_bh(Config).

end_per_suite(Config) ->
    ?end_bh(Config).

activity_filter_result_test(_Config) ->
    %% Test a filter on an account with at least transactions of that type.
    {ok, {_, _, Json}} = ?json_request("/v1/accounts/1122ZQigQfeeyfSmH2i4KM4XMQHouBqK4LsTp33ppP3W2Knqh8gY/activity?filter_types=dc_coinbase_v1"),
    #{ <<"data">> := TxnList} = Json,
    ?assert(length(TxnList) > 0).

activity_filter_no_account_test(_Config) ->
    %% Test a filter on an account that does not exist. Expect an empty list of results
    {ok, {_, _, Json}} = ?json_request("/v1/accounts/no_such_account/activity?filter_types=rewards_v1"),
    ?assertEqual(#{ <<"data">> => []}, Json).

activity_filter_no_result_test(_Config) ->
    %% Test a filter on an account with no transactions of that type. Expect an empty array
    {ok, {_, _, Json}} = ?json_request("/v1/accounts/1122ZQigQfeeyfSmH2i4KM4XMQHouBqK4LsTp33ppP3W2Knqh8gY/activity?filter_types=rewards_v1"),
    ?assertEqual(#{ <<"data">> => []}, Json).
