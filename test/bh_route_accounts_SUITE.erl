-module(bh_route_accounts_SUITE).

-compile([nowarn_export_all, export_all]).

-include("bh_route_handler.hrl").

-include("ct_utils.hrl").

all() -> [
          activity_result_test,
          activity_low_block_test,
          activity_filter_no_result_test
         ].

init_per_suite(Config) ->
    ?init_bh(Config).

end_per_suite(Config) ->
    ?end_bh(Config).

activity_result_test(_Config) ->
    %% Test activity for an account. This may or may not have data
    %% returned. Expect a maybe empty array with a start and end block
    %% and a cursor to a next block range
    {ok, {_, _, Json}} = ?json_request("/v1/accounts/1122ZQigQfeeyfSmH2i4KM4XMQHouBqK4LsTp33ppP3W2Knqh8gY/activity"),
    #{ <<"data">> := Data,
       <<"meta">> := #{ <<"start_block">> := StartBlock,
                        <<"end_block">> := EndBlock
                      },
       <<"cursor">> := Cursor
     } = Json,
    {ok,
     #{ <<"block">> := CursorBlock,
        <<"range">> := Range
      }
    } = ?CURSOR_DECODE(Cursor),
    ?assert(length(Data) >= 0),
    ?assert(StartBlock - EndBlock =< ?ACTIVITY_LIST_BLOCK_LIMIT),
    ?assertEqual(EndBlock, CursorBlock),
    ?assertEqual(?ACTIVITY_LIST_BLOCK_LIMIT, Range).

activity_low_block_test(_Config) ->
    GetCursor = #{ block => 50 },
    {ok, {_, _, Json}} = ?json_request("/v1/accounts/1122ZQigQfeeyfSmH2i4KM4XMQHouBqK4LsTp33ppP3W2Knqh8gY/activity?cursor=" ++ binary_to_list(?CURSOR_ENCODE(GetCursor))),
    #{ <<"data">> := Data,
       <<"meta">> := #{ <<"start_block">> := StartBlock,
                        <<"end_block">> := EndBlock
                      }
     } = Json,
    %% This account has just one coinebase transaction in block 1
    ?assertEqual(1, length(Data)),
    ?assertEqual(undefined, maps:get(<<"cursor">>, Json, undefined)),
    ?assertEqual(2, StartBlock),
    ?assertEqual(1, EndBlock).

activity_filter_no_result_test(_Config) ->
    %% We know this account has only a coinbase transaction in block 1 over that block range
    %% so filtering for rewards should return no data.
    GetCursor = #{ block => 50,
                   types => <<"rewards_v1">>
                 },
    {ok, {_, _, Json}} = ?json_request("/v1/accounts/1122ZQigQfeeyfSmH2i4KM4XMQHouBqK4LsTp33ppP3W2Knqh8gY/activity?cursor=" ++  binary_to_list(?CURSOR_ENCODE(GetCursor))),
    #{ <<"data">> := Data } = Json,
    ?assertEqual(0, length(Data)),
    ok.
