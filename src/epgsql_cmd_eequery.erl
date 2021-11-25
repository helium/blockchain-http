%% Single-roundtrip version of epgsql:equery/3
%%
%% It does parse-bind-execute sequence in 1 network roundtrip.
%% The cost is that user should manually provide the datatype information for
%% each bind-parameter.
%% Another potential problem is that connection will crash if epgsql does not
%% have a codec for any of result columns. Explicit type casting may save you
%% in this case: `SELECT my_enum::text FROM my_tab'. Or you can implement the
%% codec you need.
%%
%% Examples:
%% <pre>
%% CREATE TABLE public.test_eequery
%% (
%%   id bigserial NOT NULL DEFAULT,
%%   my_blob bytea,
%%   my_text text,
%%   my_timestamp timestamp with time zone,
%%   my_json json,
%%   CONSTRAINT test_eequery_pk PRIMARY KEY (id)
%% )
%% </pre>
%% <pre>
%% > epgsql_cmd_eequery:run(
%%     C,
%%     "INSERT INTO test_eequery (my_blob, my_text, my_timestamp, my_json) VALUES ($1, $2, $3, $4)",
%%     [<<1,2,3>>, <<"hello">>, calendar:universal_time(), <<"{}">>],
%%     [bytea, text, timestamptz, json]).
%% {ok,1}
%% > epgsql_cmd_eequery:run(
%%     C,
%%     "SELECT * FROM test_eequery", [], []).
%% {ok,[#column{name = <<"id">>,type = int8,oid = 20,size = 8,                            
%%              modifier = -1,format = 1},
%%      #column{name = <<"my_blob">>,type = bytea,oid = 17,
%%              size = -1,modifier = -1,format = 1},
%%      #column{name = <<"my_text">>,type = text,oid = 25,size = -1,
%%              modifier = -1,format = 1},
%%      #column{name = <<"my_timestamp">>,type = timestamptz,
%%              oid = 1184,size = 8,modifier = -1,format = 1},
%%      #column{name = <<"my_json">>,type = json,oid = 114,
%%              size = -1,modifier = -1,format = 1}],
%%     [{1,
%%       <<1,2,3>>,
%%       <<"hello">>,
%%       {{2020,1,28},{16,25,26.0}},
%%       <<"{}">>}]}
%% </pre>
%%
%% In case you provided wrong datatype, it's nothing serious, but server
%% will return an #error{} (no type conversion attempt will be made):
%% <pre>
%% > epgsql_cmd_eequery:run(C, "INSERT INTO test_eequery (my_json) VALUES ($1)", [<<"{}">>], [text]).
%% {error,#error{severity = error,code = <<"42804">>,
%%               codename = datatype_mismatch,
%%               message = <<"column \"my_json\" is of type json but expression is of type text">>,
%%               extra = [{file,<<"parse_target.c">>},
%%                        {hint,<<"You will need to rewrite or cast the expression.">>},
%%                        {line,<<"540">>},
%%                        {position,<<"44">>},
%%                        {routine,<<"transformAssignedExpr">>},
%%                        {severity,<<"ERROR">>}]}}
%% </pre>
%%
%% You can still do explicit type conversion:
%% <pre>
%% > epgsql_cmd_eequery:run(C, "INSERT INTO test_eequery (my_json) VALUES ($1::text::json)", [<<"{}">>], [text]).
%% {ok,1}
%% </pre>
%% But, if you are able to do that, it means you already know the type! So, why
%% add extra complexity?
%%
%% https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
%% > Parse
%% < ParseComplete
%% > Bind
%% < BindComplete
%% > Describe
%% < ParameterDescription
%% < RowDescription | NoData
%% > Execute
%% < {DataRow*
%% < CommandComplete} | EmptyQuery
%% > Close
%% < CloseComplete
%% > Sync
%% < ReadyForQuery
-module(epgsql_cmd_eequery).
-behaviour(epgsql_command).
-export([init/1, execute/2, handle_message/4]).
-export([run/4]).
-export_type([response/0]).

-type response() :: {ok, Count :: non_neg_integer(), Cols :: [epgsql:column()], Rows :: [tuple()]}
                  | {ok, Count :: non_neg_integer()}
                  | {ok, Cols :: [epgsql:column()], Rows :: [tuple()]}
                  | {error, epgsql:query_error()}.

-include_lib("epgsql/include/epgsql.hrl").
-include_lib("epgsql/include/protocol.hrl").

-record(eequery,
        {
         %% Data from client (init/1):
         sql :: [iodata()],
         param_types :: [[epgsql:epgsql_type()]],
         params :: [[any()]],
         %% Data from server:
         columns = [] :: [epgsql:column()],
         decoder :: undefined | epgsql_wire:row_decoder()
        }).

-spec run(epgsql:connection(), epgsql:sql_query(),
          [epgsql:bind_param()], [epgsql:epgsql_type()]) -> response().
run(C, SQL, Params, ParamTypes) ->
    epgsql_sock:sync_command(C, ?MODULE, {SQL, Params, ParamTypes}).

init({batch, Queries}) ->
    lists:foldl(fun({SQL, Params, Types}, #eequery{sql=SQLs, param_types=Typess, params = Paramss}) ->
                        #eequery{sql=[SQL|SQLs], param_types=[Types|Typess], params=[Params|Paramss]}
                end, #eequery{sql=[], param_types=[], params=[]}, lists:reverse(Queries));
init({SQL, Params, Types}) ->
    #eequery{sql = [SQL],
             param_types = [Types],
             params = [Params]}.


execute(Sock, #eequery{sql = Sqls, param_types = ParamTypess, params = Paramss} = St) ->
    %% #statement{name = StatementName, columns = Columns} = Stmt,
    Codec = epgsql_sock:get_codec(Sock),
    Commands = lists:flatmap(fun({Sql, ParamTypes, Params}) ->
                          BinParamTypes = epgsql_wire:encode_types(ParamTypes, Codec),
                          TypedParams = lists:zip(ParamTypes, Params),
                          BinParameters = epgsql_wire:encode_parameters(TypedParams, Codec),
                          %% XXX: we ask server to send all columns in binary format.
                          %% If we don't have a decoder for any of the result columns (eg, enums),
                          %% connection process will crash
                          BinAllBinaryResult = <<1:?int16, 1:?int16>>,
                          [
                           {?PARSE, ["", 0, Sql, 0, BinParamTypes]},
                           {?BIND, ["", 0, "", 0, BinParameters, BinAllBinaryResult]},
                           {?DESCRIBE, [?PREPARED_STATEMENT, "", 0]},
                           {?EXECUTE, ["", 0, <<0:?int32>>]},
                           {?CLOSE, [?PREPARED_STATEMENT, "", 0]}]
                  end, lists:zip3(Sqls, ParamTypess, Paramss)),
    epgsql_sock:send_multi(
      Sock,
        Commands ++ [{?SYNC, []}]),
    {ok, Sock, St}.


handle_message(?PARSE_COMPLETE, <<>>, Sock, _State) ->
    {noaction, Sock};

handle_message(?PARAMETER_DESCRIPTION, _Bin, Sock, _State) ->
    %% Since ?BIND is executed before ?DESCRIBE, we will not get
    %% ?PARAMETER_DESCRIPTION message at all if user-provided types do not
    %% match server expectations (server will send #error{} instead).
    %% If they do match, there is no point parsing this message, because we
    %% already have all the same info in #eequery.param_types
    {noaction, Sock};
handle_message(?ROW_DESCRIPTION, <<Count:?int16, Bin/binary>>, Sock,
               #eequery{} = St) ->
    Codec = epgsql_sock:get_codec(Sock),
    Columns = epgsql_wire:decode_columns(Count, Bin, Codec),
    Columns2 = [Col#column{format = epgsql_wire:format(Col, Codec)}
                || Col <- Columns],
    Decoder = epgsql_wire:build_decoder(Columns2, Codec),
    Sock2 = epgsql_sock:notify(Sock, {columns, Columns2}),
    {noaction, Sock2, St#eequery{columns = Columns2, decoder = Decoder}};
handle_message(?NO_DATA, <<>>, Sock, #eequery{}) ->
    {noaction, Sock};

handle_message(?BIND_COMPLETE, <<>>, Sock, #eequery{}) ->
    {noaction, Sock};

handle_message(?DATA_ROW, <<_Count:?int16, Bin/binary>>,
               Sock, #eequery{decoder = Decoder} = St) ->
    Row = epgsql_wire:decode_data(Bin, Decoder),
    {add_row, Row, Sock, St};
handle_message(?COMMAND_COMPLETE, Bin, Sock,
               #eequery{columns = Cols} = St) ->
    Complete = epgsql_wire:decode_complete(Bin),
    Rows = epgsql_sock:get_rows(Sock),
    Result = case Complete of
                 {_, Count} when Cols == [] ->
                     {ok, Count};
                 {_, Count} ->
                     {ok, Count, Cols, Rows};
                 _ ->
                     {ok, Cols, Rows}
             end,
    {add_result, Result, {complete, Complete}, Sock, St};
handle_message(?EMPTY_QUERY, <<>>, Sock, St) ->
    {add_result, {ok, [], []}, {complete, empty}, Sock, St};

handle_message(?CLOSE_COMPLETE, _, Sock, _State) ->
    {noaction, Sock};

handle_message(?READY_FOR_QUERY, _Status, Sock, _State) ->
    case epgsql_sock:get_results(Sock) of
        [Result] ->
            {finish, Result, done, Sock};
        [] ->
            {finish, done, done, Sock};
        Results ->
            {finish, Results, done, Sock}
    end;

handle_message(?ERROR, Error, Sock, St) ->
    Result = {error, Error},
    {add_result, Result, Result, Sock, St};
handle_message(_, _, _, _) ->
    unknown.
