%%%-------------------------------------------------------------------
%%% @author miguel benitezhm@gmail.com
%%% @copyright (C) 2014, <Systrix LLC>
%%% @doc
%%%
%%% @end
%%% Created : 22. dic 2014 02:05 PM
%%%-------------------------------------------------------------------
-module(boss_db_indexer_riak).
-author("miguel - miguel@systrix.net").
-include_lib("kernel/include/file.hrl").
%% API
-export([indexing_model/2, files/2, run/0]).

%% Top is the Top directory where everything starts
%% Re is a regular expression to match for (see module regexp)
%% Actions is a Fun to apply to each found file
%% Return value is a lists of the return values from the
%% Action function

%% Example: find:files("/home/klacke",
%%                     ".*\.erl", fun(F) -> {File, c:c(File)} end)
%% Will find all erlang files in my top dir, compile them and
%% return a long list of {File, CompilationResult} tuples
%% If an error occurs, {error, {File, Reason}} is returned
%% The Action fun is passed the full long file name as parameter

%% By now the only way to update a solr schema is to execute this rp(yz_index:reload(<<"index_name">>)). via riak attach
%% Important Note: by this time the re-index of old values y solr are no posible, there is a proposal about this on
%% https://github.com/basho/yokozuna/issues/403 section Mutating Schemas


run() ->
  files("src/model", ".*\.erl").

files(Top, Re) ->
  case file:list_dir(Top) of
    {ok, Files} ->
      files(Top, Files, Re, []),
      lager:info("~n\tIf you are updating a schema remember to run \"rp(yz_index:reload(<<\"index_name\">>)).\" through riak attach~n");
    {error, Reason}  ->
      {error, {Top, Reason}}
  end.

files(Top, [F|Tail], Re, Ack) ->
  F2 = Top ++ "/" ++ F,
  case file:read_file_info(F2) of
    {ok, FileInfo} when FileInfo#file_info.type == directory ->
      case files(F2, Re) of
        {error, Reason} ->
          {error, Reason};
        List ->
          files(Top, Tail, Re, List ++ Ack)
      end;
    {error, Reason} ->
      {error, {F2, Reason}};
    {ok, FileInfo} when FileInfo#file_info.type == regular ->
      case catch re:run(F, Re) of
        {match, _} ->
          files(Top, Tail, Re, [indexing_model(F2, F) | Ack]);
        nomatch ->
          files(Top, Tail, Re, Ack);
        {error, Reason} ->
          {error, {F2, {re, Reason}}}
      end;
    _Other ->
      files(Top, Tail, Re, Ack)
  end;

files(_Top, [], _Re, Ack) ->
  Ack.

indexing_model(File, FileName) ->
  io:format("Process file: ~s~n", [File]),
  {ok, Device} = file:open(File, [read]),
  try get_all_lines(Device, FileName, [])
    after file:close(Device)
  end.

get_all_lines(Device, FileName, IndexedFields) ->
  case io:get_line(Device, "") of
    eof  ->
      [XMLFile, _] = string:tokens(FileName, "."),
      % All of these fields are required by Riak Search
      RequiredFields = [{field, [{name, "_yz_id"}, {type, "_yz_str"}, {indexed, "true"}, {stored, "true"}, {multiValued, "false"}, {required, "true"}], []},
        {field, [{name, "_yz_ed"}, {type, "_yz_str"}, {indexed, "true"}, {stored, "false"}, {multiValued, "false"}], []},
        {field, [{name, "_yz_pn"}, {type, "_yz_str"}, {indexed, "true"}, {stored, "false"}, {multiValued, "false"}], []},
        {field, [{name, "_yz_fpn"}, {type, "_yz_str"}, {indexed, "true"}, {stored, "false"}, {multiValued, "false"}], []},
        {field, [{name, "_yz_vtag"}, {type, "_yz_str"}, {indexed, "true"}, {stored, "false"}, {multiValued, "false"}], []},
        {field, [{name, "_yz_rk"}, {type, "_yz_str"}, {indexed, "true"}, {stored, "true"}, {multiValued, "false"}], []},
        {field, [{name, "_yz_rt"}, {type, "_yz_str"}, {indexed, "true"}, {stored, "true"}, {multiValued, "false"}], []},
        {field, [{name, "_yz_rb"}, {type, "_yz_str"}, {indexed, "true"}, {stored, "true"}, {multiValued, "false"}], []},
        {field, [{name, "_yz_err"}, {type, "_yz_str"}, {indexed, "true"}, {stored, "false"}, {multiValued, "false"}], []},
        {dynamicField, [{name, "*"}, {type, "ignored"}], []}],
      XMLData = set_required_fields(IndexedFields, RequiredFields),
      Types = [{fieldType, [{name, "_yz_str"}, {class, "solr.StrField"}, {sortMissingLast, "true"}], []},
        {fieldType, [{name, "string"}, {class, "solr.StrField"}, {sortMissingLast, "true"}], []},
        {fieldType, [{name, "boolean"}, {class, "solr.BoolField"}, {sortMissingLast, "true"}], []},
        {fieldType, [{name, "int"}, {class, "solr.TrieIntField"}, {precisionStep, "0"}, {positionIncrementGap, "0"}], []},
        {fieldType, [{name, "float"}, {class, "solr.TrieFloatField"}, {precisionStep, "0"}, {positionIncrementGap, "0"}], []},
        {fieldType, [{name, "date"}, {class, "solr.TrieDateField"}, {precisionStep, "0"}, {positionIncrementGap, "0"}], []},
        {fieldType, [{name, "ignored"}, {stored, "false"}, {indexed, "false"}, {multiValued, "true"}, {class, "solr.StrField"}], []}],
      Fields = [{fields, [], XMLData}, {uniqueKey, [], ["_yz_id"]}, {types, [], Types}],
      Type = inflector:pluralize(XMLFile),
      Data2 = {schema, [{name, Type}, {version, "1.5"}], Fields},
      Prolog = ["<?xml version=\"1.0\" encoding=\"UTF-8\"?>"],
      file:write_file("/tmp/" ++ Type ++ ".xml", xmerl:export_simple([Data2], xmerl_xml, [{prolog, Prolog}])),
      lager:info("XML File: ~s.xml~n", [Type]),
      if
        length(IndexedFields) > 0 ->
          create_index("/tmp/" ++ Type ++ ".xml");
        true ->
          lager:info("Model ~p without indexed fields", [Type])
      end;
    Line ->
      case re:run(Line, "%- indexed") of
        {match, _} ->
          io:fwrite(Line),
          case re:run(Line, "::()") of % If type not defined usr string by default
            {match, _} -> [RawField, FieldType, _] = string:tokens(string:strip(Line, left, $ ), "::(),");
            nomatch ->
              [RawField, _, _] = string:tokens(string:strip(Line, left, $ ), "% "),
              FieldType = "string"
          end,
          Field = join_fields(re:split(RawField, "(?=[A-Z])"), []),
          lager:info("Field: ~s~n", [string:to_lower(Field)]),
          lager:info("Type: ~s~n", [FieldType]),
          Data = {field, [{name, string:to_lower(Field)}, get_solr_type(list_to_atom(FieldType)), {indexed, "true"}, {stored, "true"}], []},
          get_all_lines(Device, FileName, [Data | IndexedFields]);
        nomatch ->
          get_all_lines(Device, FileName, IndexedFields);
        {error, Reason} ->
          {error, {Line, {re, Reason}}}
      end
  end.

get_solr_type(string) ->
  {type, "string"};
get_solr_type(integer) ->
  {type, "int"};
get_solr_type(boolean) ->
  {type, "boolean"};
get_solr_type(float) ->
  {type, "float"};
get_solr_type(datetime) ->
  {type, "date"};
get_solr_type(timestamp) ->
  {type, "date"}.

join_fields([], Fields) ->
  string:substr(Fields, 1, string:len(Fields) -1);
join_fields([H|Tail], Fields) ->
  F = Fields ++ binary_to_list(H) ++ "_",
  join_fields(Tail, F).

set_required_fields([], XMLData) ->
  XMLData;
set_required_fields([H|Tail], XMLData) ->
  set_required_fields(Tail, [H | XMLData]).


create_index(XMLFile) ->
  {ok, Config} = file:consult("boss.config"),
  [H|_] = Config,
  [C|_] = H,
  Options = element(2, C),
  {ok, Conn} = init_db(Options),
  lager:info("DB connection ready~n"),
  [_, IndexName, _] = string:tokens(XMLFile, "/."),
  lager:info("IndexName ~s~n", [IndexName]),
  Type = inflector:pluralize(IndexName),
  lager:info("Type ~s~n", [list_to_binary(Type)]),
  {ok, SchemaData} = file:read_file(XMLFile),
  %Install schema for search
  case riakc_pb_socket:create_search_schema(Conn, list_to_binary(Type), SchemaData) of
    ok -> lager:info("Schema created successfully~n");
    {error, Reason} -> lager:error(Reason)
  end,
  timer:sleep(5000),
  % Create Index
  case riakc_pb_socket:create_search_index(Conn, list_to_binary(Type), list_to_binary(Type), []) of
    ok -> lager:info("Index created successfully~n");
    {error, ReasonI} -> io:fwrite(ReasonI)
  end,
  timer:sleep(5000),
  % Asociate with bucket
  case riakc_pb_socket:set_search_index(Conn, list_to_binary(Type), list_to_binary(Type)) of
    ok -> lager:info("Index installed successfully on bucket~n");
    {error, _} ->
      lager:warning("The installation on the bucket has fail, retrying in 5 seconds...~n"),
      timer:sleep(5000),
      case riakc_pb_socket:set_search_index(Conn, list_to_binary(Type), list_to_binary(Type)) of
        ok -> lager:info("Index installed successfully on bucket~n");
        {error, ReasonB2} ->
          lager:error(ReasonB2)
      end
  end,
  terminate(Conn).

init_db(Options) ->
  Host = proplists:get_value(db_host, Options, "localhost"),
  Port = proplists:get_value(db_port, Options, 8087),
  riakc_pb_socket:start_link(Host, Port).

terminate(Conn) ->
  riakc_pb_socket:stop(Conn).

%TO RUN THE DEMO EXCEUTE THIS FROM ./init-dev console
%boss_db_indexer_riak:files("/home/miguel/Descargas/hello_world/src/model", ".*\.erl").
