-module(boss_db_adapter_riak).
-behaviour(boss_db_adapter).
-export([init/1, terminate/1, start/1, stop/0, find/2, find/3, find/7]).
-export([count/3, counter/2, incr/2, incr/3, delete/2, save_record/2]).
-export([push/2, pop/2]).

-define(LOG(Name, Value), lager:debug("DEBUG: ~s: ~p~n", [Name, Value])).

-define(HUGE_INT, 1000 * 1000 * 1000 * 1000).

start(_) ->
    ok.

stop() ->
    ok.

init(Options) ->
    Host = proplists:get_value(db_host, Options, "localhost"),
    Port = proplists:get_value(db_port, Options, 8087),
    riakc_pb_socket:start_link(Host, Port).

terminate(Conn) ->
    riakc_pb_socket:stop(Conn).

find(Conn, Id, Bucket) ->
    Socket = riakc_pb_socket:get(Conn, type_to_bucket(Bucket), Id),
    query_riak_socket(binary_to_list(Id), list_to_atom(Bucket), Socket).

find(Conn, Id) ->
    {Type, Bucket, Key} = infer_type_from_id(Id),
    Socket = riakc_pb_socket:get(Conn, Bucket, Key),
    query_riak_socket(Id, Type, Socket).

query_riak_socket(Id, Type, _Socket = {ok, Res} ) ->
    Value		= riakc_obj:get_value(Res),
    Data		= Value,
    ConvertedData	= riak_search_decode_data(Data),
    AttributeTypes	= boss_record_lib:attribute_types(Type),
    Record              = get_record_from_riak(Type, ConvertedData,
					       AttributeTypes);
query_riak_socket(_Id, _Type, _Socket = {error, Reason} ) ->
    {error, Reason}.


get_record_from_riak(Type, ConvertedData, AttributeTypes) ->
    Lambda = fun (AttrName) ->
		     Val      = proplists:get_value(AttrName, ConvertedData),
		     AttrType = proplists:get_value(AttrName, AttributeTypes),
		     boss_record_lib:convert_value_to_type(Val, AttrType)
             end,
    apply(Type, new, lists:map(Lambda, boss_record_lib:attribute_names(Type))).

find_acc(_, _, [], Acc) ->
    lists:reverse(Acc);
find_acc(Conn, Prefix, [Id | Rest], Acc) ->
    case find(Conn, Id, Prefix) of
        {error, _Reason} ->
            find_acc(Conn, Prefix, Rest, Acc);

        Value ->
            find_acc(Conn, Prefix, Rest, [Value | Acc])
    end.

% this is a stub just to make the tests runable
find(Conn, Type, Conditions, Max, Skip, Sort, SortOrder) ->
    Bucket = type_to_bucket_name(Type), % return bucket name into list type
    {ok, Keys} = get_keys(Conn, Conditions, Bucket),
    Records = find_acc(Conn, atom_to_list(Type), Keys, []),
    Sorted = if
        is_atom(Sort) ->
            lists:sort(fun (A, B) ->
                        case SortOrder of
                            ascending  -> A:Sort() =< B:Sort();
                            descending -> A:Sort() > B:Sort()
                        end
                end,
                Records);
        true -> Records
    end,
    case Max of
        all -> lists:nthtail(Skip, Sorted);
        Max when Skip < length(Sorted) ->
            lists:sublist(Sorted, Skip + 1, Max);
        _ ->
            []
    end.

get_keys(Conn, [], Bucket) ->
    riakc_pb_socket:list_keys(Conn, Bucket);
get_keys(Conn, Conditions, Bucket) ->
    {ok, {search_results, KeysExt, _, _}} = riakc_pb_socket:search(
					      Conn, list_to_binary(Bucket), list_to_binary(build_search_query(Conditions))),
    {ok, lists:map(fun ({_,X}) ->
			   proplists:get_value(<<"_yz_rk">>, X)
		   end, KeysExt)}.


% this is a stub just to make the tests runable
count(Conn, Type, Conditions) ->
    length(find(Conn, Type, Conditions, all, 0, 0, 0)).

counter(_Conn, _Id) ->
    {error, notimplemented}.

incr(Conn, Id) ->
    incr(Conn, Id, 1).
incr(_Conn, _Id, _Count) ->
    {error, notimplemented}.


delete(Conn, Id) ->
    {_Type, Bucket, Key} = infer_type_from_id(Id),
    ok = riakc_pb_socket:delete(Conn, Bucket, Key).

%The call riakc_obj:new(Bucket::binary(),'undefined',PropList::[{binary(),_}]) 
% will never return since the success typing is
% ('undefined' | binary(),'undefined' | binary(),'undefined' | binary()) -> 
% {'riakc_obj','undefined' | binary(),'undefined' | binary(),'undefined',[],'undefined','undefined' | binary()} and the contract is 
% (bucket(),key(),value()) -> riakc_obj()
save_record(Conn, Record) ->
    Type = element(1, Record),
    Bucket = list_to_binary(type_to_bucket_name(Type)),
    %PropList = [string:join(["\"", riak_search_encode_key(K), "\":", riak_search_encode_value(V), ","], "") || {K, V} <- Record:attributes(), K =/= id],
    PropList = [string:join(["\"", atom_to_list(K), "\":", riak_encode_value(V), ","], "") || {K, V} <- Record:attributes(), K =/= id],
    %PropList = [{riak_search_encode_key(K), riak_search_encode_value(V)} || {K, V} <- Record:attributes(), K =/= id],
    RiakKey = case Record:id() of
        id -> % New entry
        	  GUID       = uuid:to_string(uuid:uuid4()),
            IdPropList = string:join(["\"", atom_to_list(id), "\":\"", atom_to_list(Type), "-", GUID, "\"}"], ""),
            NewPropList = string:join(["{", PropList, IdPropList], ""),
            io:format("~p", [NewPropList]),
            %NewPropList = [{id, atom_to_list(Type) ++ "-" ++ GUID} | PropList],
            O		= riakc_obj:new(Bucket, list_to_binary(GUID), list_to_binary(NewPropList), atom_to_list('application/json')),
            %O		= riakc_obj:new(Bucket, GUID, NewPropList, atom_to_list('application/json')),
            {ok, RO}	= riakc_pb_socket:put(Conn, O, [return_body]),
            element(3, RO);
        DefinedId when is_list(DefinedId) -> % Existing Entry
            [_ | Tail]	= string:tokens(DefinedId, "-"),
            Key		= string:join(Tail, "-"),
            BinKey	= list_to_binary(Key),
            {ok, O}	= riakc_pb_socket:get(Conn, Bucket, BinKey),
            O2		= riakc_obj:update_value(O, PropList),
            ok		= riakc_pb_socket:put(Conn, O2),
            Key
    end,
    {ok, Record:set(id, lists:concat([Type, "-", binary_to_list(RiakKey)]))}.

% These 2 functions are not part of the behaviour but are required for
% tests to pass
push(_Conn, _Depth) -> ok.

pop(_Conn, _Depth) -> ok.

% Internal functions

infer_type_from_id(Id) when is_list(Id) ->
    [Type | Tail] = string:tokens(Id, "-"),
    BossId = string:join(Tail, "-"),
    {list_to_atom(Type), type_to_bucket(Type), list_to_binary(BossId)}.

% Find bucket name from Boss type
type_to_bucket(Type) ->
    list_to_binary(type_to_bucket_name(Type)).

type_to_bucket_name(Type) when is_atom(Type) ->
    type_to_bucket_name(atom_to_list(Type));
type_to_bucket_name(Type) when is_list(Type) ->
    inflector:pluralize(Type).

build_search_query(Conditions) ->
    Terms = build_search_query(Conditions, []),
    string:join(Terms, " AND ").

build_search_query([], Acc) ->
    lists:reverse(Acc);
build_search_query([{Key, 'equals', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat([Key, ":", quote_value(Value)])|Acc]);
build_search_query([{Key, 'not_equals', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat(["NOT ", Key, ":", quote_value(Value)])|Acc]);
build_search_query([{Key, 'in', Value}|Rest], Acc) when is_list(Value) ->
    build_search_query(Rest, [lists:concat(["(", string:join(lists:map(fun(Val) ->
                                    lists:concat([Key, ":", quote_value(Val)])
                            end, Value), " OR "), ")"])|Acc]);
build_search_query([{Key, 'not_in', Value}|Rest], Acc) when is_list(Value) ->
    build_search_query(Rest, [lists:concat(["(", string:join(lists:map(fun(Val) ->
                                    lists:concat(["NOT ", Key, ":", quote_value(Val)])
                            end, Value), " AND "), ")"])|Acc]);
build_search_query([{Key, 'in', {Min, Max}}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat([Key, ":", "[", Min, " TO ", Max, "]"])|Acc]);
build_search_query([{Key, 'not_in', {Min, Max}}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat(["NOT ", Key, ":", "[", Min, " TO ", Max, "]"])|Acc]);
build_search_query([{Key, 'gt', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat([Key, ":", "{", Value, " TO ", ?HUGE_INT, "}"])|Acc]);
build_search_query([{Key, 'lt', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat([Key, ":", "{", -?HUGE_INT, " TO ", Value, "}"])|Acc]);
build_search_query([{Key, 'ge', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat([Key, ":", "[", Value, " TO ", ?HUGE_INT, "]"])|Acc]);
build_search_query([{Key, 'le', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat([Key, ":", "[", -?HUGE_INT, " TO ", Value, "]"])|Acc]);
build_search_query([{Key, 'matches', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat([Key, ":", Value])|Acc]);
build_search_query([{Key, 'not_matches', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat(["NOT ", Key, ":", Value])|Acc]);
build_search_query([{Key, 'contains', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat([Key, ":", escape_value(Value)])|Acc]);
build_search_query([{Key, 'not_contains', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat(["NOT ", Key, ":", escape_value(Value)])|Acc]);
build_search_query([{Key, 'contains_all', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat(["(", string:join(lists:map(fun(Val) ->
                                lists:concat([Key, ":", escape_value(Val)])
                        end, Value), " AND "), ")"])|Acc]);
build_search_query([{Key, 'not_contains_all', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat(["NOT ", "(", string:join(lists:map(fun(Val) ->
                                lists:concat([Key, ":", escape_value(Val)])
                        end, Value), " AND "), ")"])|Acc]);
build_search_query([{Key, 'contains_any', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat(["(", string:join(lists:map(fun(Val) ->
                                lists:concat([Key, ":", escape_value(Val)])
                        end, Value), " OR "), ")"])|Acc]);
build_search_query([{Key, 'contains_none', Value}|Rest], Acc) ->
    build_search_query(Rest, [lists:concat(["NOT ", "(", string:join(lists:map(fun(Val) ->
                                lists:concat([Key, ":", escape_value(Val)])
                        end, Value), " OR "), ")"])|Acc]).

quote_value(Value) when is_list(Value) ->
    quote_value(Value, []);
quote_value(Value) ->
    Value.

quote_value([], Acc) ->
    [$"|lists:reverse([$"|Acc])];
quote_value([$"|T], Acc) ->
    quote_value(T, lists:reverse([$\\, $"], Acc));
quote_value([H|T], Acc) ->
    quote_value(T, [H|Acc]).

escape_value(Value) ->
    escape_value(Value, []).

escape_value([], Acc) ->
    lists:reverse(Acc);
escape_value([H|T], Acc) when H=:=$+; H=:=$-; H=:=$&; H=:=$|; H=:=$!; H=:=$(; H=:=$);
                              H=:=$[; H=:=$]; H=:=${; H=:=$}; H=:=$^; H=:=$"; H=:=$~;
                              H=:=$*; H=:=$?; H=:=$:; H=:=$\\ ->
    escape_value(T, lists:reverse([$\\, H], Acc));
escape_value([H|T], Acc) ->
    escape_value(T, [H|Acc]).

riak_search_encode_key(K) ->
    list_to_binary(atom_to_list(K)).

riak_search_encode_value(V) when is_list(V) ->
    list_to_binary(V);
riak_search_encode_value(V) ->
    V.

riak_search_decode_data(Data) ->
    [{riak_search_decode_key(K), riak_search_decode_value(V)} || {K, V} <- element(2, mochijson:decode(Data))].

riak_search_decode_key(K) when is_binary(K) ->
    list_to_atom(binary_to_list(K));
riak_search_decode_key(K) ->
    list_to_atom(K).

riak_search_decode_value(V) when is_binary(V) ->
    binary_to_list(V);
riak_search_decode_value(V) ->
    V.

riak_encode_value(V) when is_list(V) ->
    "\"" ++ V ++ "\"";
riak_encode_value(V) when is_integer(V) ->
    integer_to_list(V);
riak_encode_value(V) when is_boolean(V) ->
    atom_to_list(V);
riak_encode_value(V) when is_float(V) ->
    float_to_list(V).