%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_clientinfo_memory_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

all() ->
    case os:getenv("EMQX_CLIENTINFO_MEMORY_SCENARIO") of
        false -> [];
        _ -> [t_measure]
    end.

init_per_suite(Config) ->
    Port = emqx_common_test_helpers:select_free_port(tcp),
    AppConfig = io_lib:format(
        """
        listeners.tcp.default.bind = "127.0.0.1:~B"
        listeners.tcp.default.enable_authn = true
        listeners.ssl.default.enable = false
        listeners.ws.default.enable = false
        listeners.wss.default.enable = false
        authorization.no_match = allow
        authorization.cache.enable = true
        mqtt.client_attrs_init = [
          {expression = "user_property.tns", set_as_attr = "tns"},
          {expression = "user_property.attr_a", set_as_attr = "attr_a"},
          {expression = "user_property.attr_b", set_as_attr = "attr_b"},
          {expression = "user_property.untrusted", set_as_attr = "untrusted"}
        ]
        """,
        [Port]
    ),
    Apps = emqx_cth_suite:start(
        [{emqx, #{config => AppConfig}}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    ok = emqx_hooks:add(
        'client.authenticate', {?MODULE, authenticate, []}, ?HP_LOWEST
    ),
    [{apps, Apps}, {port, Port} | Config].

end_per_suite(Config) ->
    emqx_hooks:del('client.authenticate', {?MODULE, authenticate}),
    emqx_cth_suite:stop(?config(apps, Config)).

%% Measure stable MQTT channel terms and process memory for one trust scenario.
t_measure(Config) ->
    Scenario = scenario(),
    Count = env_integer("EMQX_CLIENTINFO_MEMORY_COUNT", 1_000),
    Runs = env_integer("EMQX_CLIENTINFO_MEMORY_RUNS", 3),
    persistent_term:put({?MODULE, scenario}, Scenario),
    configure_scenario(Scenario),
    Results = [measure_run(Run, Count, ?config(port, Config)) || Run <- lists:seq(1, Runs)],
    Output = os:getenv("EMQX_CLIENTINFO_MEMORY_OUTPUT"),
    ok = file:write_file(
        Output,
        io_lib:format("~p.~n", [
            #{
                scenario => Scenario,
                count => Count,
                runs => Results,
                otp_release => erlang:system_info(otp_release),
                word_size => erlang:system_info(wordsize),
                implementation_stage => implementation_stage()
            }
        ])
    ),
    ok.

authenticate(_Credential, _DefaultResult) ->
    Result0 = #{is_superuser => false},
    Result =
        case implementation_stage() of
            final -> Result0#{trusted_attrs => trusted_mask()};
            _ -> Result0
        end,
    {stop, {ok, Result}}.

measure_run(Run, Count, Port) ->
    Pairs = [connect_client(Run, I, Port) || I <- lists:seq(1, Count)],
    Clients = [Client || {_ClientId, Client} <- Pairs],
    ClientIds = [ClientId || {ClientId, _Client} <- Pairs],
    [subscribe(Client) || Client <- Clients],
    [publish(Client) || Client <- Clients],
    ChannelPids = [channel_pid(ClientId) || ClientId <- ClientIds],
    stable_gc(ChannelPids),
    ProcessMeasurements = [process_measurement(Pid) || Pid <- ChannelPids],
    RepresentativePid = hd(ChannelPids),
    ConnectionState = sys:get_state(RepresentativePid),
    Channel = find_tagged_tuple(channel, ConnectionState),
    ClientInfo = maps:get(clientinfo, emqx_connection:info(RepresentativePid)),
    {dictionary, ProcessDictionary} = process_info(RepresentativePid, dictionary),
    Result = #{
        run => Run,
        process => summarize_processes(ProcessMeasurements),
        clientinfo => term_measurement(ClientInfo),
        channel => term_measurement(Channel),
        connection_state => term_measurement(ConnectionState),
        process_dictionary => term_measurement(ProcessDictionary)
    },
    [emqtt:disconnect(Client) || Client <- Clients],
    wait_disconnected(ClientIds),
    Result.

connect_client(Run, I, Port) ->
    ClientId = iolist_to_binary(io_lib:format("mem-~2..0B-~6..0B", [Run, I])),
    Username = <<"untrusted-username-000000000000">>,
    UserProperties = [
        {<<"tns">>, <<"tenant-000000001">>},
        {<<"attr_a">>, binary:copy(<<"a">>, 96)},
        {<<"attr_b">>, binary:copy(<<"b">>, 96)},
        {<<"untrusted">>, binary:copy(<<"u">>, 96)}
    ],
    {ok, Client} = emqtt:start_link([
        {host, "127.0.0.1"},
        {port, Port},
        {proto_ver, v5},
        {clientid, ClientId},
        {username, Username},
        {properties, #{'User-Property' => UserProperties}}
    ]),
    {ok, _} = emqtt:connect(Client),
    {ClientId, Client}.

subscribe(Client) ->
    {ok, _, [0]} = emqtt:subscribe(Client, <<"bench/topic">>, 0),
    ok.

publish(Client) ->
    {ok, _} = emqtt:publish(Client, <<"bench/publish">>, <<"payload">>, 1),
    ok.

channel_pid(ClientId) ->
    [Pid] = emqx_cm:lookup_channels(ClientId),
    Pid.

stable_gc(Pids) ->
    [true = erlang:garbage_collect(Pid) || Pid <- Pids],
    timer:sleep(500),
    [true = erlang:garbage_collect(Pid) || Pid <- Pids],
    ok.

process_measurement(Pid) ->
    Info = maps:from_list(
        process_info(Pid, [
            memory,
            total_heap_size,
            heap_size,
            stack_size,
            message_queue_len,
            binary
        ])
    ),
    #{
        memory => maps:get(memory, Info),
        total_heap_size => maps:get(total_heap_size, Info),
        heap_size => maps:get(heap_size, Info),
        stack_size => maps:get(stack_size, Info),
        message_queue_len => maps:get(message_queue_len, Info),
        off_heap_binary_bytes => lists:sum([
            Size
         || {_Binary, Size, _Refs} <- maps:get(binary, Info)
        ])
    }.

summarize_processes(Measurements) ->
    maps:from_list([
        {Key, summarize([maps:get(Key, Measurement) || Measurement <- Measurements])}
     || Key <- [
            memory,
            total_heap_size,
            heap_size,
            stack_size,
            message_queue_len,
            off_heap_binary_bytes
        ]
    ]).

summarize(Values) ->
    Sorted = lists:sort(Values),
    Count = length(Sorted),
    Sum = lists:sum(Sorted),
    #{
        min => hd(Sorted),
        median => lists:nth((Count + 1) div 2, Sorted),
        p95 => lists:nth(max(1, (Count * 95 + 99) div 100), Sorted),
        max => lists:last(Sorted),
        mean => Sum / Count
    }.

term_measurement(Term) ->
    WordSize = erlang:system_info(wordsize),
    #{
        shared_heap_bytes => erts_debug:size(Term) * WordSize,
        flat_heap_bytes => erts_debug:flat_size(Term) * WordSize,
        external_binary_bytes => external_binary_bytes(Term),
        encoded_bytes => byte_size(term_to_binary(Term))
    }.

external_binary_bytes(Term) ->
    Binaries = collect_binaries(Term, #{}),
    maps:fold(
        fun(_Binary, ReferencedSize, Acc) -> Acc + ReferencedSize end,
        0,
        Binaries
    ).

collect_binaries(Binary, Acc) when is_binary(Binary) ->
    Acc#{Binary => binary:referenced_byte_size(Binary)};
collect_binaries(Map, Acc) when is_map(Map) ->
    maps:fold(
        fun(Key, Value, Acc0) ->
            collect_binaries(Value, collect_binaries(Key, Acc0))
        end,
        Acc,
        Map
    );
collect_binaries([Head | Tail], Acc) ->
    collect_binaries(Tail, collect_binaries(Head, Acc));
collect_binaries([], Acc) ->
    Acc;
collect_binaries(Tuple, Acc) when is_tuple(Tuple) ->
    collect_binaries(tuple_to_list(Tuple), Acc);
collect_binaries(_Term, Acc) ->
    Acc.

find_tagged_tuple(Tag, Tuple) when is_tuple(Tuple), element(1, Tuple) =:= Tag ->
    Tuple;
find_tagged_tuple(Tag, Tuple) when is_tuple(Tuple) ->
    find_tagged_tuple(Tag, tuple_to_list(Tuple));
find_tagged_tuple(Tag, [Head | Tail]) ->
    case find_tagged_tuple(Tag, Head) of
        undefined -> find_tagged_tuple(Tag, Tail);
        Found -> Found
    end;
find_tagged_tuple(_Tag, []) ->
    undefined;
find_tagged_tuple(_Tag, _Term) ->
    undefined.

wait_disconnected(ClientIds) ->
    lists:foreach(
        fun(ClientId) ->
            wait_disconnected(ClientId, 100)
        end,
        ClientIds
    ).

wait_disconnected(_ClientId, 0) ->
    error(channel_not_disconnected);
wait_disconnected(ClientId, Attempts) ->
    case emqx_cm:lookup_channels(ClientId) of
        [] ->
            ok;
        _ ->
            timer:sleep(20),
            wait_disconnected(ClientId, Attempts - 1)
    end.

configure_scenario(Scenario) ->
    case implementation_stage() of
        final ->
            RequireTrusted = Scenario =/= disabled,
            emqx_config:put([authorization, require_trusted_attributes], RequireTrusted),
            emqx_config:put([mqtt, require_trusted_attributes], RequireTrusted),
            emqx_config:put([multi_tenancy, require_trusted_attributes], RequireTrusted);
        _ ->
            ok
    end.

trusted_mask() ->
    case scenario() of
        disabled ->
            #{clientid => true};
        clientid_only ->
            #{clientid => true};
        several_attrs ->
            #{
                clientid => true,
                client_attrs => #{
                    <<"tns">> => true,
                    <<"attr_a">> => true,
                    <<"attr_b">> => true
                }
            }
    end.

scenario() ->
    case os:getenv("EMQX_CLIENTINFO_MEMORY_SCENARIO") of
        "disabled" -> disabled;
        "clientid_only" -> clientid_only;
        "several_attrs" -> several_attrs
    end.

implementation_stage() ->
    case code:ensure_loaded(emqx_clientinfo) of
        {module, emqx_clientinfo} ->
            case erlang:function_exported(emqx_clientinfo, merge_authn_result, 3) of
                true -> final;
                false -> core
            end;
        _ ->
            baseline
    end.

env_integer(Name, Default) ->
    case os:getenv(Name) of
        false -> Default;
        Value -> list_to_integer(Value)
    end.
