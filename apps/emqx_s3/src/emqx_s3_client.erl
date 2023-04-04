%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_s3_client).

-include_lib("emqx/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("erlcloud/include/erlcloud_aws.hrl").

-export([
    create/1,

    put_object/3,
    put_object/4,

    start_multipart/2,
    start_multipart/3,
    upload_part/5,
    complete_multipart/4,
    abort_multipart/3,
    list/2,
    uri/2,

    format/1,
    format_request/1
]).

-export_type([
    client/0,
    headers/0
]).

-type headers() :: #{binary() | string() => iodata()}.

-type key() :: string().
-type part_number() :: non_neg_integer().
-type upload_id() :: string().
-type etag() :: string().

-type upload_options() :: list({acl, emqx_s3:acl()}).

-opaque client() :: #{
    aws_config := aws_config(),
    options := upload_options(),
    bucket := string(),
    headers := headers()
}.

-type config() :: #{
    scheme := string(),
    host := string(),
    port := part_number(),
    bucket := string(),
    headers := headers(),
    acl := emqx_s3:acl(),
    url_expire_time := pos_integer(),
    access_key_id := string() | undefined,
    secret_access_key := string() | undefined,
    http_pool := ehttpc:pool_name(),
    request_timeout := timeout()
}.

-type s3_options() :: list({string(), string()}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec create(config()) -> client().
create(Config) ->
    #{
        aws_config => aws_config(Config),
        upload_options => upload_options(Config),
        bucket => maps:get(bucket, Config),
        url_expire_time => maps:get(url_expire_time, Config),
        headers => headers(Config)
    }.

-spec put_object(client(), key(), iodata()) -> ok_or_error(term()).
put_object(Client, Key, Value) ->
    put_object(Client, #{}, Key, Value).

-spec put_object(client(), headers(), key(), iodata()) -> ok_or_error(term()).
put_object(
    #{bucket := Bucket, upload_options := Options, headers := Headers, aws_config := AwsConfig},
    SpecialHeaders,
    Key,
    Value
) ->
    AllHeaders = join_headers(Headers, SpecialHeaders),
    try erlcloud_s3:put_object(Bucket, erlcloud_key(Key), Value, Options, AllHeaders, AwsConfig) of
        Props when is_list(Props) ->
            ok
    catch
        error:{aws_error, Reason} ->
            ?SLOG(debug, #{msg => "put_object_fail", key => Key, reason => Reason}),
            {error, Reason}
    end.

-spec start_multipart(client(), key()) -> ok_or_error(upload_id(), term()).
start_multipart(Client, Key) ->
    start_multipart(Client, #{}, Key).

-spec start_multipart(client(), headers(), key()) -> ok_or_error(upload_id(), term()).
start_multipart(
    #{bucket := Bucket, upload_options := Options, headers := Headers, aws_config := AwsConfig},
    SpecialHeaders,
    Key
) ->
    AllHeaders = join_headers(Headers, SpecialHeaders),
    case erlcloud_s3:start_multipart(Bucket, erlcloud_key(Key), Options, AllHeaders, AwsConfig) of
        {ok, Props} ->
            {ok, response_property('uploadId', Props)};
        {error, Reason} ->
            ?SLOG(debug, #{msg => "start_multipart_fail", key => Key, reason => Reason}),
            {error, Reason}
    end.

-spec upload_part(client(), key(), upload_id(), part_number(), iodata()) ->
    ok_or_error(etag(), term()).
upload_part(
    #{bucket := Bucket, headers := Headers, aws_config := AwsConfig},
    Key,
    UploadId,
    PartNumber,
    Value
) ->
    case
        erlcloud_s3:upload_part(
            Bucket, erlcloud_key(Key), UploadId, PartNumber, Value, Headers, AwsConfig
        )
    of
        {ok, Props} ->
            {ok, response_property(etag, Props)};
        {error, Reason} ->
            ?SLOG(debug, #{msg => "upload_part_fail", key => Key, reason => Reason}),
            {error, Reason}
    end.

-spec complete_multipart(client(), key(), upload_id(), [etag()]) -> ok_or_error(term()).
complete_multipart(
    #{bucket := Bucket, headers := Headers, aws_config := AwsConfig},
    Key,
    UploadId,
    ETags
) ->
    case
        erlcloud_s3:complete_multipart(
            Bucket, erlcloud_key(Key), UploadId, ETags, Headers, AwsConfig
        )
    of
        ok ->
            ok;
        {error, Reason} ->
            ?SLOG(debug, #{msg => "complete_multipart_fail", key => Key, reason => Reason}),
            {error, Reason}
    end.

-spec abort_multipart(client(), key(), upload_id()) -> ok_or_error(term()).
abort_multipart(#{bucket := Bucket, headers := Headers, aws_config := AwsConfig}, Key, UploadId) ->
    case erlcloud_s3:abort_multipart(Bucket, erlcloud_key(Key), UploadId, [], Headers, AwsConfig) of
        ok ->
            ok;
        {error, Reason} ->
            ?SLOG(debug, #{msg => "abort_multipart_fail", key => Key, reason => Reason}),
            {error, Reason}
    end.

-spec list(client(), s3_options()) -> ok_or_error(term()).
list(#{bucket := Bucket, aws_config := AwsConfig}, Options) ->
    try
        {ok, erlcloud_s3:list_objects(Bucket, Options, AwsConfig)}
    catch
        error:{aws_error, Reason} ->
            ?SLOG(debug, #{msg => "list_objects_fail", bucket => Bucket, reason => Reason}),
            {error, Reason}
    end.

-spec uri(client(), key()) -> iodata().
uri(#{bucket := Bucket, aws_config := AwsConfig, url_expire_time := ExpireTime}, Key) ->
    erlcloud_s3:make_get_url(ExpireTime, Bucket, erlcloud_key(Key), AwsConfig).

-spec format(client()) -> term().
format(#{aws_config := AwsConfig} = Client) ->
    Client#{aws_config => AwsConfig#aws_config{secret_access_key = "***"}}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

upload_options(Config) ->
    [
        {acl, maps:get(acl, Config)}
    ].

headers(#{headers := Headers}) ->
    headers_user_to_erlcloud_request(Headers);
headers(#{}) ->
    [].

aws_config(#{
    scheme := Scheme,
    host := Host,
    port := Port,
    access_key_id := AccessKeyId,
    secret_access_key := SecretAccessKey,
    http_pool := HttpPool,
    request_timeout := Timeout
}) ->
    #aws_config{
        s3_scheme = Scheme,
        s3_host = Host,
        s3_port = Port,
        s3_bucket_access_method = path,
        s3_bucket_after_host = true,

        access_key_id = AccessKeyId,
        secret_access_key = SecretAccessKey,

        http_client = request_fun(HttpPool),
        timeout = Timeout
    }.

-type http_pool() :: term().

-spec request_fun(http_pool()) -> erlcloud_httpc:request_fun().
request_fun(HttpPool) ->
    fun(Url, Method, Headers, Body, Timeout, _Config) ->
        with_path_and_query_only(Url, fun(PathQuery) ->
            Request = make_request(
                Method, PathQuery, headers_erlcloud_request_to_ehttpc(Headers), Body
            ),
            ?SLOG(debug, #{
                msg => "s3_ehttpc_request",
                timeout => Timeout,
                pool => HttpPool,
                method => Method,
                request => Request
            }),
            ehttpc_request(HttpPool, Method, Request, Timeout)
        end)
    end.

ehttpc_request(HttpPool, Method, Request, Timeout) ->
    ?SLOG(debug, #{
        msg => "s3_ehttpc_request",
        timeout => Timeout,
        pool => HttpPool,
        method => Method,
        request => format_request(Request)
    }),
    try ehttpc:request(HttpPool, Method, Request, Timeout) of
        {ok, StatusCode, RespHeaders} ->
            ?SLOG(debug, #{
                msg => "s3_ehttpc_request_ok",
                status_code => StatusCode,
                headers => RespHeaders
            }),
            {ok, {
                {StatusCode, undefined}, headers_ehttpc_to_erlcloud_response(RespHeaders), undefined
            }};
        {ok, StatusCode, RespHeaders, RespBody} ->
            ?SLOG(debug, #{
                msg => "s3_ehttpc_request_ok",
                status_code => StatusCode,
                headers => RespHeaders,
                body => RespBody
            }),
            {ok, {
                {StatusCode, undefined}, headers_ehttpc_to_erlcloud_response(RespHeaders), RespBody
            }};
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "s3_ehttpc_request_fail",
                reason => Reason,
                timeout => Timeout,
                pool => HttpPool,
                method => Method
            }),
            {error, Reason}
    catch
        error:badarg ->
            ?SLOG(error, #{
                msg => "s3_ehttpc_request_fail",
                reason => badarg,
                timeout => Timeout,
                pool => HttpPool,
                method => Method
            }),
            {error, no_ehttpc_pool};
        error:Reason ->
            ?SLOG(error, #{
                msg => "s3_ehttpc_request_fail",
                reason => Reason,
                timeout => Timeout,
                pool => HttpPool,
                method => Method
            }),
            {error, Reason}
    end.

-define(IS_BODY_EMPTY(Body), (Body =:= undefined orelse Body =:= <<>>)).
-define(NEEDS_NO_BODY(Method), (Method =:= get orelse Method =:= head orelse Method =:= delete)).

make_request(Method, PathQuery, Headers, Body) when
    ?IS_BODY_EMPTY(Body) andalso ?NEEDS_NO_BODY(Method)
->
    {PathQuery, Headers};
make_request(_Method, PathQuery, Headers, Body) when ?IS_BODY_EMPTY(Body) ->
    {PathQuery, [{<<"content-length">>, <<"0">>} | Headers], <<>>};
make_request(_Method, PathQuery, Headers, Body) ->
    {PathQuery, Headers, Body}.

format_request({PathQuery, Headers, _Body}) -> {PathQuery, Headers, <<"...">>};
format_request({PathQuery, Headers}) -> {PathQuery, Headers}.

with_path_and_query_only(Url, Fun) ->
    case string:split(Url, "//", leading) of
        [_Scheme, UrlRem] ->
            case string:split(UrlRem, "/", leading) of
                [_HostPort, PathQuery] ->
                    Fun([$/ | PathQuery]);
                _ ->
                    {error, {invalid_url, Url}}
            end;
        _ ->
            {error, {invalid_url, Url}}
    end.

%% We need some header conversions to tie the emqx_s3, erlcloud and ehttpc APIs together.

%% The request header flow is:

%% UserHeaders -> [emqx_s3_client API] -> ErlcloudRequestHeaders0 ->
%% -> [erlcloud API] -> ErlcloudRequestHeaders1 -> [emqx_s3_client injected request_fun] ->
%% -> EhttpcRequestHeaders -> [ehttpc API]

%% The response header flow is:

%% [ehttpc API] -> EhttpcResponseHeaders -> [emqx_s3_client injected request_fun] ->
%% -> ErlcloudResponseHeaders0 -> [erlcloud API] -> [emqx_s3_client API]

%% UserHeders (emqx_s3 API headers) are maps with string/binary keys.
%% ErlcloudRequestHeaders are lists of tuples with string keys and iodata values
%% ErlcloudResponseHeders are lists of tuples with lower case string keys and iodata values.
%% EhttpcHeaders are lists of tuples with binary keys and iodata values.

%% Users provide headers as a map, but erlcloud expects a list of tuples with string keys and values.
headers_user_to_erlcloud_request(UserHeaders) ->
    [{to_list_string(K), V} || {K, V} <- maps:to_list(UserHeaders)].

%% Ehttpc returns operates on headers as a list of tuples with binary keys.
%% Erlcloud expects a list of tuples with string values and lowcase string keys
%% from the underlying http library.
headers_ehttpc_to_erlcloud_response(EhttpcHeaders) ->
    [{string:to_lower(to_list_string(K)), to_list_string(V)} || {K, V} <- EhttpcHeaders].

%% Ehttpc expects a list of tuples with binary keys.
%% Erlcloud provides a list of tuples with string keys.
headers_erlcloud_request_to_ehttpc(ErlcloudHeaders) ->
    [{to_binary(K), V} || {K, V} <- ErlcloudHeaders].

join_headers(ErlcloudHeaders, UserSpecialHeaders) ->
    ErlcloudHeaders ++ headers_user_to_erlcloud_request(UserSpecialHeaders).

to_binary(Val) when is_list(Val) -> list_to_binary(Val);
to_binary(Val) when is_binary(Val) -> Val.

to_list_string(Val) when is_binary(Val) ->
    binary_to_list(Val);
to_list_string(Val) when is_list(Val) ->
    Val.

erlcloud_key(Characters) ->
    binary_to_list(unicode:characters_to_binary(Characters)).

response_property(Name, Props) ->
    case proplists:get_value(Name, Props) of
        undefined ->
            %% This schould not happen for valid S3 implementations
            ?SLOG(error, #{
                msg => "missing_s3_response_property",
                name => Name,
                props => Props
            }),
            error({missing_s3_response_property, Name});
        Value ->
            Value
    end.