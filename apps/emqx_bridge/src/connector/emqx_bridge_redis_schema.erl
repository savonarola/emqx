-module(emqx_bridge_redis_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2]).

-export([roots/0, fields/1, desc/1]).

%%======================================================================================
%% Hocon Schema Definitions
roots() -> [].

fields("ingress") ->
    [emqx_bridge_schema:direction_field(ingress, emqx_connector_mqtt_schema:ingress_desc())] ++
        emqx_bridge_schema:common_bridge_fields(emqx_bridge_connector_redis_schema:connector_type()) ++
        emqx_bridge_connector_redis_schema:fields("ingress");
fields("egress") ->
    [emqx_bridge_schema:direction_field(egress, emqx_connector_mqtt_schema:egress_desc())] ++
        emqx_bridge_schema:common_bridge_fields(emqx_bridge_connector_redis_schema:connector_type()) ++
        emqx_bridge_connector_redis_schema:fields("egress");
fields("post_ingress") ->
    [
        type_field(),
        name_field()
    ] ++ proplists:delete(enable, fields("ingress"));
fields("post_egress") ->
    [
        type_field(),
        name_field()
    ] ++ proplists:delete(enable, fields("egress"));
fields("put_ingress") ->
    proplists:delete(enable, fields("ingress"));
fields("put_egress") ->
    proplists:delete(enable, fields("egress"));
fields("get_ingress") ->
    emqx_bridge_schema:metrics_status_fields() ++ fields("post_ingress");
fields("get_egress") ->
    emqx_bridge_schema:metrics_status_fields() ++ fields("post_egress").

desc(_) ->
    undefined.

%%======================================================================================
type_field() ->
    {type,
        mk(
            redis,
            #{
                required => true,
                %% ?DESC("desc_type")
                desc => ""
            }
        )}.

name_field() ->
    {name,
        mk(
            binary(),
            #{
                required => true,
                % ?DESC("desc_name")
                desc => ""
            }
        )}.
