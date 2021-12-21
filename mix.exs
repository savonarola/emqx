defmodule EMQXBundle.MixProject do
  use Mix.Project

  System.version()
  |> Version.parse!()
  |> Version.compare(Version.parse!("1.13.2"))
  |> Kernel.==(:lt)
  |> if(do: Code.require_file("lib/mix/release.exs"))

  def project do
    [
      app: :emqx_bundle,
      version: pkg_vsn(),
      deps: deps(),
      releases: releases()
    ]
  end

  def pkg_vsn() do
    project_path()
    |> Path.join("pkg-vsn.sh")
    |> System.cmd([])
    |> elem(0)
    |> String.trim()
    |> String.split("-")
    |> Enum.reverse()
    |> tl()
    |> Enum.reverse()
    |> fix_vsn()
    |> Enum.join("-")
  end

  def project_path() do
    Path.expand("..", __ENV__.file)
  end

  defp fix_vsn([vsn | extras]) do
    if Version.parse(vsn) == :error do
      [vsn <> ".0" | extras]
    else
      [vsn | extras]
    end
  end

  defp deps do
    [
      # EMQX
      {:emqx, path: "apps/emqx", override: true},

      {:emqx_authn, path: "apps/emqx_authn", override: true},
      {:emqx_connector, path: "apps/emqx_connector", override: true},
      {:emqx_authz, path: "apps/emqx_authz", override: true},
      {:emqx_auto_subscribe, path: "apps/emqx_auto_subscribe", override: true},
      {:emqx_bridge, path: "apps/emqx_bridge", override: true},
      {:emqx_conf, path: "apps/emqx_conf", override: true},
      {:emqx_dashboard, path: "apps/emqx_dashboard", override: true},
      {:emqx_exhook, path: "apps/emqx_exhook", override: true},
      {:emqx_gateway, path: "apps/emqx_gateway", override: true},
      {:emqx_management, path: "apps/emqx_management", override: true},
      {:emqx_modules, path: "apps/emqx_modules", override: true},
      {:emqx_plugins, path: "apps/emqx_plugins", override: true},
      {:emqx_plugin_libs, path: "apps/emqx_plugin_libs", override: true},
      {:emqx_prometheus, path: "apps/emqx_prometheus", override: true},
      {:emqx_psk, path: "apps/emqx_psk", override: true},
      {:emqx_resource, path: "apps/emqx_resource", override: true},
      {:emqx_retainer, path: "apps/emqx_retainer", override: true},
      {:emqx_rule_engine, path: "apps/emqx_rule_engine", override: true},
      {:emqx_slow_subs, path: "apps/emqx_slow_subs", override: true},
      {:emqx_statsd, path: "apps/emqx_statsd", override: true},

      {:emqx_machine, path: "apps/emqx_machine", override: true},

      # Conflicts
      {:cowboy, github: "emqx/cowboy", tag: "2.9.0", override: true},
      {:cowlib, "2.8.0", override: true},
      {:pbkdf2, github: "emqx/erlang-pbkdf2", tag: "2.0.4", override: true},
      {:snabbkaffe, github: "kafka4beam/snabbkaffe", tag: "0.16.0", override: true},
      {:quicer, github: "emqx/quic", tag: "0.0.9", override: true},
      {:gun, github: "emqx/gun", tag: "1.3.4", override: true},
      {:getopt, "1.0.2", override: true},
      {:ranch, github: "ninenines/ranch", tag: "1.8.0", override: true},
      {:gproc, "0.8.0", override: true},
      {:jsx, "2.9.0", override: true},
      {:typerefl, github: "k32/typerefl", tag: "0.8.5", override: true},
      {:epgsql, github: "emqx/epgsql", tag: "4.6.0", override: true},
      {:recon, github: "ferd/recon", tag: "2.5.1", override: true},



      {:mria, github: "emqx/mria", tag: "0.1.5", override: true},
      {:ekka, github: "emqx/ekka", tag: "0.11.1", override: true},
      {:gen_rpc, github: "emqx/gen_rpc", tag: "2.5.1", override: true},
      {:esasl, github: "emqx/esasl", tag: "0.2.0", override: true},


      {:hocon, github: "emqx/hocon", tag: "0.22.0"},

      # Standalone deps
      {:ehttpc, github: "emqx/ehttpc", tag: "0.1.12"},
      {:ecpool, github: "emqx/ecpool", tag: "0.5.1"},
      {:rulesql, github: "emqx/rulesql", tag: "0.1.4"},
      {:observer_cli, "1.7.1"},
      {:system_monitor, github: "klarna-incubator/system_monitor", tag: "2.2.0"},
      {:emqx_http_lib, github: "emqx/emqx_http_lib", tag: "0.4.1"}
    ]
  end

    defp releases do
    [
      emqx: [
        applications: [
          logger: :permanent,
          esasl: :load,
          crypto: :permanent,
          public_key: :permanent,
          asn1: :permanent,
          syntax_tools: :permanent,
          ssl: :permanent,
          os_mon: :permanent,
          inets: :permanent,
          compiler: :permanent,
          runtime_tools: :permanent,
          hocon: :load,
          emqx: :load,
          emqx_conf: :load,
          emqx_machine: :permanent,
          mria: :load,
          mnesia: :load,
          ekka: :load,
          emqx_plugin_libs: :load,
          emqx_http_lib: :permanent,
          emqx_resource: :permanent,
          emqx_connector: :permanent,
          emqx_authn: :permanent,
          emqx_authz: :permanent,
          emqx_auto_subscribe: :permanent,
          emqx_gateway: :permanent,
          emqx_exhook: :permanent,
          emqx_bridge: :permanent,
          emqx_rule_engine: :permanent,
          emqx_modules: :permanent,
          emqx_management: :permanent,
          emqx_dashboard: :permanent,
          emqx_statsd: :permanent,
          emqx_retainer: :permanent,
          emqx_prometheus: :permanent,
          emqx_psk: :permanent,
          emqx_slow_subs: :permanent,
          emqx_plugins: :permanent,
          emqx_bundle: :load
        ],
        skip_mode_validation_for: [
          :emqx_gateway,
          :emqx_dashboard,
          :emqx_resource,
          :emqx_connector,
          :emqx_exhook,
          :emqx_bridge,
          :emqx_modules,
          :emqx_management,
          :emqx_statsd,
          :emqx_retainer,
          :emqx_prometheus,
          :emqx_plugins
        ],
        steps: [:assemble, &copy_files/1]
      ]
    ]
  end

  def copy_files(release) do
    etc = Path.join(release.path, "etc")

    # FIXME: Remove??
    File.mkdir_p!(etc)
    File.cp!("apps/emqx_authz/etc/acl.conf", Path.join(etc, "acl.conf"))
    File.cp_r!("apps/emqx/etc/certs", Path.join(etc, "certs"))

    release
  end
end
