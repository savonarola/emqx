defmodule Mix.Tasks.Compile.RustNif do
  use Mix.Task.Compiler

  @shortdoc "Compile Rust NIF and install .so into priv/"

  @moduledoc """
  Compiles a Rust NIF for apps that have a `Cargo.toml` in their root directory.

  The compiler is a no-op for apps without `Cargo.toml`.
  """

  @recursive true

  @impl true
  def run(_args) do
    if File.exists?("Cargo.toml") do
      build_nif()
    else
      {:noop, []}
    end
  end

  defp build_nif do
    app_root = File.cwd!()

    # Use debug for test profiles, release otherwise.
    {cargo_profile, cargo_flags} =
      case System.get_env("PROFILE") do
        "emqx-enterprise-test" -> {"debug", []}
        _ -> {"release", ["--release"]}
      end

    Mix.shell().info("Compiling Rust NIF (#{cargo_profile}) in #{app_root}")

    case System.cmd("cargo", ["build" | cargo_flags], cd: app_root, stderr_to_stdout: true) do
      {output, 0} ->
        Mix.shell().info(output)
        priv_dir = Path.join(app_root, "priv")
        File.mkdir_p!(priv_dir)

        Path.join([app_root, "target", cargo_profile, "*_nif.so"])
        |> Path.wildcard()
        |> Enum.each(fn so ->
          dest = Path.join(priv_dir, Path.basename(so))
          File.cp!(so, dest)
          Mix.shell().info("NIF installed: #{dest}")
        end)
        {:ok, []}

      {output, exit_code} ->
        Mix.shell().error("Rust NIF build failed (exit #{exit_code}):\n#{output}")
        {:error, []}
    end
  end
end
