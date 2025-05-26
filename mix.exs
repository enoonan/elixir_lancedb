defmodule ElixirLanceDB.MixProject do
  use Mix.Project

  @version "0.1.1"
  @source_url "https://github.com/enoonan/elixir_lancedb"

  def project do
    [
      app: :elixir_lancedb,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {ElixirLanceDB.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:poolex, "~> 1.0"},
      {:ecto_sql, "~> 3.12.1"},
      {:rustler_precompiled, "~> 0.8.2", runtime: false},
      {:rustler, "~> 0.36.1", runtime: false}
    ]
  end

  defp package() do
    [
      files: [
        "lib",
        "native",
        "checksum-*.exs",
        "mix.exs"
      ],
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url}
    ]
  end
end
