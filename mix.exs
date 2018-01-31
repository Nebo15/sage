defmodule Sage.Mixfile do
  use Mix.Project

  @version "0.3.2"

  def project do
    [
      app: :sage,
      description: "Sagas pattern implementation for distributed or long lived transactions and their error handling.",
      package: package(),
      version: @version,
      elixir: "~> 1.5",
      elixirc_paths: elixirc_paths(Mix.env()),
      compilers: [] ++ Mix.compilers(),
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [coveralls: :test],
      docs: [source_ref: "v#\{@version\}", main: "readme", extras: ["README.md"]],
      dialyzer: [ignore_warnings: "dialyzer.ignore-warnings"]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      extra_applications: [:logger],
      mod: {Sage, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_doc, ">= 0.16.0", only: [:dev, :test]},
      {:excoveralls, ">= 0.7.0", only: [:dev, :test]},
      {:dogma, "> 0.1.0", only: [:dev, :test]},
      {:credo, ">= 0.8.0", only: [:dev, :test]},
      {:dialyxir, "~> 0.5", only: [:dev, :test], runtime: false},
      {:inch_ex, ">= 0.0.0", only: :test}
    ]
  end

  defp package do
    [
      contributors: ["Nebo #15"],
      maintainers: ["Nebo #15"],
      licenses: ["MIT"],
      links: %{github: "https://github.com/Nebo15/annon.api"},
      files: ~w(lib LICENSE.md mix.exs README.md)
    ]
  end
end
