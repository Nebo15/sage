defmodule Sage.Mixfile do
  use Mix.Project

  @version "0.6.0"

  def project do
    [
      app: :sage,
      version: @version,
      elixir: "~> 1.7",
      elixirc_paths: elixirc_paths(Mix.env()),
      compilers: [] ++ Mix.compilers(),
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      deps: deps(),

      # Hex
      description: "Sagas pattern implementation for distributed or long lived transactions and their error handling.",
      package: package(),

      # Docs
      name: "Sage",
      docs: docs(),

      # Custom testing
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [coveralls: :test],
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

  defp deps do
    [
      {:ex_doc, ">= 0.16.0", only: [:dev, :test]},
      {:excoveralls, ">= 0.7.0", only: [:dev, :test]},
      {:dialyxir, "~> 0.5", only: [:dev, :test], runtime: false},
      {:inch_ex, ">= 0.0.0", only: :test}
    ]
  end

  defp package do
    [
      contributors: ["Nebo #15"],
      maintainers: ["Nebo #15"],
      source_ref: "v#{@version}",
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/Nebo15/sage"},
      files: ~w(mix.exs .formatter.exs lib LICENSE.md README.md)
    ]
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#\{@version\}",
      source_url: "https://github.com/Nebo15/sage",
      extras: ["README.md"]
    ]
  end
end
