defmodule KafkaBroadwaySimple.MixProject do
  use Mix.Project

  def project do
    [
      app: :kafka_broadway_simple,
      version: "0.1.0",
      elixir: "~> 1.8",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {KafkaBroadwaySimple.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:kafka_ex, "~> 0.9.0"},
      {:broadway, "~> 0.3.0"}
    ]
  end
end
