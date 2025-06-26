defmodule KafkaBatcher.MixProject do
  use Mix.Project

  def project do
    [
      app: :kafka_batcher,
      version: "1.0.2",
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      compilers: Mix.compilers(),
      aliases: aliases(),
      deps: deps(),
      test_coverage: [
        tool: ExCoveralls
      ],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test,
        "coveralls.cobertura": :test
      ],
      dialyzer: [
        plt_add_deps: :app_tree,
        plt_add_apps: [:mix, :eex],
        flags: ~w[
          error_handling extra_return missing_return underspecs unmatched_returns
        ]a,
        list_unused_filters: true
      ],
      package: [
        maintainers: ["Roman Smirnov", "Dmitry Begunkov"],
        description:
          "Library to increase the throughput of producing messages (coming one at a time) to Kafka by accumulating these messages into batches",
        links: %{"Source" => "https://github.com/samokat-oss/kafka-batcher"},
        licenses: ["Apache-2.0"]
      ],
      docs: [
        before_closing_head_tag: &add_js_to_docs/1
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:kaffe, "~> 1.22"},
      {:kafka_ex, "~> 0.12", optional: true},
      {:jason, ">= 0.0.0"},
      {:prom_ex, ">= 0.0.0", optional: true},
      {:telemetry, "~> 1.0"},
      {:brod, "~> 3.16", override: true, only: [:dev, :test]},
      # For tests and code quality
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.0", only: [:dev, :test], runtime: false},
      {:uniq, "~> 0.1"},
      {:mox, "~> 1.0", only: :test},
      {:excoveralls, "~> 0.16", only: :test},
      {:ex_doc, "~> 0.30", only: :dev, runtime: false},
      {:makeup_html, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp aliases do
    [
      "test.coverage": ["coveralls.cobertura"]
    ]
  end

  defp add_js_to_docs(:epub), do: ""

  defp add_js_to_docs(:html) do
    """
    <script src="https://cdn.jsdelivr.net/npm/mermaid@10.2.3/dist/mermaid.min.js"></script>
    <script>
      document.addEventListener("DOMContentLoaded", function () {
        mermaid.initialize({
          startOnLoad: false,
          theme: document.body.className.includes("dark") ? "dark" : "default"
        });
        let id = 0;
        for (const codeEl of document.querySelectorAll("pre code.mermaid")) {
          const preEl = codeEl.parentElement;
          const graphDefinition = codeEl.textContent;
          const graphEl = document.createElement("div");
          const graphId = "mermaid-graph-" + id++;
          mermaid.render(graphId, graphDefinition).then(({svg, bindFunctions}) => {
            graphEl.innerHTML = svg;
            bindFunctions?.(graphEl);
            preEl.insertAdjacentElement("afterend", graphEl);
            preEl.remove();
          });
        }
      });
    </script>
    """
  end
end
