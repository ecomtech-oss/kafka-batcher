defmodule KafkaBatcher.Supervisor do
  @moduledoc """
  The root of KafkaBatcher supervision tree
  Starts Collector & AccumulatorsPoolSupervisor for each configured collector
  """

  alias KafkaBatcher.{AccumulatorsPoolSupervisor, Collector}

  use Supervisor

  @spec child_spec(
          KafkaBatcher.Config.t()
          | Keyword.t()
          | nil
        ) :: Supervisor.child_spec()
  def child_spec(%KafkaBatcher.Config{} = config) do
    %{id: reg_name(config), start: {__MODULE__, :start_link, [config]}}
  end

  def child_spec(opts) when is_nil(opts) or is_list(opts) do
    opts =
      if is_nil(opts) or opts == [] do
        # Backwards compatibility
        Application.get_all_env(:kafka_batcher)
      else
        opts
      end

    opts |> KafkaBatcher.Config.build_config!() |> child_spec()
  end

  @spec start_link(KafkaBatcher.Config.t()) :: Supervisor.on_start()
  def start_link(%KafkaBatcher.Config{} = config) do
    Supervisor.start_link(__MODULE__, config, name: reg_name(config))
  end

  @impl true
  def init(%KafkaBatcher.Config{} = config) do
    children =
      [
        KafkaBatcher.ConnectionManager.child_spec(config)
        | build_data_stream_specs(config.data_stream_specs)
      ]
      |> Enum.reverse()

    opts = [strategy: :one_for_one]
    Supervisor.init(children, opts)
  end

  defp build_data_stream_specs(specs) do
    for %KafkaBatcher.DataStreamSpec{} = spec <- specs, reduce: [] do
      specs ->
        %KafkaBatcher.DataStreamSpec{
          collector_config: %Collector.Config{collector_mod: collector_mod}
        } = spec

        [
          collector_mod.child_spec(spec),
          AccumulatorsPoolSupervisor.child_spec(spec)
          | specs
        ]
    end
  end

  defp reg_name(%KafkaBatcher.Config{} = config) do
    :"#{__MODULE__}.#{KafkaBatcher.Config.get_client_name(config)}"
  end
end
