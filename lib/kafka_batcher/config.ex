defmodule KafkaBatcher.Config do
  @moduledoc """
  KafkaBatcher configuration processing.
  All config parameters are described in details in README.md
  Examples of configs can be found in the files config/test.exs and test/support/collectors/collector_handlers.ex
  """

  defmodule CollectorMissingError do
    defexception [:message]

    def exception(term) do
      %CollectorMissingError{message: "Collector #{inspect(term)} missing"}
    end
  end

  defmodule BadConfigError do
    defexception [:message]

    def exception(term) do
      %BadConfigError{message: "Topic config error #{inspect(term)}"}
    end
  end

  alias KafkaBatcher.{
    Accumulator,
    Collector,
    PipelineUnit.Validator,
    Producers
  }

  @type t :: %__MODULE__{
          producer_config: Producers.Config.t(),
          pipeline_units: [KafkaBatcher.PipelineUnit.t()],
          kafka_topic_aliases: %{optional(binary()) => binary()},
          kafka_metric_opts: Keyword.t()
        }

  @enforce_keys [
    :producer_config,
    :pipeline_units,
    :kafka_topic_aliases,
    :kafka_metric_opts
  ]
  defstruct @enforce_keys

  @spec get_client_name(t()) :: atom()
  def get_client_name(%__MODULE__{} = config) do
    %__MODULE__{
      producer_config: %Producers.Config{client_name: client_name}
    } = config

    client_name
  end

  @spec build_config!(opts :: Keyword.t()) :: t()
  def build_config!(opts) do
    producer_config =
      opts
      |> Keyword.fetch!(:kafka)
      |> Producers.Config.build!()

    pipeline_units =
      for collector <- Keyword.fetch!(opts, :collectors) do
        producer_config
        |> build_pipeline_unit!(collector, opts)
        |> validate_pipeline_unit!()
      end

    %__MODULE__{
      producer_config: producer_config,
      pipeline_units: pipeline_units,
      kafka_topic_aliases: Keyword.get(opts, :kafka_topic_aliases, %{}),
      kafka_metric_opts: Keyword.get(opts, :kafka_metric_opts, [])
    }
  end

  defp build_pipeline_unit!(producer_config, collector, opts) do
    opts =
      opts
      |> Keyword.merge(get_compile_opts!(collector))
      |> Keyword.merge(Keyword.get(opts, collector, []))
      |> Keyword.put(:collector, collector)

    collector_config = Collector.Config.build!(opts)
    accumulator_config = Accumulator.Config.build!(opts)

    %KafkaBatcher.PipelineUnit{
      collector_config: collector_config,
      accumulator_config: accumulator_config,
      producer_config: producer_config,
      opts:
        collector_config
        |> Collector.Config.to_kwlist()
        |> Keyword.merge(Accumulator.Config.to_kwlist(accumulator_config))
        |> Keyword.merge(kafka: Producers.Config.to_kwlist(producer_config))
    }
  end

  defp validate_pipeline_unit!(pipeline_unit) do
    case Validator.validate(pipeline_unit) do
      :ok ->
        pipeline_unit

      {:error, reason} ->
        raise(BadConfigError, "Collector config failed: #{inspect(reason)}")
    end
  end

  defp get_compile_opts!(module) do
    with {:module, ^module} <- Code.ensure_compiled(module),
         {:ok, [_ | _] = opts} <- {:ok, module.get_compile_opts()} do
      opts
    else
      _ ->
        raise(CollectorMissingError, "Collector: #{inspect(module)} missing")
    end
  end
end
