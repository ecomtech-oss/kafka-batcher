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
    DataStreamSpec.Validator,
    Producers
  }

  @type t :: %__MODULE__{
          producer_config: Producers.Config.t(),
          data_stream_specs: [KafkaBatcher.DataStreamSpec.t()],
          kafka_topic_aliases: %{optional(binary()) => binary()},
          kafka_metric_opts: Keyword.t()
        }

  @enforce_keys [
    :producer_config,
    :data_stream_specs,
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
      opts |> Keyword.get(:kafka, []) |> Producers.Config.build!()

    data_stream_specs =
      for collector_mod <- Keyword.fetch!(opts, :collectors) do
        producer_config
        |> build_data_stream_spec!(collector_mod, opts)
        |> validate_data_stream_spec!()
      end

    %__MODULE__{
      producer_config: producer_config,
      data_stream_specs: data_stream_specs,
      kafka_topic_aliases: Keyword.get(opts, :kafka_topic_aliases, %{}),
      kafka_metric_opts: Keyword.get(opts, :kafka_metric_opts, [])
    }
  end

  defp build_data_stream_spec!(producer_config, collector_mod, opts) do
    opts =
      opts
      |> Keyword.merge(get_compile_opts!(collector_mod))
      |> Keyword.merge(Keyword.get(opts, collector_mod, []))
      |> Keyword.put(:collector_mod, collector_mod)

    collector_config = Collector.Config.build!(opts)
    accumulator_config = Accumulator.Config.build!(opts)

    %KafkaBatcher.DataStreamSpec{
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

  defp validate_data_stream_spec!(data_stream_spec) do
    case Validator.validate(data_stream_spec) do
      :ok ->
        data_stream_spec

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
