defmodule KafkaBatcher.Producers.Config do
  @moduledoc false

  alias KafkaBatcher.{MessageObject, Producers.Config.BrodConfig}

  @typep topic :: String.t()
  @typep partition_count :: pos_integer()

  @type partition_fn ::
          (topic(), partition_count(), MessageObject.key(), MessageObject.value() ->
             pos_integer())

  @type partition_strategy :: :random | :md5 | partition_fn()

  @type t :: %__MODULE__{
          endpoints: [{String.t(), non_neg_integer()}],
          client_name: atom(),
          partition_strategy: partition_strategy() | nil,
          required_acks: integer(),
          telemetry: boolean(),
          brod_config: BrodConfig.t()
        }

  @enforce_keys [
    :endpoints,
    :client_name,
    :partition_strategy,
    :required_acks,
    :telemetry,
    :brod_config
  ]

  defstruct @enforce_keys

  @spec to_kwlist(t()) :: Keyword.t()
  def to_kwlist(%__MODULE__{} = config) do
    [
      endpoints: config.endpoints,
      client_name: config.client_name,
      partition_strategy: config.partition_strategy,
      required_acks: config.required_acks,
      telemetry: config.telemetry
    ] ++ BrodConfig.to_kwlist(config.brod_config)
  end

  @spec drop_sensitive(t()) :: t()
  def drop_sensitive(%__MODULE__{} = config) do
    %__MODULE__{
      config
      | brod_config: BrodConfig.drop_sensitive(config.brod_config)
    }
  end

  @spec build!(opts :: Keyword.t()) :: t()
  def build!(opts) do
    endpoints =
      for url <- opts |> Keyword.fetch!(:endpoints) |> String.split(",") do
        [host, port] = String.split(url, ":")
        {host, :erlang.binary_to_integer(port)}
      end

    %__MODULE__{
      endpoints: endpoints,
      client_name: Keyword.get(opts, :client_name, :kafka_producer_client),
      partition_strategy: Keyword.get(opts, :partition_strategy),
      required_acks: Keyword.get(opts, :required_acks, -1),
      telemetry: Keyword.get(opts, :telemetry, true),
      brod_config: BrodConfig.build!(opts)
    }
  end
end
