defmodule KafkaBatcher.Producers.Config.BrodConfig do
  defmodule SASLConfigError do
    defexception [:message]

    def exception(_term) do
      %SASLConfigError{message: "Sasl config error"}
    end
  end

  @moduledoc false

  @type sasl_mechanism() :: :plain | :scram_sha_256 | :scram_sha_512
  @type sasl() :: {sasl_mechanism(), binary(), binary()} | :undefined

  @type t :: %__MODULE__{
          allow_topic_auto_creation: boolean(),
          required_acks: integer(),
          ssl: boolean(),
          sasl: sasl()
        }

  @derive {Inspect, only: [:allow_topic_auto_creation, :required_acks, :ssl]}

  @enforce_keys [:allow_topic_auto_creation, :required_acks, :ssl, :sasl]
  defstruct @enforce_keys

  @spec drop_sensitive(t()) :: t()
  def drop_sensitive(%__MODULE__{} = config) do
    case config.sasl do
      :undefined -> config
      {mechanism, _, _} -> %__MODULE__{config | sasl: {mechanism, "", ""}}
    end
  end

  @spec to_kwlist(t()) :: Keyword.t()
  def to_kwlist(%__MODULE__{} = config) do
    [
      allow_topic_auto_creation: config.allow_topic_auto_creation,
      required_acks: config.required_acks,
      ssl: config.ssl,
      sasl: config.sasl
    ]
  end

  @spec build!(Keyword.t()) :: t()
  def build!(opts) do
    sasl_config =
      case validate_sasl_config(Keyword.get(opts, :sasl)) do
        {:ok, sasl_config} ->
          sasl_config

        {:error, {:invalid, config}} ->
          raise(SASLConfigError, "SASL config failed: #{inspect(config)}")
      end

    # https://github.com/kafka4beam/brod/blob/master/src/brod_producer.erl
    %__MODULE__{
      allow_topic_auto_creation: Keyword.get(opts, :allow_topic_auto_creation, false),
      required_acks: Keyword.get(opts, :required_acks, -1),
      ssl: Keyword.get(opts, :ssl, false),
      sasl: sasl_config
    }
  end

  @spec validate_sasl_config(map() | nil) ::
          {:ok, sasl()} | {:error, {:invalid, map()}}
  defp validate_sasl_config(
         %{
           mechanism: mechanism,
           login: login,
           password: password
         } = config
       ) do
    mechanism_valid? = mechanism in [:plain, :scram_sha_256, :scram_sha_512]

    if mechanism_valid? and password != nil and login != nil do
      {:ok, {mechanism, login, password}}
    else
      {:error, {:invalid, config}}
    end
  end

  defp validate_sasl_config(sasl_config)
       when sasl_config == nil or sasl_config == %{} do
    {:ok, :undefined}
  end

  defp validate_sasl_config(bad_sasl_config) do
    {:error, {:invalid, bad_sasl_config}}
  end
end
