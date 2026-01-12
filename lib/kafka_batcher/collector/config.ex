defmodule KafkaBatcher.Collector.Config do
  @moduledoc false

  alias KafkaBatcher.Config.BadConfigError
  alias KafkaBatcher.MessageObject

  @typep topic :: String.t()
  @typep partition_count :: pos_integer()

  @type partition_fn ::
          (topic(), partition_count(), MessageObject.key(), MessageObject.value() ->
             pos_integer())

  @type t :: %__MODULE__{
          collector_mod: module(),
          topic_name: String.t(),
          partition_fn: partition_fn() | nil,
          collect_by_partition: boolean()
        }

  @enforce_keys [:collector_mod, :topic_name]
  defstruct @enforce_keys ++ [:partition_fn, collect_by_partition: false]

  @spec to_kwlist(t()) :: Keyword.t()
  def to_kwlist(%__MODULE__{} = config) do
    [
      collector_mod: config.collector_mod,
      topic_name: config.topic_name,
      partition_fn: config.partition_fn,
      collect_by_partition: config.collect_by_partition
    ]
  end

  @spec build!(opts :: Keyword.t()) :: t()
  def build!(opts) do
    opts
    |> Keyword.take([
      :collector_mod,
      :topic_name,
      :partition_fn,
      :collect_by_partition
    ])
    |> then(&struct!(__MODULE__, &1))
  rescue
    ArgumentError ->
      reraise(
        BadConfigError,
        "Accumulator config failed: missing required opts: #{inspect(@enforce_keys)}",
        __STACKTRACE__
      )
  end
end
