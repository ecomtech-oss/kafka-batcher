defmodule KafkaBatcher.Accumulator.Config do
  @moduledoc false
  alias KafkaBatcher.Config.BadConfigError

  @type t :: %__MODULE__{
          collector: module(),
          topic_name: String.t(),
          partition: pos_integer() | nil,
          batch_flusher: module(),
          max_wait_time: pos_integer(),
          batch_size: pos_integer(),
          min_delay: non_neg_integer(),
          max_batch_bytesize: pos_integer(),
          max_accumulator_restarts: pos_integer(),
          accumulator_mod: module()
        }

  @enforce_keys [:collector, :topic_name]
  defstruct @enforce_keys ++
              [
                :partition,
                batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
                max_wait_time: 1_000,
                batch_size: 10,
                min_delay: 0,
                max_batch_bytesize: 1_000_000,
                max_accumulator_restarts: 100,
                accumulator_mod: KafkaBatcher.Accumulator
              ]

  @spec to_kwlist(t()) :: Keyword.t()
  def to_kwlist(%__MODULE__{} = config) do
    [
      collector: config.collector,
      topic_name: config.topic_name,
      partition: config.partition,
      batch_flusher: config.batch_flusher,
      max_wait_time: config.max_wait_time,
      batch_size: config.batch_size,
      min_delay: config.min_delay,
      max_batch_bytesize: config.max_batch_bytesize,
      max_accumulator_restarts: config.max_accumulator_restarts,
      accumulator_mod: config.accumulator_mod
    ]
  end

  @spec build!(opts :: Keyword.t()) :: t()
  def build!(opts) do
    opts
    |> Keyword.take([
      :collector,
      :topic_name,
      :batch_flusher,
      :max_wait_time,
      :batch_size,
      :min_delay,
      :max_batch_bytesize,
      :max_accumulator_restarts,
      :accumulator_mod
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
