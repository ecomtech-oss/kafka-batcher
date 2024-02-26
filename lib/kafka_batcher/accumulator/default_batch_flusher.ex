defmodule KafkaBatcher.Accumulator.DefaultBatchFlusher do
  @moduledoc """
  This module implements logic for force pushing current batch to Kafka,
  without waiting for other conditions (on size and/or interval).
  """
  @behaviour KafkaBatcher.Behaviours.BatchFlusher

  @impl KafkaBatcher.Behaviours.BatchFlusher
  def flush?(_key, _event) do
    false
  end
end
