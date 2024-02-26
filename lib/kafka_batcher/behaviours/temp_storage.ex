defmodule KafkaBatcher.Behaviours.TempStorage do
  @moduledoc """
  KafkaBatcher.Behaviours.TempStorage behaviour is used to implement events saving logic in case when Kafka is not available.
  """

  alias KafkaBatcher.TempStorage.Batch

  @type topic_name() :: String.t()

  @doc "Save batch to retry"
  @callback save_batch(Batch.t()) :: :ok

  @doc "Check if the storage is empty"
  @callback empty?(topic_name()) :: boolean()
end
