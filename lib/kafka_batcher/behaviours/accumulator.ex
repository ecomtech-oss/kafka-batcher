defmodule KafkaBatcher.Behaviours.Accumulator do
  @moduledoc """
    Receives individual messages from the collector implementation
    Sends them in batches to kafka using the configured producer
    Applies backpressure to the collector
  """

  alias KafkaBatcher.MessageObject

  @typep topic_name :: String.t()
  @typep partition :: non_neg_integer() | nil

  @callback start_link(Keyword.t()) :: GenServer.on_start()
  @callback child_spec(Keyword.t()) :: Supervisor.child_spec()
  @callback add_event(MessageObject.t(), topic_name()) :: :ok | {:error, term()}
  @callback add_event(MessageObject.t(), topic_name(), partition()) :: :ok | {:error, term()}
end
