defmodule KafkaBatcher.Behaviours.Producer do
  @moduledoc """
  KafkaBatcher.Behaviours.Producer adds an abstraction level over producer implementations in various Kafka libs.
  Defines the callbacks that a Kafka producer should implement
  """
  @type event :: KafkaBatcher.MessageObject.t()
  @type events :: list(event())

  @callback do_produce(
              events :: events(),
              topic :: binary(),
              partition :: non_neg_integer() | nil,
              config :: Keyword.t()
            ) :: :ok | {:error, binary() | atom()}

  @callback get_partitions_count(binary()) :: {:ok, integer()} | {:error, binary() | atom()}

  @callback start_client() :: {:ok, pid()} | {:error, any()}

  @callback start_producer(binary(), Keyword.t()) :: :ok | {:error, any()}
end
