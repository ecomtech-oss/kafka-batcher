defmodule KafkaBatcher.Behaviours.Producer do
  @moduledoc """
  KafkaBatcher.Behaviours.Producer adds an abstraction level over producer implementations in various Kafka libs.
  Defines the callbacks that a Kafka producer should implement
  """
  alias KafkaBatcher.Producers

  @type event :: KafkaBatcher.MessageObject.t()
  @type events :: list(event())

  @callback do_produce(
              Producers.Config.t(),
              events :: events(),
              topic :: binary(),
              partition :: non_neg_integer() | nil
            ) :: :ok | {:error, binary() | atom()}

  @callback get_partitions_count(Producers.Config.t(), binary()) ::
              {:ok, integer()} | {:error, binary() | atom()}

  @callback start_client(Producers.Config.t()) :: {:ok, pid()} | {:error, any()}

  @callback start_producer(Producers.Config.t(), binary()) :: :ok | {:error, any()}
end
