defmodule KafkaBatcher.DataStreamSpec.Validator do
  @moduledoc false

  alias KafkaBatcher.{Collector, DataStreamSpec, Producers}

  @spec validate(DataStreamSpec.t()) :: :ok | {:error, String.t()}
  def validate(%DataStreamSpec{} = data_stream_spec) do
    %DataStreamSpec{
      collector_config: %Collector.Config{
        collector_mod: collector_mod,
        partition_fn: partition_fn,
        collect_by_partition: collect_by_partition
      },
      producer_config: %Producers.Config{
        partition_strategy: partition_strategy
      }
    } = data_stream_spec

    cond do
      collect_by_partition and is_nil(partition_fn) ->
        {:error, "collector #{inspect(collector_mod)}. Not found required key :partition_fn"}

      not collect_by_partition and is_nil(partition_strategy) ->
        {:error, "collector #{inspect(collector_mod)}. Not found required key :partition_strategy"}

      :otherwise ->
        :ok
    end
  end
end
