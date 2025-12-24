defmodule KafkaBatcher.PipelineUnit.Validator do
  @moduledoc false

  alias KafkaBatcher.{Collector, PipelineUnit, Producers}

  @spec validate(PipelineUnit.t()) :: :ok | {:error, String.t()}
  def validate(%PipelineUnit{} = pipeline_unit) do
    %PipelineUnit{
      collector_config: %Collector.Config{
        collector: collector,
        partition_fn: partition_fn,
        collect_by_partition: collect_by_partition
      },
      producer_config: %Producers.Config{
        partition_strategy: partition_strategy
      }
    } = pipeline_unit

    cond do
      collect_by_partition and is_nil(partition_fn) ->
        {:error, "collector #{inspect(collector)}. Not found required key :partition_fn"}

      not collect_by_partition and is_nil(partition_strategy) ->
        {:error, "collector #{inspect(collector)}. Not found required key :partition_strategy"}

      :otherwise ->
        :ok
    end
  end
end
