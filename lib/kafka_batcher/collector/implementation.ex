defmodule KafkaBatcher.Collector.Implementation do
  @moduledoc """
  Part of the KafkaBatcher.Collector implementation not related to GenServer behavior.
  """

  require Logger

  alias KafkaBatcher.{
    AccumulatorsPoolSupervisor,
    Collector,
    Collector.State,
    MessageObject,
    PipelineUnit
  }

  @producer Application.compile_env(:kafka_batcher, :producer_module, KafkaBatcher.Producers.Kaffe)

  def choose_partition(_message, _pipeline_unit, nil), do: {:error, :kafka_unavailable}

  def choose_partition(
        %MessageObject{key: key, value: value},
        %KafkaBatcher.PipelineUnit{} = pipeline_unit,
        partitions_count
      ) do
    %Collector.Config{
      partition_fn: partition_fn,
      topic_name: topic_name
    } = pipeline_unit.collector_config

    partition = partition_fn.(topic_name, partitions_count, key, value)

    {:ok, partition}
  end

  def start_accumulators(%State{} = state) do
    collect_by_partition? =
      PipelineUnit.collect_by_partition?(state.pipeline_unit)

    cond do
      collect_by_partition? and is_nil(state.partitions_count) ->
        {:error, :kafka_unavailable}

      collect_by_partition? ->
        start_accumulators_by_partitions(
          state.pipeline_unit,
          state.partitions_count
        )

      not collect_by_partition? ->
        start_accumulator(state.pipeline_unit)
    end
  end

  defp start_accumulators_by_partitions(pipeline_unit, count) do
    Enum.reduce_while(
      0..(count - 1),
      :ok,
      fn partition, _ ->
        pipeline_unit = PipelineUnit.set_partition(pipeline_unit, partition)

        case start_accumulator(pipeline_unit) do
          :ok ->
            {:cont, :ok}

          {:error, reason} ->
            {:halt, {:error, reason}}
        end
      end
    )
  end

  defp start_accumulator(pipeline_unit) do
    case AccumulatorsPoolSupervisor.start_accumulator(pipeline_unit) do
      {:ok, _} ->
        :ok

      {:error, {:already_started, _pid}} ->
        :ok

      {:error, reason} ->
        Logger.warning("""
          KafkaBatcher: Accumulator has failed to start with args: #{inspect(pipeline_unit)}.
          Reason: #{inspect(reason)}}
        """)

        {:error, reason}
    end
  end

  @spec store_partition_count(State.t()) :: State.t()
  def store_partition_count(%State{partitions_count: nil} = state) do
    %State{
      pipeline_unit: %PipelineUnit{
        collector_config: %Collector.Config{topic_name: topic_name},
        producer_config: producer_config
      }
    } = state

    case @producer.get_partitions_count(producer_config, topic_name) do
      {:ok, partitions_count} ->
        %State{state | partitions_count: partitions_count}

      {:error, _reason} ->
        state
    end
  end

  def store_partition_count(%State{partitions_count: count} = state) when is_integer(count), do: state
end
