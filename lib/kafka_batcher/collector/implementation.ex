defmodule KafkaBatcher.Collector.Implementation do
  @moduledoc """
  Part of the KafkaBatcher.Collector implementation not related to GenServer behavior.
  """

  require Logger
  alias KafkaBatcher.{AccumulatorsPoolSupervisor, MessageObject, Collector.State}
  @producer Application.compile_env(:kafka_batcher, :producer_module, KafkaBatcher.Producers.Kaffe)

  def calculate_partition(%MessageObject{key: key, value: value}, topic_name, config) do
    calc_partition_fn = Keyword.fetch!(config, :partition_fn)

    case @producer.get_partitions_count(topic_name) do
      {:ok, partitions_count} ->
        partition = calc_partition_fn.(topic_name, partitions_count, key, value)
        {:ok, partition}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def start_accumulators(%State{topic_name: topic_name, collect_by_partition: true} = state) do
    case @producer.get_partitions_count(topic_name) do
      {:ok, count} ->
        start_accumulators_by_partitions(count, state)

      {:error, reason} ->
        {:error, reason}
    end
  end

  def start_accumulators(%State{topic_name: topic_name, config: config, collect_by_partition: false} = state) do
    start_accumulator(topic_name: topic_name, config: config, collector: state.collector)
  end

  defp start_accumulators_by_partitions(count, %State{topic_name: topic_name, config: config} = state) do
    Enum.reduce_while(
      0..(count - 1),
      :ok,
      fn partition, _ ->
        case start_accumulator(topic_name: topic_name, partition: partition, config: config, collector: state.collector) do
          :ok ->
            {:cont, :ok}

          {:error, reason} ->
            {:halt, {:error, reason}}
        end
      end
    )
  end

  defp start_accumulator(args) do
    case AccumulatorsPoolSupervisor.start_accumulator(args) do
      {:ok, _} ->
        :ok

      {:error, {:already_started, _pid}} ->
        :ok

      {:error, reason} ->
        Logger.warning("""
          KafkaBatcher: Accumulator has failed to start with args: #{inspect(args)}.
          Reason: #{inspect(reason)}}
        """)

        {:error, reason}
    end
  end
end
