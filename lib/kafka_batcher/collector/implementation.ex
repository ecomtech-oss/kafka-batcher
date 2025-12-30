defmodule KafkaBatcher.Collector.Implementation do
  @moduledoc """
  Part of the KafkaBatcher.Collector implementation not related to GenServer behavior.
  """

  require Logger

  alias KafkaBatcher.{
    AccumulatorsPoolSupervisor,
    Collector,
    Collector.State,
    DataStreamSpec,
    MessageObject
  }

  @producer Application.compile_env(:kafka_batcher, :producer_module, KafkaBatcher.Producers.Kaffe)

  def choose_partition(_message, _data_stream_spec, nil), do: {:error, :kafka_unavailable}

  def choose_partition(
        %MessageObject{key: key, value: value},
        %KafkaBatcher.DataStreamSpec{} = data_stream_spec,
        partitions_count
      ) do
    %Collector.Config{
      partition_fn: partition_fn,
      topic_name: topic_name
    } = data_stream_spec.collector_config

    partition = partition_fn.(topic_name, partitions_count, key, value)

    {:ok, partition}
  end

  def start_accumulators(%State{} = state) do
    collect_by_partition? =
      DataStreamSpec.collect_by_partition?(state.data_stream_spec)

    cond do
      collect_by_partition? and is_nil(state.partitions_count) ->
        {:error, :kafka_unavailable}

      collect_by_partition? ->
        start_accumulators_by_partitions(
          state.data_stream_spec,
          state.partitions_count
        )

      not collect_by_partition? ->
        start_accumulator(state.data_stream_spec)
    end
  end

  defp start_accumulators_by_partitions(data_stream_spec, count) do
    Enum.reduce_while(
      0..(count - 1),
      :ok,
      fn partition, _ ->
        data_stream_spec = DataStreamSpec.set_partition(data_stream_spec, partition)

        case start_accumulator(data_stream_spec) do
          :ok ->
            {:cont, :ok}

          {:error, reason} ->
            {:halt, {:error, reason}}
        end
      end
    )
  end

  defp start_accumulator(data_stream_spec) do
    case AccumulatorsPoolSupervisor.start_accumulator(data_stream_spec) do
      {:ok, _} ->
        :ok

      {:error, {:already_started, _pid}} ->
        :ok

      {:error, reason} ->
        Logger.warning("""
          KafkaBatcher: Accumulator has failed to start with args: #{inspect(data_stream_spec)}.
          Reason: #{inspect(reason)}}
        """)

        {:error, reason}
    end
  end

  @spec store_partition_count(State.t()) :: State.t()
  def store_partition_count(%State{partitions_count: nil} = state) do
    %State{
      data_stream_spec: %DataStreamSpec{
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
