defmodule KafkaBatcher.TempStorage do
  @moduledoc """
  Implements wrap-functions that are called to save batches when Kafka is unavailable.
  """

  alias KafkaBatcher.Collector.State, as: CollectorState

  @storage_impl Application.compile_env(:kafka_batcher, :storage_impl, KafkaBatcher.TempStorage.Default)
  @recheck_kafka_availability_interval Application.compile_env(
                                         :kafka_batcher,
                                         :recheck_kafka_availability_interval,
                                         5_000
                                       )

  @spec save_batch(KafkaBatcher.TempStorage.Batch.t()) :: :ok
  def save_batch(batch), do: @storage_impl.save_batch(batch)

  @spec check_storage(CollectorState.t()) :: CollectorState.t()
  def check_storage(%CollectorState{last_check_timestamp: last_check_timestamp} = state) do
    now = System.os_time(:millisecond)

    if should_recheck?(last_check_timestamp, now) do
      recheck_and_update(state, now)
    else
      state
    end
  end

  defp recheck_and_update(%CollectorState{topic_name: topic, locked?: true} = state, now) do
    if @storage_impl.empty?(topic) do
      %CollectorState{state | locked?: false, last_check_timestamp: nil}
    else
      %CollectorState{state | last_check_timestamp: now}
    end
  end

  defp should_recheck?(last_check_timestamp, now) do
    is_nil(last_check_timestamp) || last_check_timestamp + @recheck_kafka_availability_interval < now
  end
end
