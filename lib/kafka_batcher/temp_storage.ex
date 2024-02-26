defmodule KafkaBatcher.TempStorage do
  @moduledoc """
  Implements wrap-functions that are called to save batches when Kafka is unavailable.
  """

  @storage_impl Application.compile_env(:kafka_batcher, :storage_impl, KafkaBatcher.TempStorage.Default)
  @recheck_kafka_availability_interval Application.compile_env(
                                         :kafka_batcher,
                                         :recheck_kafka_availability_interval,
                                         5_000
                                       )

  @spec save_batch(KafkaBatcher.TempStorage.Batch.t()) :: :ok
  def save_batch(batch) do
    @storage_impl.save_batch(batch)
  end

  @spec empty?(binary(), integer()) :: boolean()
  def empty?(topic, last_check_timestamp) do
    now = System.os_time(:millisecond)

    if is_nil(last_check_timestamp) || last_check_timestamp + @recheck_kafka_availability_interval < now do
      @storage_impl.empty?(topic)
    else
      false
    end
  end
end
