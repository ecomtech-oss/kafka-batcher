defmodule KafkaBatcher.Test.CalculatePartitionByValueCollector do
  @moduledoc false
  use KafkaBatcher.Collector,
    collect_by_partition: true,
    partition_fn: &__MODULE__.calculate_partition/4,
    required_acks: -1,
    batch_size: 30

  def calculate_partition(_topic, partitions_count, _key, value) do
    val = value["client_id"] || value["device_id"]
    :erlang.phash2(val, partitions_count)
  end
end

defmodule KafkaBatcher.Test.SimpleCollector do
  @moduledoc false
  use KafkaBatcher.Collector,
    collect_by_partition: false,
    partition_strategy: :md5,
    batch_size: 10
end

defmodule KafkaBatcher.Test.BatchFlushCollector do
  @moduledoc false
  use KafkaBatcher.Collector,
    collect_by_partition: false,
    partition_strategy: :random,
    batch_size: 10,
    batch_flusher: Producers.CollectorTest.BatchFlusher
end

defmodule KafkaBatcher.Test.CalculatePartitionByKeyCollector do
  @moduledoc false
  use KafkaBatcher.Collector,
    collect_by_partition: true,
    partition_fn: &__MODULE__.calculate_partition/4,
    required_acks: 1,
    batch_size: 10

  def calculate_partition(_topic, partitions_count, key, _value) do
    :erlang.phash2(key, partitions_count)
  end
end

defmodule KafkaBatcher.Test.SimpleCollectorWithDelay do
  @moduledoc false
  use KafkaBatcher.Collector,
    collect_by_partition: false,
    partition_strategy: :md5,
    required_acks: 0,
    batch_size: 10,
    min_delay: 50
end

defmodule KafkaBatcher.Test.SimpleCollectorMaxByteSizeControl do
  @moduledoc false
  use KafkaBatcher.Collector,
    collect_by_partition: false,
    partition_strategy: :md5,
    required_acks: 0,
    batch_size: 10,
    min_delay: 50,
    max_batch_bytesize: 400
end

defmodule KafkaBatcher.Test.SimpleCollectorMaxWaitTime do
  @moduledoc false
  use KafkaBatcher.Collector,
    collect_by_partition: false,
    partition_strategy: :md5,
    required_acks: 0,
    batch_size: 10,
    max_wait_time: 50,
    min_delay: 20
end

defmodule KafkaBatcher.Test.CollectorWithWrongConfig do
  @moduledoc false
  use KafkaBatcher.Collector,
    collect_by_partition: true
end

defmodule KafkaBatcher.Test.StartAccumulatorFail do
  @moduledoc false
  use KafkaBatcher.Collector,
    collect_by_partition: true
end

defmodule KafkaBatcher.Test.FailingCollector do
  @moduledoc false
  use GenServer

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil)
  end

  @impl true
  def init(nil) do
    {:ok, nil}
  end

  @impl true
  def handle_call(_request, _from, _state) do
    throw(:timeout)
  end
end
