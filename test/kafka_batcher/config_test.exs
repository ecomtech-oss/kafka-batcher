defmodule KafkaBatcher.ConfigTest do
  use ExUnit.Case
  doctest KafkaBatcher.Config
  alias KafkaBatcher.Config

  test "supervisor child spec for collectors" do
    assert [
             %{
               id: :"Elixir.KafkaBatcher.AccumulatorsPoolSupervisor.topic1",
               start:
                 {KafkaBatcher.AccumulatorsPoolSupervisor, :start_link,
                  [
                    [
                      ssl: false,
                      sasl: :undefined,
                      endpoints: [{"localhost", 9092}],
                      partition_fn: &KafkaBatcher.Test.CalculatePartitionByValueCollector.calculate_partition/4,
                      allow_topic_auto_creation: false,
                      partition_strategy: :random,
                      required_acks: -1,
                      collect_by_partition: true,
                      telemetry: true,
                      batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
                      max_wait_time: 1000,
                      batch_size: 30,
                      min_delay: 0,
                      max_batch_bytesize: 1_000_000,
                      topic_name: "topic1"
                    ]
                  ]},
               type: :supervisor
             },
             %{
               id: KafkaBatcher.Test.CalculatePartitionByValueCollector,
               start:
                 {KafkaBatcher.Test.CalculatePartitionByValueCollector, :start_link,
                  [
                    [
                      ssl: false,
                      sasl: :undefined,
                      endpoints: [{"localhost", 9092}],
                      partition_fn: &KafkaBatcher.Test.CalculatePartitionByValueCollector.calculate_partition/4,
                      allow_topic_auto_creation: false,
                      partition_strategy: :random,
                      required_acks: -1,
                      collect_by_partition: true,
                      telemetry: true,
                      batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
                      max_wait_time: 1000,
                      batch_size: 30,
                      min_delay: 0,
                      max_batch_bytesize: 1_000_000,
                      topic_name: "topic1"
                    ]
                  ]},
               type: :worker
             },
             %{
               id: :"Elixir.KafkaBatcher.AccumulatorsPoolSupervisor.topic2",
               start:
                 {KafkaBatcher.AccumulatorsPoolSupervisor, :start_link,
                  [
                    [
                      ssl: false,
                      sasl: :undefined,
                      endpoints: [{"localhost", 9092}],
                      allow_topic_auto_creation: false,
                      partition_strategy: :md5,
                      required_acks: -1,
                      collect_by_partition: false,
                      telemetry: true,
                      batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
                      max_wait_time: 1000,
                      batch_size: 10,
                      min_delay: 0,
                      max_batch_bytesize: 1_000_000,
                      topic_name: "topic2"
                    ]
                  ]},
               type: :supervisor
             },
             %{
               id: KafkaBatcher.Test.SimpleCollector,
               start:
                 {KafkaBatcher.Test.SimpleCollector, :start_link,
                  [
                    [
                      ssl: false,
                      sasl: :undefined,
                      endpoints: [{"localhost", 9092}],
                      allow_topic_auto_creation: false,
                      partition_strategy: :md5,
                      required_acks: -1,
                      collect_by_partition: false,
                      telemetry: true,
                      batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
                      max_wait_time: 1000,
                      batch_size: 10,
                      min_delay: 0,
                      max_batch_bytesize: 1_000_000,
                      topic_name: "topic2"
                    ]
                  ]},
               type: :worker
             },
             %{
               id: :"Elixir.KafkaBatcher.AccumulatorsPoolSupervisor.topic3",
               start:
                 {KafkaBatcher.AccumulatorsPoolSupervisor, :start_link,
                  [
                    [
                      ssl: false,
                      sasl: :undefined,
                      endpoints: [{"localhost", 9092}],
                      allow_topic_auto_creation: false,
                      partition_strategy: :random,
                      required_acks: -1,
                      collect_by_partition: false,
                      telemetry: true,
                      batch_flusher: Producers.CollectorTest.BatchFlusher,
                      max_wait_time: 1000,
                      batch_size: 10,
                      min_delay: 0,
                      max_batch_bytesize: 1_000_000,
                      topic_name: "topic3"
                    ]
                  ]},
               type: :supervisor
             },
             %{
               id: KafkaBatcher.Test.BatchFlushCollector,
               start:
                 {KafkaBatcher.Test.BatchFlushCollector, :start_link,
                  [
                    [
                      ssl: false,
                      sasl: :undefined,
                      endpoints: [{"localhost", 9092}],
                      allow_topic_auto_creation: false,
                      partition_strategy: :random,
                      required_acks: -1,
                      collect_by_partition: false,
                      telemetry: true,
                      batch_flusher: Producers.CollectorTest.BatchFlusher,
                      max_wait_time: 1000,
                      batch_size: 10,
                      min_delay: 0,
                      max_batch_bytesize: 1_000_000,
                      topic_name: "topic3"
                    ]
                  ]},
               type: :worker
             },
             %{
               id: :"Elixir.KafkaBatcher.AccumulatorsPoolSupervisor.topic4",
               start:
                 {KafkaBatcher.AccumulatorsPoolSupervisor, :start_link,
                  [
                    [
                      ssl: false,
                      sasl: :undefined,
                      endpoints: [{"localhost", 9092}],
                      partition_fn: &KafkaBatcher.Test.CalculatePartitionByKeyCollector.calculate_partition/4,
                      allow_topic_auto_creation: false,
                      partition_strategy: :random,
                      required_acks: 1,
                      collect_by_partition: true,
                      telemetry: true,
                      batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
                      max_wait_time: 1000,
                      batch_size: 10,
                      min_delay: 0,
                      max_batch_bytesize: 1_000_000,
                      topic_name: "topic4"
                    ]
                  ]},
               type: :supervisor
             },
             %{
               id: KafkaBatcher.Test.CalculatePartitionByKeyCollector,
               start:
                 {KafkaBatcher.Test.CalculatePartitionByKeyCollector, :start_link,
                  [
                    [
                      ssl: false,
                      sasl: :undefined,
                      endpoints: [{"localhost", 9092}],
                      partition_fn: &KafkaBatcher.Test.CalculatePartitionByKeyCollector.calculate_partition/4,
                      allow_topic_auto_creation: false,
                      partition_strategy: :random,
                      required_acks: 1,
                      collect_by_partition: true,
                      telemetry: true,
                      batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
                      max_wait_time: 1000,
                      batch_size: 10,
                      min_delay: 0,
                      max_batch_bytesize: 1_000_000,
                      topic_name: "topic4"
                    ]
                  ]},
               type: :worker
             },
             %{
               id: :"Elixir.KafkaBatcher.AccumulatorsPoolSupervisor.topic6",
               start:
                 {KafkaBatcher.AccumulatorsPoolSupervisor, :start_link,
                  [
                    [
                      ssl: false,
                      sasl: :undefined,
                      endpoints: [{"localhost", 9092}],
                      allow_topic_auto_creation: false,
                      partition_strategy: :md5,
                      required_acks: 0,
                      collect_by_partition: false,
                      telemetry: true,
                      batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
                      max_wait_time: 1000,
                      batch_size: 10,
                      min_delay: 50,
                      max_batch_bytesize: 1_000_000,
                      topic_name: "topic6"
                    ]
                  ]},
               type: :supervisor
             },
             %{
               id: KafkaBatcher.Test.SimpleCollectorWithDelay,
               start:
                 {KafkaBatcher.Test.SimpleCollectorWithDelay, :start_link,
                  [
                    [
                      ssl: false,
                      sasl: :undefined,
                      endpoints: [{"localhost", 9092}],
                      allow_topic_auto_creation: false,
                      partition_strategy: :md5,
                      required_acks: 0,
                      collect_by_partition: false,
                      telemetry: true,
                      batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
                      max_wait_time: 1000,
                      batch_size: 10,
                      min_delay: 50,
                      max_batch_bytesize: 1_000_000,
                      topic_name: "topic6"
                    ]
                  ]},
               type: :worker
             },
             %{
               id: :"Elixir.KafkaBatcher.AccumulatorsPoolSupervisor.topic7",
               start:
                 {KafkaBatcher.AccumulatorsPoolSupervisor, :start_link,
                  [
                    [
                      ssl: false,
                      sasl: :undefined,
                      endpoints: [{"localhost", 9092}],
                      allow_topic_auto_creation: false,
                      partition_strategy: :md5,
                      required_acks: 0,
                      collect_by_partition: false,
                      telemetry: true,
                      batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
                      max_wait_time: 1000,
                      batch_size: 10,
                      min_delay: 50,
                      max_batch_bytesize: 400,
                      topic_name: "topic7"
                    ]
                  ]},
               type: :supervisor
             },
             %{
               id: KafkaBatcher.Test.SimpleCollectorMaxByteSizeControl,
               start:
                 {KafkaBatcher.Test.SimpleCollectorMaxByteSizeControl, :start_link,
                  [
                    [
                      ssl: false,
                      sasl: :undefined,
                      endpoints: [{"localhost", 9092}],
                      allow_topic_auto_creation: false,
                      partition_strategy: :md5,
                      required_acks: 0,
                      collect_by_partition: false,
                      telemetry: true,
                      batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
                      max_wait_time: 1000,
                      batch_size: 10,
                      min_delay: 50,
                      max_batch_bytesize: 400,
                      topic_name: "topic7"
                    ]
                  ]},
               type: :worker
             },
             %{
               id: :"Elixir.KafkaBatcher.AccumulatorsPoolSupervisor.topic8",
               start:
                 {KafkaBatcher.AccumulatorsPoolSupervisor, :start_link,
                  [
                    [
                      ssl: false,
                      sasl: :undefined,
                      endpoints: [{"localhost", 9092}],
                      allow_topic_auto_creation: false,
                      partition_strategy: :md5,
                      required_acks: 0,
                      collect_by_partition: false,
                      telemetry: true,
                      batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
                      max_wait_time: 50,
                      batch_size: 10,
                      min_delay: 20,
                      max_batch_bytesize: 1_000_000,
                      topic_name: "topic8"
                    ]
                  ]},
               type: :supervisor
             },
             %{
               id: KafkaBatcher.Test.SimpleCollectorMaxWaitTime,
               start:
                 {KafkaBatcher.Test.SimpleCollectorMaxWaitTime, :start_link,
                  [
                    [
                      ssl: false,
                      sasl: :undefined,
                      endpoints: [{"localhost", 9092}],
                      allow_topic_auto_creation: false,
                      partition_strategy: :md5,
                      required_acks: 0,
                      collect_by_partition: false,
                      telemetry: true,
                      batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
                      max_wait_time: 50,
                      batch_size: 10,
                      min_delay: 20,
                      max_batch_bytesize: 1_000_000,
                      topic_name: "topic8"
                    ]
                  ]},
               type: :worker
             },
             %{
               id: KafkaBatcher.ConnectionManager,
               start: {KafkaBatcher.ConnectionManager, :start_link, []},
               type: :worker
             }
           ] ==
             Config.collectors_spec()
  end

  test "general collectors config" do
    assert [
             {:collect_by_partition, false},
             {:batch_flusher, KafkaBatcher.Accumulator.DefaultBatchFlusher},
             {:max_wait_time, 1000},
             {:batch_size, 10},
             {:min_delay, 0},
             {:max_batch_bytesize, 1_000_000},
             {:ssl, false},
             {:sasl, :undefined},
             {:endpoints, [{"localhost", 9092}]},
             {:telemetry, true},
             {:allow_topic_auto_creation, false},
             {:partition_strategy, :random},
             {:required_acks, 1}
           ] == Config.general_producer_config()
  end

  test "get configs by topic_name" do
    assert %{
             "topic1" => [
               ssl: false,
               sasl: :undefined,
               endpoints: [{"localhost", 9092}],
               partition_fn: &KafkaBatcher.Test.CalculatePartitionByValueCollector.calculate_partition/4,
               allow_topic_auto_creation: false,
               partition_strategy: :random,
               required_acks: -1,
               collect_by_partition: true,
               telemetry: true,
               batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
               max_wait_time: 1000,
               batch_size: 30,
               min_delay: 0,
               max_batch_bytesize: 1_000_000,
               topic_name: "topic1"
             ],
             "topic2" => [
               ssl: false,
               sasl: :undefined,
               endpoints: [{"localhost", 9092}],
               allow_topic_auto_creation: false,
               partition_strategy: :md5,
               required_acks: -1,
               collect_by_partition: false,
               telemetry: true,
               batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
               max_wait_time: 1000,
               batch_size: 10,
               min_delay: 0,
               max_batch_bytesize: 1_000_000,
               topic_name: "topic2"
             ],
             "topic3" => [
               ssl: false,
               sasl: :undefined,
               endpoints: [{"localhost", 9092}],
               allow_topic_auto_creation: false,
               partition_strategy: :random,
               required_acks: -1,
               collect_by_partition: false,
               telemetry: true,
               batch_flusher: Producers.CollectorTest.BatchFlusher,
               max_wait_time: 1000,
               batch_size: 10,
               min_delay: 0,
               max_batch_bytesize: 1_000_000,
               topic_name: "topic3"
             ],
             "topic4" => [
               ssl: false,
               sasl: :undefined,
               endpoints: [{"localhost", 9092}],
               partition_fn: &KafkaBatcher.Test.CalculatePartitionByKeyCollector.calculate_partition/4,
               allow_topic_auto_creation: false,
               partition_strategy: :random,
               required_acks: 1,
               collect_by_partition: true,
               telemetry: true,
               batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
               max_wait_time: 1000,
               batch_size: 10,
               min_delay: 0,
               max_batch_bytesize: 1_000_000,
               topic_name: "topic4"
             ],
             "topic6" => [
               ssl: false,
               sasl: :undefined,
               endpoints: [{"localhost", 9092}],
               allow_topic_auto_creation: false,
               partition_strategy: :md5,
               required_acks: 0,
               collect_by_partition: false,
               telemetry: true,
               batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
               max_wait_time: 1000,
               batch_size: 10,
               min_delay: 50,
               max_batch_bytesize: 1_000_000,
               topic_name: "topic6"
             ],
             "topic7" => [
               ssl: false,
               sasl: :undefined,
               endpoints: [{"localhost", 9092}],
               allow_topic_auto_creation: false,
               partition_strategy: :md5,
               required_acks: 0,
               collect_by_partition: false,
               telemetry: true,
               batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
               max_wait_time: 1000,
               batch_size: 10,
               min_delay: 50,
               max_batch_bytesize: 400,
               topic_name: "topic7"
             ],
             "topic8" => [
               ssl: false,
               sasl: :undefined,
               endpoints: [{"localhost", 9092}],
               allow_topic_auto_creation: false,
               partition_strategy: :md5,
               required_acks: 0,
               collect_by_partition: false,
               telemetry: true,
               batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
               max_wait_time: 50,
               batch_size: 10,
               min_delay: 20,
               max_batch_bytesize: 1_000_000,
               topic_name: "topic8"
             ]
           } == Config.get_configs_by_topic_name()
  end

  test "get configs by collectors" do
    assert [
             "Elixir.KafkaBatcher.Test.CalculatePartitionByValueCollector": [
               ssl: false,
               sasl: :undefined,
               endpoints: [{"localhost", 9092}],
               partition_fn: &KafkaBatcher.Test.CalculatePartitionByValueCollector.calculate_partition/4,
               allow_topic_auto_creation: false,
               partition_strategy: :random,
               required_acks: -1,
               collect_by_partition: true,
               telemetry: true,
               batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
               max_wait_time: 1000,
               batch_size: 30,
               min_delay: 0,
               max_batch_bytesize: 1_000_000,
               topic_name: "topic1"
             ],
             "Elixir.KafkaBatcher.Test.SimpleCollector": [
               ssl: false,
               sasl: :undefined,
               endpoints: [{"localhost", 9092}],
               allow_topic_auto_creation: false,
               partition_strategy: :md5,
               required_acks: -1,
               collect_by_partition: false,
               telemetry: true,
               batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
               max_wait_time: 1000,
               batch_size: 10,
               min_delay: 0,
               max_batch_bytesize: 1_000_000,
               topic_name: "topic2"
             ],
             "Elixir.KafkaBatcher.Test.BatchFlushCollector": [
               ssl: false,
               sasl: :undefined,
               endpoints: [{"localhost", 9092}],
               allow_topic_auto_creation: false,
               partition_strategy: :random,
               required_acks: -1,
               collect_by_partition: false,
               telemetry: true,
               batch_flusher: Producers.CollectorTest.BatchFlusher,
               max_wait_time: 1000,
               batch_size: 10,
               min_delay: 0,
               max_batch_bytesize: 1_000_000,
               topic_name: "topic3"
             ],
             "Elixir.KafkaBatcher.Test.CalculatePartitionByKeyCollector": [
               ssl: false,
               sasl: :undefined,
               endpoints: [{"localhost", 9092}],
               partition_fn: &KafkaBatcher.Test.CalculatePartitionByKeyCollector.calculate_partition/4,
               allow_topic_auto_creation: false,
               partition_strategy: :random,
               required_acks: 1,
               collect_by_partition: true,
               telemetry: true,
               batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
               max_wait_time: 1000,
               batch_size: 10,
               min_delay: 0,
               max_batch_bytesize: 1_000_000,
               topic_name: "topic4"
             ],
             "Elixir.KafkaBatcher.Test.SimpleCollectorWithDelay": [
               ssl: false,
               sasl: :undefined,
               endpoints: [{"localhost", 9092}],
               allow_topic_auto_creation: false,
               partition_strategy: :md5,
               required_acks: 0,
               collect_by_partition: false,
               telemetry: true,
               batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
               max_wait_time: 1000,
               batch_size: 10,
               min_delay: 50,
               max_batch_bytesize: 1_000_000,
               topic_name: "topic6"
             ],
             "Elixir.KafkaBatcher.Test.SimpleCollectorMaxByteSizeControl": [
               ssl: false,
               sasl: :undefined,
               endpoints: [{"localhost", 9092}],
               allow_topic_auto_creation: false,
               partition_strategy: :md5,
               required_acks: 0,
               collect_by_partition: false,
               telemetry: true,
               batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
               max_wait_time: 1000,
               batch_size: 10,
               min_delay: 50,
               max_batch_bytesize: 400,
               topic_name: "topic7"
             ],
             "Elixir.KafkaBatcher.Test.SimpleCollectorMaxWaitTime": [
               ssl: false,
               sasl: :undefined,
               endpoints: [{"localhost", 9092}],
               allow_topic_auto_creation: false,
               partition_strategy: :md5,
               required_acks: 0,
               collect_by_partition: false,
               telemetry: true,
               batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
               max_wait_time: 50,
               batch_size: 10,
               min_delay: 20,
               max_batch_bytesize: 1_000_000,
               topic_name: "topic8"
             ]
           ] == Config.get_configs_by_collector!()

    old_collectors_config = Application.get_env(:kafka_batcher, :collectors)

    assert_raise KafkaBatcher.Config.BadConfigError, fn ->
      new_collectors_config = [KafkaBatcher.Test.CollectorWithWrongConfig | old_collectors_config]
      Application.put_env(:kafka_batcher, :collectors, new_collectors_config)
      Config.get_configs_by_collector!()
    end

    Application.put_env(:kafka_batcher, :collectors, old_collectors_config)
  end

  test "get config by collectors not exist collector" do
    old_collectors_config = Application.get_env(:kafka_batcher, :collectors)

    assert_raise KafkaBatcher.Config.CollectorMissingError, fn ->
      new_collectors_config = [KafkaBatcher.Test.NotExistsCollector | old_collectors_config]
      Application.put_env(:kafka_batcher, :collectors, new_collectors_config)
      Config.get_configs_by_collector!()
    end

    Application.put_env(:kafka_batcher, :collectors, old_collectors_config)
  end

  test "get collector config by topic name" do
    assert [
             {:ssl, false},
             {:sasl, :undefined},
             {:endpoints, [{"localhost", 9092}]},
             {:partition_fn, &KafkaBatcher.Test.CalculatePartitionByValueCollector.calculate_partition/4},
             {:allow_topic_auto_creation, false},
             {:partition_strategy, :random},
             {:required_acks, -1},
             {:collect_by_partition, true},
             {:telemetry, true},
             {:batch_flusher, KafkaBatcher.Accumulator.DefaultBatchFlusher},
             {:max_wait_time, 1000},
             {:batch_size, 30},
             {:min_delay, 0},
             {:max_batch_bytesize, 1_000_000},
             {:topic_name, "topic1"}
           ] == Config.get_collector_config("topic1")
  end

  test "build collector config" do
    right_config1 = [
      collect_by_partition: false,
      partition_strategy: :md5,
      batch_size: 10
    ]

    assert [
             {:allow_topic_auto_creation, false},
             {:partition_strategy, :md5},
             {:required_acks, -1},
             {:collect_by_partition, false},
             {:telemetry, true},
             {:batch_flusher, KafkaBatcher.Accumulator.DefaultBatchFlusher},
             {:max_wait_time, 1000},
             {:batch_size, 10},
             {:min_delay, 0},
             {:max_batch_bytesize, 1_000_000}
           ] == Config.build_topic_config(right_config1)

    right_config2 = [
      collect_by_partition: false,
      partition_strategy: :random,
      required_acks: 1,
      batch_size: 10,
      min_delay: 50,
      max_batch_bytesize: 10
    ]

    assert [
             {:allow_topic_auto_creation, false},
             {:partition_strategy, :random},
             {:required_acks, 1},
             {:collect_by_partition, false},
             {:telemetry, true},
             {:batch_flusher, KafkaBatcher.Accumulator.DefaultBatchFlusher},
             {:max_wait_time, 1000},
             {:batch_size, 10},
             {:min_delay, 50},
             {:max_batch_bytesize, 10}
           ] == Config.build_topic_config(right_config2)

    extra_params_config = [
      some_params1: false,
      some_params1: 100,
      required_acks: 1,
      batch_size: 10
    ]

    assert [
             {:allow_topic_auto_creation, false},
             {:partition_strategy, :random},
             {:required_acks, 1},
             {:collect_by_partition, false},
             {:telemetry, true},
             {:batch_flusher, KafkaBatcher.Accumulator.DefaultBatchFlusher},
             {:max_wait_time, 1000},
             {:batch_size, 10},
             {:min_delay, 0},
             {:max_batch_bytesize, 1_000_000}
           ] == Config.build_topic_config(extra_params_config)
  end

  test "get endpoints" do
    assert [{"localhost", 9092}] == Config.get_endpoints()
  end
end
