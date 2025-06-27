import Config

config :kafka_batcher, producer_module: KafkaBatcher.Producers.TestProducer

config :kafka_batcher, :brod_client, KafkaBatcher.BrodMock

config :kafka_batcher, :kafka_ex_client, KafkaBatcher.KafkaExMock

config :kafka_batcher, :kafka_ex_metadata, KafkaBatcher.KafkaEx.MetadataMock

config :kafka_batcher, :accumulator, proxy: KafkaBatcher.Accumulator.ProxyMock

config :kafka_batcher,
  recheck_kafka_availability_interval: 50,
  storage_impl: KafkaBatcher.TempStorage.TestStorage,
  reconnect_timeout: 100

config :kafka_batcher, KafkaBatcher.Test.CalculatePartitionByValueCollector, topic_name: "topic1"
config :kafka_batcher, KafkaBatcher.Test.SimpleCollector, topic_name: "topic2"
config :kafka_batcher, KafkaBatcher.Test.BatchFlushCollector, topic_name: "topic3"
config :kafka_batcher, KafkaBatcher.Test.CalculatePartitionByKeyCollector, topic_name: "topic4"
config :kafka_batcher, KafkaBatcher.Test.SimpleCollectorWithDelay, topic_name: "topic6"
config :kafka_batcher, KafkaBatcher.Test.SimpleCollectorMaxByteSizeControl, topic_name: "topic7"
config :kafka_batcher, KafkaBatcher.Test.SimpleCollectorMaxWaitTime, topic_name: "topic8"

config :kafka_batcher,
  collectors: [
    KafkaBatcher.Test.CalculatePartitionByValueCollector,
    KafkaBatcher.Test.SimpleCollector,
    KafkaBatcher.Test.BatchFlushCollector,
    KafkaBatcher.Test.CalculatePartitionByKeyCollector,
    KafkaBatcher.Test.SimpleCollectorWithDelay,
    KafkaBatcher.Test.SimpleCollectorMaxByteSizeControl,
    KafkaBatcher.Test.SimpleCollectorMaxWaitTime
  ]

config :kafka_batcher, :kafka,
  telemetry: true,
  allow_topic_auto_creation: false,
  partition_strategy: :random,
  required_acks: 1
