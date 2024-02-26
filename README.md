# KafkaBatcher

A library to increase the throughput of producing messages (coming one at a time) to Kafka by accumulating these messages into batches.

## Installation

  1. Add `KafkaBatcher` to your list of dependencies in `mix.exs`:

  ```elixir
    def deps do
      [
        {:kafka_batcher, "~> 1.0.0"},
        # and one of kafka libraries
        # {:kaffe, "~> 1.24"}
        # or
        # {:kafka_ex, "~> 0.12"}
      ]
    end
  ```

  2. Add KafkaBatcher supervisor to your supervisor tree 
  ```elixir
    def start(_type, _args) do
      children = [
        # Describe the child spec
        KafkaBatcher.Supervisor
      ]

      opts = [strategy: :one_for_one, name: MyApp.Supervisor, max_restarts: 3, max_seconds: 5]
      Supervisor.start_link(children, opts)
    end
  ```  
  
  3. Configure a KafkaBatcher Producer

  Config example:

  ```elixir
  config :kafka_batcher, KafkaBatcher.Collector1, topic_name: "topic1"
  config :kafka_batcher, KafkaBatcher.Collector2, topic_name: "topic2"
  config :kafka_batcher, KafkaBatcher.Collector3, topic_name: "topic3"
  config :kafka_batcher, KafkaBatcher.Collector4, topic_name: "topic4"
  config :kafka_batcher, KafkaBatcher.Collector5, topic_name: "topic5"

  config :kafka_batcher, collectors:
          [
            KafkaBatcher.Collector1,
            KafkaBatcher.Collector2,
            KafkaBatcher.Collector3,
            KafkaBatcher.Collector4,
            KafkaBatcher.Collector5
          ]
          
  config :kafka_batcher, :kafka,
    endpoints: "localhost:9092",
    # in case you use SASL
    # sasl: %{mechanism: :scram_sha_512, login: "login", password: "password"},
    # ssl: true,
    telemetry: true,
    allow_topic_auto_creation: false,
    kafka_topic_aliases: %{
      "real_topic_name1" => "incoming-events",
      "real_topic_name2" => "special-topic"
    }
  
  # In case you use KafkaEx, you need to disable default worker to avoid crashes
  config :kafka_ex, :disable_default_worker, true
  ```

Available parameters:

* `:required_acks` How many acknowledgements the kafka broker should receive from the clustered replicas before acking producer.
* `:endpoints` Kafka cluster endpoints, can be any of the brokers in the cluster, which does not necessarily have to be the leader of any partition, e.g. a load-balanced entrypoint to the remote Kafka cluster.
  More information in the [Brod producer](https://github.com/kafka4beam/brod/blob/master/src/brod_producer.erl) docs.
* `:telemetry`, if set to `true`, metrics will be collected and exposed with PromEx.
* `:allow_topic_auto_creation`, if set true, topics automatically are created with default parameters.
* `:partition_strategy`, allows values: :random, :md5 or function (e.g. `fn _topic, _partitions_count, key, _value -> key end`)
* `:partition_fn`, a function that takes four arguments and returns a number of topic (see below)
* `:collect_by_partition`, if set to `true`, producer accumulates messages separately for each partition of the topic
* `:batch_size`, count of messages to be accumulated by collector before producing
* `:max_wait_time`, max interval between batches in milliseconds. The batch will be produced to Kafka either by `batch_size` or by `max_wait_time` parameter.
* `:batch_flusher`, a module implementing a function `flush?(binary(), map()) :: boolean()`.
  If the function returns true, the current batch will be sent to Kafka immediately.
* `:kafka_topic_aliases` - you could define custom aliases for Kafka topics, it doesn't affect anything except metric labels.
* `:sasl` - optional parameter. The parameter includes three parameters: 
  %{mechanism: mechanism, login: "login", password: "password"}. Mechanism is 
  plain | scram_sha_256 | scram_sha_512. Login and password should be type binary().
* `:ssl` - optional parameter. Ssl should be type boolean(). By default `:ssl` is `false`.
* `:min_delay` - optional parameter. Set minimal delay before send events. This parameter allows to increase max throughput in case when you get more messages (in term of count per second) than you expected when set `batch_size` parameter.
* `:max_batch_bytesize` -  optional parameter. Allows to set a limit on the maximum batch size. By default it is 1_000_000 bytes.

**Important:** The size of one message should not exceed `max_batch_bytesize` setting. If you need to work with large messages you must increase `max_batch_bytesize` value and value of Kafka topic setting `max.message.bytes` as well.

**Note:** you can still produce messages to any Kafka topic (even if it is not described in the kafka_batcher config) by using direct calls of Kaffe or KafkaEx.


## Usage

### Collector examples

```elixir
  defmodule MyApp.Collector1 do
    use KafkaBatcher.Collector, 
      collect_by_partition: true,
      partition_fn: &MyApp.Collector1.calculate_partition/4,
      required_acks: -1,
      batch_size: 30,
      max_wait_time: 20_000
                                        
    def calculate_partition(_topic, partitions_count, _key, value) do
      val = value["client_id"] || value["device_id"]
      :erlang.phash2(val, partitions_count)
    end
  end
  
  defmodule MyApp.Collector2 do
    use KafkaBatcher.Collector, 
      collect_by_partition: false,
      required_acks: 0,
      batch_size: 10,
      max_wait_time: 20_000
    end

  defmodule MyApp.Collector3 do
    use KafkaBatcher.Collector, 
      collect_by_partition: false,
      required_acks: 0,
      batch_size: 10,
      max_wait_time: 20_000,
      partition_strategy: :random
  end

  defmodule MyApp.Collector4 do
    use KafkaBatcher.Collector, 
      collect_by_partition: true,
      required_acks: 0,
      partition_fn: &MyApp.Collector4.calculate_partition/4,
      batch_size: 50,
      batch_flusher: MyApp.Collector4.BatchFlusher

    def calculate_partition(_topic, partitions_count, _key, value) do
      rem(key, partitions_count)
    end
    
    defmodule BatchFlusher do
      def flush?(_key, %{"type" => "SpecialType"}) do
        true
      end

      def flush?(_key, _value) do
        false
      end
    end
  end
  
  defmodule MyApp.Collector5 do
    use KafkaBatcher.Collector, 
      collect_by_partition: false,
      partition_strategy: :md5,
      batch_size: 100,
      max_wait_time: 20_000,
      min_delay: 100,
      max_batch_bytesize: 1_000_000
  end
``` 

### Collector usage

```elixir
  defmodule MyApp.MyModule do
    ...
    def produce_to_kafka_topic1(event)  do
      MyApp.Collector1.add_events(event)
    end

    def produce_to_kafka_topic2(event)  do
      MyApp.Collector2.add_events(event)
    end
    ...
  end

```

### Getting current config of topic

  ```elixir
    KafkaBatcher.Config.get_collector_config("topic1")
  ```

### Getting all topics with config

  ```elixir
    KafkaBatcher.Config.get_configs_by_topic()
  ```

## Testing

```bash
mix test
```

or

```bash
mix test --cover
```
see https://github.com/parroty/excoveralls for details


## Prometheus metrics

The library exposes the following metrics using the PromEx exporter plugin:

- `prom_ex_kafka_consumer_batch_messages_count` (histogram) - The count of messages in one batch consumer.
- `prom_ex_kafka_consumer_batch_total_size_byte` (histogram) - The size of a batch consumer of messages.
- `prom_ex_kafka_consumer_duration_seconds` (histogram) - The time to produce one batch to Kafka.
- `prom_ex_kafka_producer_batch_messages_count` (histogram) - The count of messages in one batch producer.
- `prom_ex_kafka_producer_batch_total_size_byte` (histogram) - The size of a batch producer of messages.
- `prom_ex_kafka_producer_duration_seconds` (histogram) - The time to produce one batch to Kafka.
