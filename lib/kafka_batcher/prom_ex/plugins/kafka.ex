if Code.ensure_loaded?(PromEx) do
  defmodule KafkaBatcher.PromEx.Plugins.Kafka do
    @moduledoc """
    PromEx plugin to collect Prometheus metrics of interactions with Kafka.
    The following metrics are collected here:

    * prom_ex_kafka_producer_batch_total_size_byte_bucket
    * prom_ex_kafka_producer_batch_total_size_byte_sum
    * prom_ex_kafka_producer_batch_total_size_byte_count
    * prom_ex_kafka_producer_batch_messages_count_bucket
    * prom_ex_kafka_producer_batch_messages_count_sum
    * prom_ex_kafka_producer_batch_messages_count_count
    * prom_ex_kafka_producer_duration_seconds_bucket
    * prom_ex_kafka_producer_duration_seconds_sum
    * prom_ex_kafka_producer_duration_seconds_count

    Each metric has the following labels:
    * topic (topic name)
    * partition (partition number)
    * topic_alias (short name of topic to improve readability of Grafana dashboards in case when topic

    Configuration options that allow you to set metrics display preferences:
      :kafka_topic_aliases - allows you to set an alias for display in metrics
      :producer_buckets - allows to set bucket parameters for grouping metrics
      For example:

         config :kafka_batcher,
           kafka_topic_aliases: %{
             my_topic1 => "topic1",
             my_topic2 => "topic2"
           }

         config :kafka_batcher,
           :kafka_metric_opts,
             producer_buckets:
             [
               duration: [1, 2, 3, 4, 5, 10, 15, 20, 50, 100],
               byte_size: [1_000, 2_000, 5_000, 10_000, 20_000, 50_000, 100_000],
               messages_count: [1, 5, 10, 15, 20, 30, 40, 50, 100]
             ]
    """

    use PromEx.Plugin
    require Logger

    @producer_event_metrics [:prom_ex, :plugin, :kafka, :producer]
    @consumer_event_metrics [:prom_ex, :plugin, :kafka, :consumer]

    @default_producer_buckets [
      duration: [1, 2, 3, 4, 5, 10, 15, 20, 50, 100],
      byte_size: [1_000, 2_000, 5_000, 10_000, 20_000, 50_000, 100_000],
      messages_count: [1, 5, 10, 15, 20, 30, 40, 50, 100]
    ]

    @default_consumer_buckets [
      duration: [10, 20, 50, 100, 150, 200, 500, 1000, 2000],
      byte_size: [1_000, 2_000, 5_000, 10_000, 20_000, 50_000, 100_000],
      messages_count: [1, 5, 10, 15, 20, 30, 40, 50, 100]
    ]

    @impl true
    def event_metrics(_opts) do
      metric_prefix = [:prom_ex, :kafka]

      labels = %{}
      buckets = Application.get_env(:kafka_batcher, :kafka_metric_opts, [])

      [
        producer_event_metrics(metric_prefix, labels, buckets),
        consumer_event_metrics(metric_prefix, labels, buckets)
      ]
    end

    def producer_event_metrics(metric_prefix, labels, buckets) do
      producer_metrics_tags = Map.keys(labels) ++ [:topic, :partition, :topic_alias]
      buckets = Keyword.get(buckets, :producer_buckets, @default_producer_buckets)

      Event.build(
        :producer_event_metrics,
        build_kafka_metrics(
          metric_prefix,
          producer_metrics_tags,
          labels,
          :producer,
          @producer_event_metrics,
          buckets
        )
      )
    end

    def consumer_event_metrics(metric_prefix, labels, buckets) do
      consumer_metrics_tags = Map.keys(labels) ++ [:topic, :partition, :topic_alias]
      buckets = Keyword.get(buckets, :consumer_buckets, @default_consumer_buckets)

      Event.build(
        :consumer_event_metrics,
        build_kafka_metrics(
          metric_prefix,
          consumer_metrics_tags,
          labels,
          :consumer,
          @consumer_event_metrics,
          buckets
        )
      )
    end

    defp build_kafka_metrics(metric_prefix, metrics_tags, labels, name, event_name, buckets) do
      aliases = Application.get_env(:kafka_batcher, :kafka_topic_aliases, %{})

      [
        distribution(
          metric_prefix ++ [name, :duration, :seconds],
          event_name: event_name,
          description: "The time to produce one batch to Kafka.",
          reporter_options: [
            buckets: Keyword.fetch!(buckets, :duration)
          ],
          measurement: fn measurements, _metadata ->
            measurements.duration
          end,
          tag_values: fn metadata ->
            set_tags_value(metadata, aliases, labels)
          end,
          tags: metrics_tags,
          unit: {:native, :second}
        ),
        distribution(
          metric_prefix ++ [name, :batch, :messages, :count],
          event_name: event_name,
          description: "The count of messages in one batch #{name}",
          reporter_options: [
            buckets: Keyword.fetch!(buckets, :messages_count)
          ],
          measurement: fn measurements, _metadata ->
            measurements.batch_size
          end,
          tag_values: fn metadata ->
            set_tags_value(metadata, aliases, labels)
          end,
          tags: metrics_tags
        ),
        distribution(
          metric_prefix ++ [name, :batch, :total, :size, :byte],
          event_name: event_name,
          description: "The size of a batch #{name} of messages",
          reporter_options: [
            buckets: Keyword.fetch!(buckets, :byte_size)
          ],
          measurement: fn measurements, _metadata ->
            measurements.batch_byte_size
          end,
          tag_values: fn metadata ->
            set_tags_value(metadata, aliases, labels)
          end,
          tags: metrics_tags
        )
      ]
    end

    defp set_tags_value(%{topic: topic} = metadata, aliases, labels) do
      Map.take(metadata, [:topic, :partition])
      |> Map.put(:topic_alias, Map.get(aliases, topic, topic))
      |> Map.merge(labels)
    end
  end
end
