defmodule KafkaBatcher.Producers.Base do
  @moduledoc """
  General part of the Kafka producer implementation
  """

  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      alias KafkaBatcher.Producers

      @error_notifier Application.compile_env(:kafka_batcher, :error_notifier, KafkaBatcher.DefaultErrorNotifier)

      require Logger

      def produce_list(%Producers.Config{} = config, messages, topic, nil) when is_list(messages) and is_binary(topic) do
        with {:ok, partitions_count} <- get_partitions_count(config, topic),
             partition_strategy = config.partition_strategy || :random,
             grouped_messages <- group_messages(messages, topic, partitions_count, partition_strategy),
             :ok <- produce_list_to_topic(config, grouped_messages, topic) do
          :ok
        else
          error ->
            @error_notifier.report(
              type: "KafkaBatcherProducerError",
              message: "event#produce topic=#{topic} error=#{inspect(error)}"
            )

            error
        end
      rescue
        err ->
          @error_notifier.report(err, stacktrace: __STACKTRACE__)
          {:error, :failed_push_to_kafka}
      end

      def produce_list(%Producers.Config{} = config, messages, topic, partition)
          when is_list(messages) and is_binary(topic) and is_integer(partition) do
        produce_list_to_topic(config, %{partition => messages}, topic)
      rescue
        err ->
          @error_notifier.report(err, stacktrace: __STACKTRACE__)
          {:error, :failed_push_to_kafka}
      end

      def produce_list(%Producers.Config{} = config, messages, topic, partition) do
        @error_notifier.report(
          type: "KafkaBatcherProducerError",
          message: """
          Invalid params for produce_list/4:
          (topic #{inspect(topic)}, opts #{inspect(config)}, partition #{inspect(partition)} messages #{inspect(messages)})
          """
        )

        {:error, :internal_error}
      end

      defp produce_list_to_topic(%Producers.Config{} = config, message_list, topic) do
        message_list
        |> Enum.reduce_while(:ok, fn {partition, messages}, :ok ->
          Logger.debug("KafkaBatcher: event#produce_list_to_topic topic=#{topic} partition=#{partition}")
          start_time = System.monotonic_time()

          case __MODULE__.do_produce(config, messages, topic, partition) do
            :ok ->
              push_metrics(start_time, topic, partition, messages, config.telemetry)
              {:cont, :ok}

            {:error, _reason} = error ->
              {:halt, error}
          end
        end)
      end

      defp group_messages(messages, _topic, partitions_count, :random) do
        partition = :rand.uniform(partitions_count) - 1
        %{partition => messages}
      end

      defp group_messages(messages, _topic, partitions_count, :md5) do
        Enum.group_by(messages, fn %KafkaBatcher.MessageObject{key: key} -> :erlang.phash2(key, partitions_count) end)
      end

      defp group_messages(messages, topic, partitions_count, partition_strategy_fn) when is_function(partition_strategy_fn) do
        Enum.group_by(messages, fn %KafkaBatcher.MessageObject{key: key, value: value} ->
          partition_strategy_fn.(topic, partitions_count, key, value)
        end)
      end

      defp push_metrics(_start_time, _topic, _partition, _messages, false) do
        :ok
      end

      defp push_metrics(start_time, topic, partition, messages, true) do
        duration = System.monotonic_time() - start_time

        :telemetry.execute(
          [:prom_ex, :plugin, :kafka, :producer],
          %{
            duration: duration,
            batch_size: Enum.count(messages),
            batch_byte_size: :erlang.external_size(messages)
          },
          %{
            topic: topic,
            partition: partition
          }
        )
      end
    end
  end
end
