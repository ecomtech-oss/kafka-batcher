defmodule KafkaBatcher.Producers.Base do
  @moduledoc """
  General part of the Kafka producer implementation
  """

  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      @error_notifier Application.compile_env(:kafka_batcher, :error_notifier, KafkaBatcher.DefaultErrorNotifier)

      require Logger

      def produce_list(messages, topic, nil, config) when is_list(messages) and is_binary(topic) and is_list(config) do
        with {:ok, partitions_count} <- get_partitions_count(topic),
             grouped_messages <- group_messages(messages, topic, partitions_count, partition_strategy_from(config)),
             :ok <- produce_list_to_topic(grouped_messages, topic, config) do
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

      def produce_list(messages, topic, partition, config)
          when is_list(messages) and is_binary(topic) and is_list(config) and is_integer(partition) do
        produce_list_to_topic(%{partition => messages}, topic, config)
      rescue
        err ->
          @error_notifier.report(err, stacktrace: __STACKTRACE__)
          {:error, :failed_push_to_kafka}
      end

      def produce_list(messages, topic, partition, config) do
        @error_notifier.report(
          type: "KafkaBatcherProducerError",
          message: """
          Invalid params for produce_list/4:
          (topic #{inspect(topic)}, opts #{inspect(config)}, partition #{inspect(partition)} messages #{inspect(messages)})
          """
        )

        {:error, :internal_error}
      end

      defp produce_list_to_topic(message_list, topic, config) do
        message_list
        |> Enum.reduce_while(:ok, fn {partition, messages}, :ok ->
          Logger.debug("KafkaBatcher: event#produce_list_to_topic topic=#{topic} partition=#{partition}")
          start_time = System.monotonic_time()

          case __MODULE__.do_produce(messages, topic, partition, config) do
            :ok ->
              push_metrics(start_time, topic, partition, messages, telemetry_on?(config))
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

      defp telemetry_on?(opts) do
        Keyword.get(opts, :telemetry, true)
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

      defp partition_strategy_from(opts) do
        case Keyword.fetch(opts, :partition_strategy) do
          {:ok, partition_strategy} ->
            partition_strategy

          :error ->
            KafkaBatcher.Config.general_producer_config()
            |> Keyword.get(:partition_strategy, :random)
        end
      end
    end
  end
end
