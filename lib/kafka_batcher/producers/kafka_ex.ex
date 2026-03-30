if Code.ensure_loaded?(KafkaEx) do
  defmodule KafkaBatcher.Producers.KafkaEx do
    @moduledoc """
    An implementation of the KafkaBatcher.Behaviours.Producer for KafkaEx
    """
    alias KafkaBatcher.Producers

    @kafka_ex_client Application.compile_env(:kafka_batcher, :kafka_ex_client, KafkaEx)
    @metadata_response Application.compile_env(:kafka_batcher, :kafka_ex_metadata, KafkaEx.Protocol.Metadata.Response)

    @behaviour KafkaBatcher.Behaviours.Producer
    use KafkaBatcher.Producers.Base

    ## -------------------------------------------------------------------------
    ## public api
    ## -------------------------------------------------------------------------

    ## KafkaEx start worker
    @impl true
    def start_client(%Producers.Config{} = config) do
      @kafka_ex_client.create_worker(config.client_name, uris: config.endpoints)
    end

    @impl true
    def start_producer(%Producers.Config{}, _topic_name) do
      :ok
    end

    @impl true
    def get_partitions_count(%Producers.Config{} = config, topic) do
      count =
        @kafka_ex_client.metadata(topic: topic, worker_name: config.client_name)
        |> @metadata_response.partitions_for_topic(topic)
        |> length()

      {:ok, count}
    end

    @impl true
    def do_produce(%Producers.Config{} = config, messages, topic, partition) do
      case @kafka_ex_client.produce(
             %KafkaEx.Protocol.Produce.Request{
               topic: topic,
               partition: partition,
               required_acks: config.required_acks,
               messages: transform_messages(messages)
             },
             worker_name: config.client_name
           ) do
        {:ok, _offset} ->
          :ok

        :ok ->
          :ok

        nil ->
          {:error, "Producing was failed"}

        {:error, reason} ->
          {:error, reason}
      end
    end

    ## -------------------------------------------------------------------------
    ## internal functions
    ## -------------------------------------------------------------------------

    defp transform_messages(messages) do
      Enum.map(
        messages,
        fn
          %KafkaBatcher.MessageObject{key: key, value: value, headers: headers} ->
            %KafkaEx.Protocol.Produce.Message{headers: headers, key: key, value: value}
        end
      )
    end
  end
end
