defmodule KafkaBatcher.Producers.Kaffe do
  @moduledoc """
  An implementation of the KafkaBatcher.Behaviours.Producer for Kaffe
  """

  @brod_client Application.compile_env(:kafka_batcher, :brod_client, :brod)
  @client_name :kafka_producer_client

  @behaviour KafkaBatcher.Behaviours.Producer
  use KafkaBatcher.Producers.Base

  ## -------------------------------------------------------------------------
  ## public api
  ## -------------------------------------------------------------------------

  @impl true
  def start_client() do
    config = KafkaBatcher.Config.general_producer_config()
    endpoints = Keyword.fetch!(config, :endpoints)

    @brod_client.start_link_client(endpoints, @client_name, config)
  end

  @impl true
  def start_producer(topic_name, config) do
    @brod_client.start_producer(@client_name, topic_name, config)
  end

  @impl true
  def get_partitions_count(topic) do
    @brod_client.get_partitions_count(@client_name, topic)
  end

  @impl true
  def do_produce(messages, topic, partition, _config) do
    @brod_client.produce_sync(@client_name, topic, partition, "ignored", transform_messages(messages))
  end

  ## -------------------------------------------------------------------------
  ## internal functions
  ## -------------------------------------------------------------------------

  defp transform_messages(messages) do
    Enum.map(
      messages,
      fn %KafkaBatcher.MessageObject{headers: headers, key: key, value: value} ->
        %{
          headers: headers,
          ts: System.os_time(:millisecond),
          key: key,
          value: value
        }
      end
    )
  end
end
