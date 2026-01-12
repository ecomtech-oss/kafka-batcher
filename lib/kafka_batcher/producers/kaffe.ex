defmodule KafkaBatcher.Producers.Kaffe do
  @moduledoc """
  An implementation of the KafkaBatcher.Behaviours.Producer for Kaffe
  """
  alias KafkaBatcher.{Producers, Producers.Config.BrodConfig}

  @brod_client Application.compile_env(:kafka_batcher, :brod_client, :brod)

  @behaviour KafkaBatcher.Behaviours.Producer
  use KafkaBatcher.Producers.Base

  ## -------------------------------------------------------------------------
  ## public api
  ## -------------------------------------------------------------------------

  @impl true
  def start_client(%Producers.Config{} = config) do
    @brod_client.start_link_client(
      config.endpoints,
      config.client_name,
      BrodConfig.to_kwlist(config.brod_config)
    )
  end

  @impl true
  def start_producer(%Producers.Config{} = config, topic_name) do
    @brod_client.start_producer(
      config.client_name,
      topic_name,
      BrodConfig.to_kwlist(config.brod_config)
    )
  end

  @impl true
  def get_partitions_count(%Producers.Config{} = config, topic) do
    @brod_client.get_partitions_count(config.client_name, topic)
  end

  @impl true
  def do_produce(%Producers.Config{} = config, messages, topic, partition) do
    @brod_client.produce_sync(
      config.client_name,
      topic,
      partition,
      "ignored",
      transform_messages(messages)
    )
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
