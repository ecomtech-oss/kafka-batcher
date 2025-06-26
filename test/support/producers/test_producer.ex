defmodule KafkaBatcher.Producers.TestProducer do
  @moduledoc false

  @behaviour KafkaBatcher.Behaviours.Producer
  use KafkaBatcher.Producers.Base
  use KafkaBatcher.ClientHelper, reg_name: __MODULE__
  use KafkaBatcher.MoxHelper, client: __MODULE__

  @topic1 "topic1"
  @topic2 "topic2"
  @topic3 "topic3"
  @topic4 "topic4"
  @topic5 "topic5"
  @topic6 "topic6"
  @topic7 "topic7"
  @topic8 "topic8"

  @partition_counts %{
    @topic1 => 10,
    @topic2 => 20,
    @topic3 => 10,
    @topic4 => 4,
    @topic5 => 5,
    @topic6 => 6,
    @topic7 => 7,
    @topic8 => 8
  }

  @impl true
  def start_client do
    process_callback(%{action: :start_client}, {:ok, self()})
  end

  @impl true
  def start_producer(topic_name, config) do
    process_callback(%{action: :start_producer, parameters: {topic_name, config}}, :ok)
  end

  @impl true
  def get_partitions_count(topic_name) do
    response = {:ok, @partition_counts[topic_name]}
    process_callback(%{action: :get_partitions_count, parameters: topic_name}, response)
  end

  @impl true
  def do_produce(messages, topic, partition, config) do
    process_callback(%{action: :do_produce, parameters: {messages, topic, partition, config}}, :ok)
  end

  def topic_name(idx) when idx >= 1 and idx <= 8, do: "topic#{idx}"
end
