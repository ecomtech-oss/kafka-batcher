defmodule Producers.KafkaExTest do
  alias KafkaBatcher.{MessageObject, Producers}

  use ExUnit.Case
  use KafkaBatcher.Mocks

  @client_name :kafka_producer_client
  @topic1 "topic1"
  @topic2 "topic2"
  @topic3 "topic3"
  @topic4 "topic4"
  @topic5 "topic5"

  @partition_topics %{
    @topic1 => 10,
    @topic2 => 20,
    @topic3 => 10,
    @topic4 => 4,
    @topic5 => 5
  }

  @messages [%MessageObject{key: "key1", value: "value1"}, %MessageObject{key: "key2", value: "value2"}]
  @expected_messages Enum.map(@messages, fn %MessageObject{key: key, value: value, headers: headers} ->
                       %KafkaEx.Protocol.Produce.Message{key: key, value: value, headers: headers}
                     end)

  setup :prepare_config

  def prepare_config(_context) do
    :ok
  end

  test "start client" do
    expect(KafkaBatcher.KafkaExMock, :create_worker, fn client_id, config ->
      assert client_id == @client_name
      assert config == [uris: [{"localhost", 9092}]]
      {:ok, self()}
    end)

    {:ok, _} = Producers.KafkaEx.start_client()
  end

  test "get partitions count" do
    partitions_count = Map.get(@partition_topics, @topic1)

    expect(KafkaBatcher.KafkaExMock, :metadata, fn opts ->
      assert opts == [topic: @topic1, worker_name: @client_name]
      %{@topic1 => %{:topic_metadatas => gen_list(partitions_count)}}
    end)

    expect(KafkaBatcher.KafkaEx.MetadataMock, :partitions_for_topic, fn metadata, topic ->
      %{@topic1 => %{:topic_metadatas => topic_metadatas}} = metadata
      assert partitions_count == length(topic_metadatas)
      assert topic == @topic1
      topic_metadatas
    end)

    {:ok, cnt1} = Producers.KafkaEx.get_partitions_count(@topic1)
    assert cnt1 == partitions_count
  end

  test "produce by partitions" do
    topic1_config = KafkaBatcher.Config.get_collector_config(@topic1)

    expect(KafkaBatcher.KafkaExMock, :produce, fn parameters, opts ->
      %{topic: topic, partition: partition, required_acks: require_acks, messages: kafka_messages} = parameters
      [worker_name: client_id] = opts
      assert client_id == @client_name
      assert topic == @topic1
      assert partition == 5
      assert require_acks == Keyword.get(topic1_config, :required_acks)
      assert kafka_messages == @expected_messages
      {:ok, :rand.uniform(1_000)}
    end)

    Producers.KafkaEx.produce_list(@messages, @topic1, 5, topic1_config)
  end

  test "produce without partitions" do
    partitions_count = Map.get(@partition_topics, @topic2)

    grouped_messages =
      Enum.group_by(@messages, fn %MessageObject{key: key} ->
        :erlang.phash2(key, partitions_count)
      end)

    expected_grouped_messages =
      Enum.group_by(
        @expected_messages,
        fn %KafkaEx.Protocol.Produce.Message{key: key} ->
          :erlang.phash2(key, partitions_count)
        end
      )

    topic2_config = KafkaBatcher.Config.get_collector_config(@topic2)

    Enum.each(grouped_messages, fn {partition, _messages} ->
      expect(KafkaBatcher.KafkaExMock, :produce, fn parameters, opts ->
        %{topic: topic, partition: call_partition, required_acks: require_acks, messages: kafka_messages} = parameters
        [worker_name: client_id] = opts

        assert client_id == @client_name
        assert topic == @topic2
        assert call_partition == partition
        assert require_acks == Keyword.get(topic2_config, :required_acks)
        assert kafka_messages == Map.get(expected_grouped_messages, partition)
        {:ok, :rand.uniform(1_000)}
      end)
    end)

    expect(KafkaBatcher.KafkaExMock, :metadata, fn opts ->
      assert opts == [topic: @topic2, worker_name: @client_name]
      %{@topic2 => %{:topic_metadatas => gen_list(partitions_count)}}
    end)

    expect(KafkaBatcher.KafkaEx.MetadataMock, :partitions_for_topic, fn metadata, topic ->
      %{@topic2 => %{:topic_metadatas => topic_metadatas}} = metadata
      assert partitions_count == length(topic_metadatas)
      assert topic == @topic2
      topic_metadatas
    end)

    Producers.KafkaEx.produce_list(@messages, @topic2, nil, topic2_config)
  end

  defp gen_list(len) do
    Enum.map(1..len, fn i -> i end)
  end
end
