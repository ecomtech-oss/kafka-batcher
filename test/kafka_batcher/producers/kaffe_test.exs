defmodule Producers.KaffeTest do
  use ExUnit.Case
  use KafkaBatcher.Mocks
  alias KafkaBatcher.MessageObject

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
  @expected_messages Enum.map(
                       @messages,
                       fn %MessageObject{headers: headers, key: key, value: value} ->
                         %{
                           headers: headers,
                           key: key,
                           value: value
                         }
                       end
                     )

  setup :prepare_config

  def prepare_config(%{test: :"test start client with SASL_SSL"} = _context) do
    config =
      Application.get_env(:kafka_batcher, :kafka)
      |> Keyword.put(:sasl, %{mechanism: :scram_sha_512, login: "login", password: "password"})
      |> Keyword.put(:ssl, true)

    Application.put_env(:kafka_batcher, :kafka, config)
  end

  def prepare_config(%{test: :"test start client with empty parameter SASL_SSL"} = _context) do
    config =
      Application.get_env(:kafka_batcher, :kafka)
      |> Keyword.put(:sasl, %{})
      |> Keyword.put(:ssl, false)

    Application.put_env(:kafka_batcher, :kafka, config)
  end

  def prepare_config(_context) do
    config =
      Application.get_env(:kafka_batcher, :kafka)
      |> Keyword.delete(:sasl)
      |> Keyword.delete(:ssl)

    Application.put_env(:kafka_batcher, :kafka, config)
  end

  test "start client" do
    expect(KafkaBatcher.BrodMock, :start_link_client, fn endpoints, client_id, config ->
      assert client_id == @client_name
      assert endpoints == [{"localhost", 9092}]

      assert config == [
               collect_by_partition: false,
               batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
               max_wait_time: 1000,
               batch_size: 10,
               min_delay: 0,
               max_batch_bytesize: 1_000_000,
               ssl: false,
               sasl: :undefined,
               endpoints: [{"localhost", 9092}],
               telemetry: true,
               allow_topic_auto_creation: false,
               partition_strategy: :random,
               required_acks: 1
             ]

      {:ok, self()}
    end)

    {:ok, _} = KafkaBatcher.Producers.Kaffe.start_client()
  end

  test "start client with SASL_SSL" do
    expect(KafkaBatcher.BrodMock, :start_link_client, fn endpoints, client_id, config ->
      assert client_id == @client_name
      assert endpoints == [{"localhost", 9092}]

      assert config == [
               collect_by_partition: false,
               batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
               max_wait_time: 1000,
               batch_size: 10,
               min_delay: 0,
               max_batch_bytesize: 1_000_000,
               ssl: true,
               sasl: {:scram_sha_512, "login", "password"},
               endpoints: [{"localhost", 9092}],
               telemetry: true,
               allow_topic_auto_creation: false,
               partition_strategy: :random,
               required_acks: 1
             ]

      {:ok, self()}
    end)

    {:ok, _} = KafkaBatcher.Producers.Kaffe.start_client()
  end

  test "start client with empty parameter SASL_SSL" do
    expect(KafkaBatcher.BrodMock, :start_link_client, fn endpoints, client_id, config ->
      assert client_id == @client_name
      assert endpoints == [{"localhost", 9092}]

      assert config == [
               collect_by_partition: false,
               batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
               max_wait_time: 1000,
               batch_size: 10,
               min_delay: 0,
               max_batch_bytesize: 1_000_000,
               ssl: false,
               sasl: :undefined,
               endpoints: [{"localhost", 9092}],
               telemetry: true,
               allow_topic_auto_creation: false,
               partition_strategy: :random,
               required_acks: 1
             ]

      {:ok, self()}
    end)

    {:ok, _} = KafkaBatcher.Producers.Kaffe.start_client()
  end

  test "start client with bad SASL parameter" do
    old_config = Application.get_env(:kafka_batcher, :kafka)

    config =
      old_config
      |> Keyword.put(:sasl, %{mechanism: :bad_mechanism, login: "login", password: "password"})
      |> Keyword.put(:ssl, false)

    assert_raise KafkaBatcher.Config.SASLConfigError, fn ->
      Application.put_env(:kafka_batcher, :kafka, config)
      KafkaBatcher.Config.general_producer_config()
    end

    Application.put_env(:kafka_batcher, :kafka, old_config)
  end

  test "start producer" do
    expect(KafkaBatcher.BrodMock, :start_producer, fn client_id, topic_name, config ->
      assert client_id == @client_name
      assert topic_name == @topic1

      assert [
               {:ssl, false},
               {:sasl, :undefined},
               {:endpoints, [{"localhost", 9092}]},
               {:partition_fn, &KafkaBatcher.Test.CalculatePartitionByValueCollector.calculate_partition/4},
               {:allow_topic_auto_creation, false},
               {:partition_strategy, :random},
               {:required_acks, -1},
               {:collect_by_partition, true},
               {:telemetry, true},
               {:batch_flusher, KafkaBatcher.Accumulator.DefaultBatchFlusher},
               {:max_wait_time, 1000},
               {:batch_size, 30},
               {:min_delay, 0},
               {:max_batch_bytesize, 1_000_000},
               {:topic_name, "topic1"}
             ] == config

      :ok
    end)

    topic1_config = KafkaBatcher.Config.get_collector_config(@topic1)
    :ok = KafkaBatcher.Producers.Kaffe.start_producer(@topic1, topic1_config)
  end

  test "get partitions count" do
    expect(KafkaBatcher.BrodMock, :get_partitions_count, fn client_id, topic ->
      assert client_id == @client_name
      assert topic == @topic1
      {:ok, Map.get(@partition_topics, @topic1)}
    end)

    cnt = Map.get(@partition_topics, @topic1)
    {:ok, cnt1} = KafkaBatcher.Producers.Kaffe.get_partitions_count(@topic1)
    assert cnt == cnt1
  end

  test "produce sync by partitions" do
    expect(KafkaBatcher.BrodMock, :produce_sync, fn client_id, topic, partition, _key, messages ->
      assert client_id == @client_name
      assert topic == @topic1
      assert partition == 5
      assert Enum.map(messages, fn message -> Map.drop(message, [:ts]) end) == @expected_messages
      :ok
    end)

    topic1_config = KafkaBatcher.Config.get_collector_config(@topic1)
    KafkaBatcher.Producers.Kaffe.produce_list(@messages, @topic1, 5, topic1_config)
  end

  test "produce sync without partitions" do
    partitions_count = Map.get(@partition_topics, @topic2)

    grouped_messages =
      Enum.group_by(
        @expected_messages,
        fn %{key: key, value: _value} ->
          :erlang.phash2(key, partitions_count)
        end
      )
      |> Enum.into(%{})

    Enum.each(grouped_messages, fn {partition, messages} ->
      expect(KafkaBatcher.BrodMock, :produce_sync, fn client_id, topic, expect_partition, _key, _expect_messages ->
        assert client_id == @client_name
        assert topic == @topic2
        assert expect_partition == partition
        assert Enum.map(messages, fn message -> Map.drop(message, [:ts]) end) == messages
        :ok
      end)
    end)

    expect(KafkaBatcher.BrodMock, :get_partitions_count, fn client_id, topic ->
      assert client_id == @client_name
      assert topic == @topic2
      {:ok, partitions_count}
    end)

    topic2_config = KafkaBatcher.Config.get_collector_config(@topic2)
    KafkaBatcher.Producers.Kaffe.produce_list(@messages, @topic2, nil, topic2_config)
  end
end
