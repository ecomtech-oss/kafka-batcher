defmodule Producers.CollectorTest do
  use ExUnit.Case, async: false
  use KafkaBatcher.Mocks

  alias KafkaBatcher.MessageObject
  alias KafkaBatcher.TempStorage.Batch
  alias KafkaBatcher.Producers.TestProducer
  alias KafkaBatcher.TempStorage.TestStorage

  @template_events [
    %{
      "id" => "event1",
      "client_id" => "12345"
    },
    %{
      "id" => "event2",
      "client_id" => "123",
      "type" => "push_delivery_success"
    },
    %{
      "id" => "event3",
      "client_id" => "123",
      "type" => "push_open_success"
    },
    %{
      "id" => "event4",
      "client_id" => "456",
      "type" => "push_open_success"
    },
    %{
      "id" => "event5",
      "client_id" => "456",
      "type" => "push_delivery_success"
    },
    %{
      "id" => "event6",
      "device_id" => "9999",
      "type" => "Catalog - Category - Filter",
      "source" => "https://samokat.ru/samokat-app"
    },
    %{
      "id" => "event7",
      "device_id" => "5324",
      "type" => "Catalog - Main - View",
      "source" => "https://samokat.ru/samokat-app"
    },
    %{
      "id" => "event8",
      "client_id" => "9999",
      "type" => "Catalog - Category - Filter",
      "source" => "https://samokat.ru/samokat-app"
    },
    %{
      "id" => "event9",
      "client_id" => "5324",
      "type" => "Catalog - Main - View",
      "source" => "https://samokat.ru/samokat-app"
    },
    %{
      "id" => "event10",
      "client_id" => "9999",
      "type" => "Global - App - To foreground",
      "source" => "vma.samokat.ru"
    },
    %{
      "id" => "event10",
      "client_id" => "9999",
      "type" => "Global - App - To foreground",
      "source" => "unknown_source"
    }
  ]

  setup_all do
    prepare_producers()
  end

  setup do
    prepare_mocks()
  end

  def prepare_mocks() do
    TestProducer.set_owner()
    TestProducer.set_notification_mode(:do_produce, :on)
    TestProducer.set_notification_mode(:get_partitions_count, :on)
    TestStorage.set_owner()
    TestStorage.set_notification_mode(:save_batch, :on)
    TestStorage.set_notification_mode(:empty?, :on)

    on_exit(fn ->
      TestProducer.set_notification_mode(:start_client, :off)
      TestProducer.set_notification_mode(:start_producer, :off)
      TestProducer.set_notification_mode(:do_produce, :off)
      TestProducer.set_notification_mode(:get_partitions_count, :off)
      TestStorage.set_notification_mode(:save_batch, :off)
      TestStorage.set_notification_mode(:empty?, :off)
    end)
  end

  def prepare_producers() do
    KafkaBatcher.ProducerHelper.connection_manager_up()
    :ok
  end

  test "produce by_partitions, calculate partition by value" do
    topic1_config = KafkaBatcher.Test.CalculatePartitionByValueCollector.get_config()
    topic1 = TestProducer.topic_name(1)

    sup_name = :"Elixir.KafkaBatcher.AccumulatorsPoolSupervisor.#{topic1}"

    [{pool_sup, _, _, _}] =
      Supervisor.which_children(KafkaBatcher.Supervisor)
      |> Enum.filter(fn
        {^sup_name, _, _, _} -> true
        _ -> false
      end)

    Supervisor.which_children(pool_sup)
    |> Enum.all?(fn {_, _, _, [module]} -> module == KafkaBatcher.Test.CalculatePartitionByValueCollector end)

    grouped_events =
      Enum.group_by(
        @template_events,
        fn event ->
          {:ok, partitions_count} = TestProducer.get_partitions_count(topic1)
          calc_partition(event, "", topic1, partitions_count, topic1_config)
        end,
        fn event -> {get_key_id(event), event} end
      )
      |> generate_events_by_group(Keyword.fetch!(topic1_config, :batch_size))

    grouped_messages =
      Enum.map(grouped_events, transform_messages())
      |> Enum.into(%{})

    Enum.map(
      grouped_events,
      fn {_part, events} ->
        events
      end
    )
    |> List.flatten()
    |> KafkaBatcher.Test.CalculatePartitionByValueCollector.add_events()

    Enum.each(
      grouped_messages,
      fn {call_partition, messages} ->
        parameters = {messages, topic1, call_partition, topic1_config}
        assert_receive(%{action: :do_produce, parameters: ^parameters})
      end
    )
  end

  test "produce by_partitions, calculate partition by key" do
    topic4_config = KafkaBatcher.Test.CalculatePartitionByKeyCollector.get_config()
    topic4 = TestProducer.topic_name(4)

    grouped_events =
      Enum.group_by(
        @template_events,
        fn event ->
          {:ok, partitions_count} = TestProducer.get_partitions_count(topic4)
          calc_partition(event, get_key_id(event), topic4, partitions_count, topic4_config)
        end,
        fn event -> {get_key_id(event), event} end
      )
      |> generate_events_by_group(Keyword.fetch!(topic4_config, :batch_size))

    grouped_messages =
      Enum.map(grouped_events, transform_messages())
      |> Enum.into(%{})

    Enum.map(
      grouped_events,
      fn {_part, events} ->
        events
      end
    )
    |> List.flatten()
    |> KafkaBatcher.Test.CalculatePartitionByKeyCollector.add_events()

    Enum.each(
      grouped_messages,
      fn {call_partition, messages} ->
        parameters = {messages, topic4, call_partition, topic4_config}
        assert_receive(%{action: :do_produce, parameters: ^parameters})
      end
    )
  end

  test "produce simple collector" do
    topic2_config = KafkaBatcher.Test.SimpleCollector.get_config()
    topic2 = TestProducer.topic_name(2)
    batch_size = Keyword.fetch!(topic2_config, :batch_size)

    events = generate_events(@template_events, batch_size)
    messages = Enum.map(events, fn event -> %MessageObject{key: "", value: Jason.encode!(event)} end)

    Enum.map(events, fn event -> {"", event} end)
    |> KafkaBatcher.Test.SimpleCollector.add_events()

    assert_receive(%{action: :do_produce, parameters: parameters})
    {^messages, ^topic2, _call_partition, ^topic2_config} = parameters
  end

  test "produce simple collector with error" do
    topic2_config = KafkaBatcher.Test.SimpleCollector.get_config()
    topic2 = TestProducer.topic_name(2)
    batch_size = Keyword.fetch!(topic2_config, :batch_size)

    events = generate_events(@template_events, batch_size)
    source_messages = Enum.map(events, fn event -> %MessageObject{key: "", value: Jason.encode!(event)} end)

    TestProducer.set_response(:do_produce, {:error, :usual_error})

    Enum.map(events, fn event -> {"", event} end)
    |> KafkaBatcher.Test.SimpleCollector.add_events()

    assert_receive(%{action: :do_produce, parameters: parameters})
    {^source_messages, ^topic2, _call_partition, ^topic2_config} = parameters

    assert_receive(%{action: :save_batch, parameters: retry_data})

    %Batch{
      messages: retry_messages,
      topic: retry_topic,
      partition: retry_partition,
      producer_config: retry_config
    } = retry_data

    assert retry_topic === topic2
    assert retry_config === topic2_config
    assert retry_partition === nil
    assert retry_messages === source_messages

    interval = Application.fetch_env!(:kafka_batcher, :recheck_kafka_availability_interval)

    Process.sleep(interval + 1)

    Enum.map(events, fn event -> {"", event} end)
    |> KafkaBatcher.Test.SimpleCollector.add_events()

    assert_receive(%{action: :do_produce, parameters: _parameters})
    assert_received(%{action: :save_batch})
    assert_received(%{action: :empty?, parameters: ^topic2})
    TestProducer.set_response(:do_produce, :ok)
  end

  test "produce simple collector by max wait" do
    topic8_config = KafkaBatcher.Test.SimpleCollectorMaxWaitTime.get_config()
    topic8 = TestProducer.topic_name(8)
    max_wait_time = Keyword.fetch!(topic8_config, :max_wait_time)

    events = generate_events(@template_events, 1)
    messages = Enum.map(events, fn event -> %MessageObject{key: "", value: Jason.encode!(event)} end)

    Enum.map(events, fn event -> {"", event} end)
    |> KafkaBatcher.Test.SimpleCollectorMaxWaitTime.add_events()

    assert_receive(%{action: :do_produce, parameters: parameters}, max_wait_time + 100)
    {^messages, ^topic8, _call_partition, ^topic8_config} = parameters
  end

  test "produce simple collector by max wait with producing failed" do
    topic8_config = KafkaBatcher.Test.SimpleCollectorMaxWaitTime.get_config()
    topic8 = TestProducer.topic_name(8)
    max_wait_time = Keyword.fetch!(topic8_config, :max_wait_time)

    events = generate_events(@template_events, 1)
    messages = Enum.map(events, fn event -> %MessageObject{key: "", value: Jason.encode!(event)} end)
    TestProducer.set_response(:do_produce, {:error, :kafka_unavailable})
    TestProducer.set_response(:empty?, false)

    Enum.map(events, fn event -> {"", event} end)
    |> KafkaBatcher.Test.SimpleCollectorMaxWaitTime.add_events()

    assert_receive(%{action: :do_produce, parameters: _parameters})

    Process.sleep(2 * max_wait_time)

    Enum.map(events, fn event -> {"", event} end)
    |> KafkaBatcher.Test.SimpleCollectorMaxWaitTime.add_events()

    TestProducer.set_response(:do_produce, :ok)

    assert_received(%{action: :empty?, parameters: ^topic8})

    assert_receive(%{action: :do_produce, parameters: parameters})
    {^messages, ^topic8, _call_partition, ^topic8_config} = parameters
  end

  test "produce simple collector with delay" do
    topic6_config = KafkaBatcher.Test.SimpleCollectorWithDelay.get_config()
    topic6 = TestProducer.topic_name(6)
    batch_size = Keyword.fetch!(topic6_config, :batch_size)

    events = generate_events(@template_events, batch_size)
    messages = Enum.map(events, fn event -> %MessageObject{key: "", value: Jason.encode!(event)} end)

    delay = Keyword.get(topic6_config, :min_delay)

    Enum.map(events, fn event -> {"", event} end)
    |> KafkaBatcher.Test.SimpleCollectorWithDelay.add_events()

    assert_receive(%{action: :do_produce, parameters: parameters})
    {^messages, ^topic6, _call_partition, ^topic6_config} = parameters

    Enum.map(events, fn event -> {"", event} end)
    |> KafkaBatcher.Test.SimpleCollectorWithDelay.add_events()

    refute_receive(%{action: :do_produce}, delay, "second call should be delayed")

    KafkaBatcher.Test.SimpleCollectorWithDelay.add_events([hd(messages)])
    assert_receive(%{action: :do_produce})
  end

  test "produce simple collector with max byte size control" do
    topic7_config = KafkaBatcher.Test.SimpleCollectorMaxByteSizeControl.get_config()
    topic7 = TestProducer.topic_name(7)
    batch_size = Keyword.fetch!(topic7_config, :batch_size)

    events =
      generate_events(@template_events, batch_size)
      |> Enum.map(fn event -> %MessageObject{key: "", value: Jason.encode!(event)} end)

    max_batch_bytesize = Keyword.fetch!(topic7_config, :max_batch_bytesize)

    {cnt_msg, _} =
      Enum.reduce(events, {1, 0}, fn event, {cnt, size} ->
        case size + :erlang.external_size(event) do
          new_size when new_size >= max_batch_bytesize ->
            {cnt + 1, 0}

          new_size ->
            {cnt, new_size}
        end
      end)

    KafkaBatcher.Test.SimpleCollectorMaxByteSizeControl.add_events(events)

    Enum.each(
      1..cnt_msg,
      fn _ ->
        assert_receive(%{action: :do_produce, parameters: parameters})
        {call_messages, ^topic7, _call_partition, topic7_config} = parameters
        sum = call_messages |> Enum.map(&:erlang.external_size/1) |> Enum.sum()
        assert sum <= Keyword.fetch!(topic7_config, :max_batch_bytesize)
      end
    )
  end

  test "produce with batch flusher" do
    event1 = %{
      "id" => "event2",
      "client_id" => "123",
      "type" => "Some type"
    }

    event2 = %{
      "id" => "event2",
      "client_id" => "123",
      "type" => "Flush Type"
    }

    topic3_config = KafkaBatcher.Test.BatchFlushCollector.get_config()
    topic3 = TestProducer.topic_name(3)

    expect_messages = [
      %MessageObject{headers: [], key: "", value: Jason.encode!(event1)},
      %MessageObject{headers: [], key: "", value: Jason.encode!(event2)}
    ]

    events = [event1, event2]

    Enum.each(events, fn event -> KafkaBatcher.Test.BatchFlushCollector.add_events([{"", event}]) end)

    assert_receive(%{action: :do_produce, parameters: parameters})
    {^expect_messages, ^topic3, _call_partition, ^topic3_config} = parameters
  end

  test "start accumulators fail" do
    topic_name = "topic_accumulators_fail"

    opts = [
      topic_name: topic_name,
      collect_by_partition: true,
      batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
      batch_size: 1,
      max_wait_time: 100,
      min_delay: 10,
      max_batch_bytesize: 200,
      partition_fn: fn _topic, partitions_count, key, _value -> :erlang.phash2(key, partitions_count) end
    ]

    {:ok, _sup_pid} = KafkaBatcher.AccumulatorsPoolSupervisor.start_link(opts)

    TestProducer.set_response(:get_partitions_count, {:error, "bad topic name"})
    TestProducer.set_notification_mode(:get_partitions_count, :on)
    TestProducer.set_notification_mode(:do_produce, :on)

    {:ok, _pid} = KafkaBatcher.Test.StartAccumulatorFail.start_link(opts)

    assert_receive(%{action: :get_partitions_count, parameters: ^topic_name}, 200)
    TestProducer.set_response(:get_partitions_count, {:ok, 1})
    assert_receive(%{action: :get_partitions_count, parameters: ^topic_name}, 200)

    event1 = %{
      "id" => "event2",
      "client_id" => "999",
      "type" => "Type"
    }

    message1 = %MessageObject{headers: [], key: "", value: Jason.encode!(event1)}
    ## :erlang.external_size(new_message) = 142 byte
    expect_messages = [message1]

    KafkaBatcher.Test.StartAccumulatorFail.add_events([message1, message1])
    assert_receive(%{action: :get_partitions_count, parameters: ^topic_name}, 200)
    assert_receive(%{action: :do_produce, parameters: parameters}, 200)
    {^expect_messages, ^topic_name, _call_partition, _config} = parameters

    ## After timeout
    assert_receive(%{action: :get_partitions_count, parameters: ^topic_name}, 200)
    assert_receive(%{action: :do_produce, parameters: parameters}, 200)
    {^expect_messages, ^topic_name, _call_partition, _config} = parameters
  end

  ## INTERNAL FUNCTIONS
  defp calc_partition(event, key, topic_name, partitions_count, topic1_config) do
    calc_fn = Keyword.fetch!(topic1_config, :partition_fn)
    calc_fn.(topic_name, partitions_count, key, event)
  end

  defp transform_messages() do
    fn {partition, events} ->
      {partition,
       Enum.map(events, fn {key, event} ->
         %MessageObject{headers: [], key: key, value: Jason.encode!(event)}
       end)}
    end
  end

  defp get_key_id(event) do
    event["client_id"] || event["device_id"]
  end

  ## grouping messages by partitions
  defp generate_events_by_group(grouped_messages, batch_size) do
    Enum.reduce(grouped_messages, %{}, fn {partition, messages}, acc ->
      new_messages = generate_events(messages, batch_size)
      Map.put(acc, partition, new_messages)
    end)
  end

  ## generating messages in an amount equal to the batch_size
  defp generate_events([message | _] = messages, batch_size) do
    case Enum.count(messages) do
      len when len < batch_size ->
        messages ++ List.duplicate(message, batch_size - len)

      len when len > batch_size ->
        Enum.slice(messages, 0..(batch_size - 1))

      _ ->
        messages
    end
  end

  defmodule BatchFlusher do
    @behaviour KafkaBatcher.Behaviours.BatchFlusher

    @impl KafkaBatcher.Behaviours.BatchFlusher
    def flush?(_key, %{"type" => "Flush Type"}) do
      true
    end

    def flush?(_key, _event) do
      false
    end
  end
end
