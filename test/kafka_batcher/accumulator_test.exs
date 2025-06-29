defmodule KafkaBatcher.AccumulatorTest do
  use ExUnit.Case, async: false
  use KafkaBatcher.Mocks

  alias KafkaBatcher.{Accumulator, MessageObject}
  alias KafkaBatcher.Producers.TestProducer
  alias KafkaBatcher.TempStorage.TestStorage

  setup_all do
    prepare_producers()
  end

  setup do
    prepare_mocks()
  end

  def prepare_producers do
    KafkaBatcher.ProducerHelper.connection_manager_up()
    :ok
  end

  def prepare_mocks do
    TestProducer.set_owner()
    TestProducer.set_notification_mode(:do_produce, :on)
    TestStorage.set_owner()
    TestStorage.set_notification_mode(:save_batch, :on)
    TestStorage.set_notification_mode(:empty?, :on)

    on_exit(fn ->
      TestProducer.set_notification_mode(:start_client, :off)
      TestProducer.set_notification_mode(:start_producer, :off)
      TestProducer.set_notification_mode(:do_produce, :off)
      TestStorage.set_notification_mode(:save_batch, :off)
      TestStorage.set_notification_mode(:empty?, :off)
    end)
  end

  test "accumulator cleanup with not empty batch" do
    topic_name = "topicForTerminate"
    partition_num = 1

    opts = [
      topic_name: topic_name,
      partition: partition_num,
      config: [
        batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
        batch_size: 10,
        max_wait_time: 10_000,
        min_delay: 10,
        max_batch_bytesize: 150
      ],
      collector: SomeCollector
    ]

    {:ok, pid} = Accumulator.start_link(opts)
    :erlang.unlink(pid)

    event = %MessageObject{value: "some_value", key: "some_key"}
    Accumulator.add_event(event, topic_name, partition_num)

    Process.exit(pid, :some_reason)
    assert_receive(%{action: :do_produce, parameters: parameters})
    {[^event], ^topic_name, ^partition_num, _} = parameters
  end
end
