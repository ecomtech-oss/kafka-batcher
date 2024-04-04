defmodule KafkaBatcher.TempStorageTest do
  use ExUnit.Case, async: false

  alias Uniq.UUID
  alias KafkaBatcher.Collector.State, as: CollectorState
  alias KafkaBatcher.TempStorage
  alias KafkaBatcher.TempStorage.TestStorage

  @recheck_kafka_availability_interval Application.compile_env(
                                         :kafka_batcher,
                                         :recheck_kafka_availability_interval,
                                         5_000
                                       )

  test "Check that TempStorage doesn't git the underlying storage more often than once in @recheck_kafka_availability_interval" do
    TestStorage.set_owner()
    TestStorage.set_notification_mode(:empty?, :on)
    TestStorage.set_response(:empty?, false)

    now = System.os_time(:millisecond)
    topic = UUID.uuid4()

    locked_state = TempStorage.check_storage(%CollectorState{topic_name: topic, locked?: true})
    assert Map.get(locked_state, :locked?) === true
    assert Map.get(locked_state, :last_check_timestamp) >= now
    assert_received %{action: :empty?, parameters: ^topic}

    # wait a bit, but less than @recheck_kafka_availability_interval
    Process.sleep(2)
    assert TempStorage.check_storage(locked_state) === locked_state

    refute_received %{action: :empty?, parameters: ^topic},
                    "Should be called only once during @recheck_kafka_availability_interval"

    # let's pretend that we checked stogare more than @recheck_kafka_availability_interval ms ago
    old_state = Map.put(locked_state, :last_check_timestamp, now - @recheck_kafka_availability_interval)
    new_state = TempStorage.check_storage(old_state)

    assert new_state !== old_state, "last_check_timestamp should be updated"
    assert Map.delete(new_state, :last_check_timestamp) === Map.delete(old_state, :last_check_timestamp)

    assert_received %{action: :empty?, parameters: ^topic},
                    "Should be called because passed more time than recheck_kafka_availability_interval"

    TestStorage.set_response(:empty?, true)
    TestStorage.set_notification_mode(:empty?, :off)
  end
end
