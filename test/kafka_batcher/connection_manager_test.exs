defmodule ConnectionManagerTest do
  use ExUnit.Case, async: false

  import Mox

  alias KafkaBatcher.ConnectionManager
  alias KafkaBatcher.Producers.TestProducer

  @retry_timeout 100

  setup_all do
    set_mox_global()

    stub_with(
      KafkaBatcher.Accumulator.ProxyMock,
      KafkaBatcher.Accumulator.Proxy
    )

    prepare_producers()
    on_exit(fn -> Supervisor.stop(KafkaBatcher.Supervisor) end)
  end

  setup :verify_on_exit!

  setup do
    TestProducer.set_owner()

    on_exit(fn ->
      TestProducer.set_notification_mode(:start_client, :off)
      TestProducer.set_notification_mode(:start_producer, :off)
      TestProducer.set_response(:start_client, {:ok, :erlang.whereis(:user)})
      TestProducer.set_response(:start_producer, :ok)
    end)
  end

  def prepare_producers do
    KafkaBatcher.ProducerHelper.connection_manager_up()
    :ok
  end

  test "start client retry" do
    assert true == ConnectionManager.client_started?()

    TestProducer.set_response(:start_client, {:error, "failed connection"})
    TestProducer.set_notification_mode(:start_client, :on)
    TestProducer.set_notification_mode(:start_producer, :on)

    :ok = GenServer.stop(ConnectionManager)
    assert KafkaBatcher.ProducerHelper.ready_connection_manager?()

    assert_receive(%{action: :start_client}, 2 * @retry_timeout)
    TestProducer.set_response(:start_client, {:ok, Process.whereis(:user)})
    assert_receive(%{action: :start_client}, 2 * @retry_timeout)
    assert_receive(%{action: :start_producer}, 2 * @retry_timeout)
    assert true == ConnectionManager.client_started?()
  end

  test "start producer retry" do
    TestProducer.set_response(:start_client, {:ok, Process.whereis(:user)})
    TestProducer.set_notification_mode(:start_producer, :on)
    TestProducer.set_response(:start_producer, {:error, "failed connection"})

    :ok = GenServer.stop(ConnectionManager)
    assert KafkaBatcher.ProducerHelper.ready_connection_manager?()

    assert_receive(%{action: :start_producer}, 2 * @retry_timeout)
    TestProducer.set_response(:start_producer, :ok)
    assert_receive(%{action: :start_producer}, 2 * @retry_timeout)
    assert KafkaBatcher.ProducerHelper.ready_pool?()
  end
end
