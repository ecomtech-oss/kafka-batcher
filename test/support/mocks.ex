Mox.defmock(KafkaBatcher.BrodMock, for: Test.Support.Behaviours.Brod)
Mox.defmock(KafkaBatcher.KafkaExMock, for: Test.Support.Behaviours.KafkaEx)
Mox.defmock(KafkaBatcher.KafkaEx.MetadataMock, for: Test.Support.Behaviours.KafkaEx.Metadata)

defmodule KafkaBatcher.Mocks do
  @moduledoc false
  @spec __using__(any()) :: {:__block__, [], [{:import, [...], [...]} | {:setup, [...], [...]}, ...]}
  defmacro __using__(_opts) do
    quote do
      import Mox
      setup :verify_on_exit!
      setup :set_mox_from_context
    end
  end
end

defmodule KafkaBatcher.ProducerHelper do
  alias KafkaBatcher.Producers.TestProducer
  alias KafkaBatcher.TempStorage.TestStorage

  @moduledoc false
  use ExUnit.Case

  def init do
    ## The call start_client explicitly, to avoid a race when restarting the ConnectionManager
    TestProducer.init()
    TestStorage.init()
  end

  def connection_manager_up do
    case Process.whereis(KafkaBatcher.Supervisor) do
      nil ->
        {:ok, sup_pid} = KafkaBatcher.Supervisor.start_link([])
        :erlang.unlink(sup_pid)

      pid when is_pid(pid) ->
        :ok
    end

    ## ready starting pool producers
    assert ready_connection_manager?()
    assert ready_pool?()
    :ok
  end

  def ready_connection_manager? do
    ready_connection_manager?(10)
  end

  defp ready_connection_manager?(0) do
    false
  end

  defp ready_connection_manager?(cnt) do
    case Process.whereis(KafkaBatcher.ConnectionManager) do
      nil ->
        Process.sleep(100)
        ready_connection_manager?(cnt - 1)

      _pid ->
        true
    end
  end

  def ready_pool? do
    ready_pool?(10)
  end

  defp ready_pool?(0) do
    false
  end

  defp ready_pool?(cnt) do
    case KafkaBatcher.ConnectionManager.client_started?() do
      true ->
        true

      false ->
        Process.sleep(100)
        ready_pool?(cnt - 1)
    end
  catch
    _, _reason ->
      Process.sleep(100)
      ready_pool?(cnt - 1)
  end
end
