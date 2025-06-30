defmodule KafkaBatcher.Accumulators.FailingAccumulator do
  @moduledoc false
  use GenServer

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil)
  end

  @impl true
  def init(nil) do
    {:ok, nil}
  end

  @impl true
  def handle_call(_request, _from, _state) do
    throw(:timeout)
  end
end
