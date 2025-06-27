defmodule KafkaBatcher.Accumulator.Proxy do
  @moduledoc """
    Default accumulator event proxy implementation
    Forwards message to the given accumulator GenServer
  """

  alias KafkaBatcher.Behaviours.Accumulator.Proxy

  @behaviour Proxy

  @impl true
  def call(server, message), do: GenServer.call(server, message)
end
