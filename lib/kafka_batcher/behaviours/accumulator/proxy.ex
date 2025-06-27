defmodule KafkaBatcher.Behaviours.Accumulator.Proxy do
  @moduledoc """
    Proxies an individual message to the accumulator server
  """

  @callback call(GenServer.server(), term()) :: :ok | {:error, term()}
end
