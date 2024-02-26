defmodule KafkaBatcher.Supervisor do
  @moduledoc """
  The root of KafkaBatcher supervision tree
  Starts Collector & AccumulatorsPoolSupervisor for each configured collector
  """

  use Supervisor

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init(_args) do
    children = KafkaBatcher.Config.collectors_spec()

    opts = [strategy: :one_for_one]
    Supervisor.init(children, opts)
  end
end
