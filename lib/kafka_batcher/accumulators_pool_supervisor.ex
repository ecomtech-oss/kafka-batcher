defmodule KafkaBatcher.AccumulatorsPoolSupervisor do
  @moduledoc """
  Manage dynamic pool of accumulators for the KafkaBatcher.Collector instance
  """

  use DynamicSupervisor

  alias KafkaBatcher.Accumulator
  @dialyzer {:no_return, {:init, 1}}
  @accumulator Application.compile_env(:kafka_batcher, :accumulator, Accumulator)

  def start_link(config) do
    DynamicSupervisor.start_link(__MODULE__, config, name: reg_name(config))
  end

  @doc "Returns a specification to start this module under a supervisor"
  def child_spec(config) do
    %{
      id: reg_name(config),
      start: {__MODULE__, :start_link, [config]},
      type: :supervisor
    }
  end

  def init(config) do
    # max_restarts value depends on partitions count in case when partitioned accumulation is used.
    # For example: 100 max_restarts -> 10 process restarts per second for 1 topic with 10 partitions
    DynamicSupervisor.init(
      strategy: :one_for_one,
      restart: :permanent,
      max_restarts: Keyword.get(config, :max_restart, 100),
      max_seconds: 1,
      extra_arguments: []
    )
  end

  def start_accumulator(args) do
    DynamicSupervisor.start_child(reg_name(args), @accumulator.child_spec(args))
  end

  def reg_name(args) do
    :"#{__MODULE__}.#{Keyword.fetch!(args, :topic_name)}"
  end
end
