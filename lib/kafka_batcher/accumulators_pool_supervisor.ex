defmodule KafkaBatcher.AccumulatorsPoolSupervisor do
  @moduledoc """
  Manage dynamic pool of accumulators for the KafkaBatcher.Collector instance
  """

  use DynamicSupervisor

  alias KafkaBatcher.{Accumulator, DataStreamSpec, Producers}

  @dialyzer {:no_return, {:init, 1}}

  def start_link(%DataStreamSpec{} = data_stream_spec) do
    DynamicSupervisor.start_link(
      __MODULE__,
      data_stream_spec,
      name: reg_name(data_stream_spec)
    )
  end

  @doc "Returns a specification to start this module under a supervisor"
  def child_spec(%DataStreamSpec{} = data_stream_spec) do
    %{
      id: reg_name(data_stream_spec),
      start: {__MODULE__, :start_link, [data_stream_spec]},
      type: :supervisor
    }
  end

  def init(%DataStreamSpec{} = data_stream_spec) do
    %DataStreamSpec{
      accumulator_config: %Accumulator.Config{
        max_accumulator_restarts: max_accumulator_restarts
      }
    } = data_stream_spec

    # max_restarts value depends on partitions count in case when partitioned accumulation is used.
    # For example: 100 max_restarts -> 10 process restarts per second for 1 topic with 10 partitions
    DynamicSupervisor.init(
      strategy: :one_for_one,
      restart: :permanent,
      max_restarts: max_accumulator_restarts,
      max_seconds: 1,
      extra_arguments: []
    )
  end

  def start_accumulator(%DataStreamSpec{} = data_stream_spec) do
    DynamicSupervisor.start_child(
      reg_name(data_stream_spec),
      Accumulator.child_spec(data_stream_spec)
    )
  end

  def reg_name(%DataStreamSpec{} = data_stream_spec) do
    %DataStreamSpec{
      producer_config: %Producers.Config{client_name: client_name}
    } = data_stream_spec

    topic_name = DataStreamSpec.get_topic_name(data_stream_spec)

    :"#{__MODULE__}.#{client_name}.#{topic_name}"
  end
end
