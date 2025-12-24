defmodule KafkaBatcher.AccumulatorsPoolSupervisor do
  @moduledoc """
  Manage dynamic pool of accumulators for the KafkaBatcher.Collector instance
  """

  use DynamicSupervisor

  alias KafkaBatcher.{Accumulator, PipelineUnit, Producers}

  @dialyzer {:no_return, {:init, 1}}

  def start_link(%PipelineUnit{} = pipeline_unit) do
    DynamicSupervisor.start_link(
      __MODULE__,
      pipeline_unit,
      name: reg_name(pipeline_unit)
    )
  end

  @doc "Returns a specification to start this module under a supervisor"
  def child_spec(%PipelineUnit{} = pipeline_unit) do
    %{
      id: reg_name(pipeline_unit),
      start: {__MODULE__, :start_link, [pipeline_unit]},
      type: :supervisor
    }
  end

  def init(%PipelineUnit{} = pipeline_unit) do
    %PipelineUnit{
      accumulator_config: %Accumulator.Config{
        max_accumulator_restarts: max_accumulator_restarts
      }
    } = pipeline_unit

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

  def start_accumulator(%PipelineUnit{} = pipeline_unit) do
    DynamicSupervisor.start_child(
      reg_name(pipeline_unit),
      Accumulator.child_spec(pipeline_unit)
    )
  end

  def reg_name(%PipelineUnit{} = pipeline_unit) do
    %PipelineUnit{
      producer_config: %Producers.Config{client_name: client_name}
    } = pipeline_unit

    topic_name = PipelineUnit.get_topic_name(pipeline_unit)

    :"#{__MODULE__}.#{client_name}.#{topic_name}"
  end
end
