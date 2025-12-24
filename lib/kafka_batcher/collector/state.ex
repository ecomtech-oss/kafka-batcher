defmodule KafkaBatcher.Collector.State do
  @moduledoc """
  Describes the state of KafkaBatcher.Collector and functions working with it
  """

  alias KafkaBatcher.{
    Accumulator,
    Collector,
    MessageObject,
    PipelineUnit,
    TempStorage
  }

  alias KafkaBatcher.Collector.{State, Utils}

  require Logger

  @type t :: %State{
          pipeline_unit: PipelineUnit.t(),
          locked?: boolean(),
          last_check_timestamp: non_neg_integer() | nil,
          ready?: boolean(),
          timer_ref: :timer.tref() | nil,
          partitions_count: pos_integer() | nil
        }

  @enforce_keys [:pipeline_unit]
  defstruct @enforce_keys ++
              [
                # these fields are used to handle case when Kafka went down suddenly
                locked?: false,
                last_check_timestamp: nil,
                # these fields are used to handle case when Kafka is not available at the start
                ready?: false,
                timer_ref: nil,
                partitions_count: nil
              ]

  @spec add_events(t(), [Utils.event()]) :: {:ok, t()} | {:error, term(), t()}
  def add_events(%State{} = state, events) do
    case events |> try_to_add_events(state) |> save_failed_events(state) do
      :ok ->
        {:ok, state}

      {:error, reason, _failed_event_batches} ->
        {:error, reason, %State{state | locked?: true}}
    end
  end

  defp try_to_add_events(events, %State{} = state) do
    events
    |> Enum.map(&Utils.transform_event/1)
    |> Enum.reduce(:ok, fn %MessageObject{} = event, result ->
      case choose_partition(state, event) do
        {:ok, partition} when result == :ok ->
          try_to_add_event(state, event, partition)

        {:ok, partition} ->
          keep_failed_event(result, event, elem(result, 1), partition)

        {:error, reason} ->
          keep_failed_event(result, event, reason, nil)
      end
    end)
  end

  defp try_to_add_event(%State{} = state, event, partition) do
    pipeline_unit = PipelineUnit.set_partition(state.pipeline_unit, partition)

    case Accumulator.add_event(event, pipeline_unit) do
      :ok -> :ok
      {:error, reason} -> keep_failed_event(:ok, event, reason, partition)
    end
  end

  defp keep_failed_event(:ok, event, reason, partition) do
    {:error, reason, %{partition => [event]}}
  end

  defp keep_failed_event(
         {:error, reason, failed_event_batches},
         event,
         _reason,
         partition
       ) do
    {
      :error,
      reason,
      Map.update(failed_event_batches, partition, [event], &[event | &1])
    }
  end

  defp choose_partition(%State{} = state, event) do
    if PipelineUnit.collect_by_partition?(state.pipeline_unit) do
      Collector.Implementation.choose_partition(
        event,
        state.pipeline_unit,
        state.partitions_count
      )
    else
      {:ok, nil}
    end
  end

  defp save_failed_events(:ok, _state), do: :ok

  defp save_failed_events(
         {:error, _reason, failed_event_batches} = result,
         %State{} = state
       ) do
    %State{pipeline_unit: %PipelineUnit{} = pipeline_unit} = state

    for {partition, failed_events} <- failed_event_batches do
      TempStorage.save_batch(%TempStorage.Batch{
        messages: Enum.reverse(failed_events),
        topic: PipelineUnit.get_topic_name(pipeline_unit),
        partition: partition,
        producer_config: pipeline_unit.opts
      })
    end

    result
  end
end
