defmodule KafkaBatcher.Collector.State do
  @moduledoc """
  Describes the state of KafkaBatcher.Collector and functions working with it
  """

  alias KafkaBatcher.{Accumulator, Collector, TempStorage}
  alias KafkaBatcher.Collector.{State, Utils}

  require Logger

  @type t :: %State{
          topic_name: String.t() | nil,
          config: Keyword.t(),
          collect_by_partition: boolean(),
          collector: atom() | nil,
          locked?: boolean(),
          last_check_timestamp: non_neg_integer() | nil,
          ready?: boolean(),
          timer_ref: :timer.tref() | nil,
          partitions_count: pos_integer() | nil
        }

  @accumulator Application.compile_env(:kafka_batcher, :accumulator, Accumulator)

  defstruct topic_name: nil,
            config: [],
            collect_by_partition: true,
            collector: nil,
            # these fields are used to handle case when Kafka went down suddenly
            locked?: false,
            last_check_timestamp: nil,
            # these fields are used to handle case when Kafka is not available at the start
            ready?: false,
            timer_ref: nil,
            partitions_count: nil

  @spec add_events(t(), [Utils.event()]) :: {:ok, t()} | {:error, term(), t()}
  def add_events(%State{} = state, events) do
    case events |> try_to_add_events(state) |> save_failed_events(state) do
      :ok ->
        {:ok, state}

      {:error, {reason, _failed_events}} ->
        {:error, reason, %State{state | locked?: true}}
    end
  end

  defp try_to_add_events(events, %State{} = state) do
    Enum.reduce(events, :ok, fn
      event, :ok ->
        event = Utils.transform_event(event)

        case add_event(state, event) do
          :ok -> :ok
          {:error, reason} -> {:error, {reason, [event]}}
        end

      event, {:error, {reason, events}} ->
        {:error, {reason, [Utils.transform_event(event) | events]}}
    end)
  end

  defp add_event(%State{} = state, event) do
    with {:ok, partition} <- choose_partition(state, event) do
      @accumulator.add_event(event, state.topic_name, partition)
    end
  catch
    _, _reason ->
      Logger.warning("KafkaBatcher: Couldn't get through to accumulator #{@accumulator}")
      {:error, :accumulator_unavailable}
  end

  defp choose_partition(%State{collect_by_partition: true} = state, event) do
    Collector.Implementation.choose_partition(
      event,
      state.topic_name,
      state.config,
      state.partitions_count
    )
  end

  defp choose_partition(%State{collect_by_partition: false}, _event) do
    {:ok, nil}
  end

  defp save_failed_events(:ok, _state), do: :ok

  defp save_failed_events(
         {:error, {_reason, failed_events}} = result,
         %State{} = state
       ) do
    TempStorage.save_batch(%TempStorage.Batch{
      messages: Enum.reverse(failed_events),
      topic: state.topic_name,
      partition: nil,
      producer_config: state.config
    })

    result
  end
end
