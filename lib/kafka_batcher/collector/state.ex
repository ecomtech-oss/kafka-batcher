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

      {:error, reason, _failed_event_batches} ->
        {:error, reason, %State{state | locked?: true}}
    end
  end

  defp try_to_add_events(events, %State{} = state) do
    events
    |> Enum.map(&Utils.transform_event/1)
    |> Enum.reduce(:ok, &try_to_add_event(&1, &2, state))
  end

  defp try_to_add_event(event, :ok, %State{} = state) do
    case choose_partition(state, event) do
      {:ok, partition} ->
        case Accumulator.add_event(event, state.topic_name, partition) do
          :ok -> :ok
          {:error, reason} -> {:error, reason, %{partition => [event]}}
        end

      {:error, reason} ->
        {:error, reason, %{nil => [event]}}
    end
  end

  defp try_to_add_event(event, {:error, reason, failed_event_batches}, state) do
    partition =
      case choose_partition(state, event) do
        {:ok, partition} -> partition
        {:error, _reason} -> nil
      end

    {
      :error,
      reason,
      Map.update(failed_event_batches, partition, [], &[event | &1])
    }
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
         {:error, _reason, failed_event_batches} = result,
         %State{} = state
       ) do
    for {partition, failed_events} <- failed_event_batches do
      TempStorage.save_batch(%TempStorage.Batch{
        messages: Enum.reverse(failed_events),
        topic: state.topic_name,
        partition: partition,
        producer_config: state.config
      })
    end

    result
  end
end
