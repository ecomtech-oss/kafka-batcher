defmodule KafkaBatcher.Collector.State do
  @moduledoc """
  Describes the state of KafkaBatcher.Collector and functions working with it
  """

  alias KafkaBatcher.{Accumulator, Collector.State, Collector.Utils}

  defstruct topic_name: nil,
            config: [],
            collect_by_partition: true,
            collector: nil,
            # these fields are used to handle case when Kafka went down suddenly
            locked?: false,
            last_check_timestamp: nil,
            # these fields are used to handle case when Kafka is not available at the start
            ready?: false,
            timer_ref: nil

  def add_events(state, events) do
    reply =
      Enum.reduce_while(events, :ok, fn event, _status ->
        case add_event(Utils.transform_event(event), state) do
          :ok -> {:cont, :ok}
          error -> {:halt, error}
        end
      end)

    apply_add_event_reply(state, reply)
  end

  defp add_event(event, %State{collect_by_partition: true, config: config, topic_name: topic_name}) do
    case KafkaBatcher.Collector.Implementation.calculate_partition(event, topic_name, config) do
      {:ok, partition} ->
        Accumulator.add_event(event, topic_name, partition)

      error ->
        error
    end
  end

  defp add_event(event, %State{collect_by_partition: false} = state) do
    Accumulator.add_event(event, state.topic_name)
  end

  defp apply_add_event_reply(state, :ok) do
    {:ok, state}
  end

  defp apply_add_event_reply(state, {:error, reason}) do
    {:error, reason, %State{state | locked?: true}}
  end
end
