defmodule KafkaBatcher.Accumulator.State do
  @moduledoc """
  Encapsulates all logic to detect when batch will be ready to producing

  A batch is marked as ready for producing when one of the following conditions is met:

    * reached the max byte size of the batch
    * reached the batch size (messages count) limit
    * reached the waiting time limit (max delay before producing)
    * one of special events arrived (which triggers Flusher to produce immediately)
    * timer expired (in case when a few events arrived timer helps to control that the max waiting time is not exceeded)
  """

  alias KafkaBatcher.{Accumulator.State, MessageObject}
  @error_notifier Application.compile_env(:kafka_batcher, :error_notifier, KafkaBatcher.DefaultErrorNotifier)

  @type t :: %State{
          topic_name: binary(),
          partition: non_neg_integer() | nil,
          config: Keyword.t(),
          pending_messages: list(),
          last_produced_at: non_neg_integer(),
          batch_flusher: atom(),
          batch_size: non_neg_integer(),
          max_wait_time: non_neg_integer(),
          min_delay: non_neg_integer(),
          max_batch_bytesize: non_neg_integer(),
          batch_bytesize: non_neg_integer(),
          pending_messages_count: non_neg_integer(),
          producer_config: Keyword.t(),
          messages_to_produce: list(),
          cleanup_timer_ref: reference() | nil,
          status: atom(),
          collector: atom() | nil
        }

  defstruct topic_name: nil,
            partition: nil,
            config: [],
            pending_messages: [],
            last_produced_at: 0,
            batch_flusher: KafkaBatcher.Accumulator.DefaultBatchFlusher,
            batch_size: 0,
            max_wait_time: 0,
            min_delay: 0,
            max_batch_bytesize: 0,
            batch_bytesize: 0,
            pending_messages_count: 0,
            producer_config: [],
            messages_to_produce: [],
            cleanup_timer_ref: nil,
            status: :continue,
            collector: nil

  @spec add_new_message(State.t(), MessageObject.t(), non_neg_integer()) :: State.t()
  def add_new_message(%State{} = state, %MessageObject{key: key, value: value} = event, now) do
    new_message = %MessageObject{event | value: maybe_encode(value)}

    state
    |> consider_max_bytesize(new_message)
    |> consider_max_size_and_wait_time(now)
    |> consider_istant_flush(key, value)
  end

  @spec reset_state_after_failure(State.t()) :: State.t()
  def reset_state_after_failure(%State{} = state) do
    state = stop_timer(state)
    %State{state | status: :continue, messages_to_produce: []}
  end

  @spec reset_state_after_produce(State.t()) :: State.t()
  def reset_state_after_produce(%State{} = state) do
    now = System.os_time(:millisecond)
    state = stop_timer(state)
    %State{state | last_produced_at: now, messages_to_produce: [], status: :continue}
  end

  @spec mark_as_ready(State.t()) :: State.t()
  def mark_as_ready(%State{pending_messages: pending_messages, status: :continue} = state) do
    %State{
      state
      | status: :ready,
        messages_to_produce: pending_messages,
        pending_messages: [],
        pending_messages_count: 0,
        batch_bytesize: 0
    }
  end

  defp consider_max_bytesize(%State{status: :continue, batch_bytesize: batch_bytesize} = state, new_message) do
    message_size = :erlang.external_size(new_message)

    case batch_bytesize + message_size >= state.max_batch_bytesize do
      true when message_size >= state.max_batch_bytesize ->
        @error_notifier.report(
          type: "KafkaBatcherProducerError",
          message: """
          event#produce topic=#{state.topic_name} partition=#{state.partition}.
          Message size #{inspect(message_size)} exceeds limit #{inspect(state.max_batch_bytesize)}
          """
        )

        state

      true ->
        state |> mark_as_ready() |> put_to_pending(new_message)

      false ->
        put_to_pending(state, new_message)
    end
  end

  defp consider_max_size_and_wait_time(%State{status: :continue} = state, now) do
    if state.pending_messages_count >= state.batch_size and now - state.last_produced_at >= state.min_delay do
      mark_as_ready(state)
    else
      state
    end
  end

  defp consider_max_size_and_wait_time(%State{status: :ready} = state, _), do: state

  defp consider_istant_flush(%State{status: :continue} = state, key, value) do
    if state.batch_flusher.flush?(key, value) do
      mark_as_ready(state)
    else
      state
    end
  end

  defp consider_istant_flush(%State{status: :ready} = state, _, _), do: state

  defp put_to_pending(%State{} = state, new_message) do
    %State{
      state
      | pending_messages: [new_message | state.pending_messages],
        pending_messages_count: state.pending_messages_count + 1,
        batch_bytesize: state.batch_bytesize + :erlang.external_size(new_message)
    }
  end

  defp stop_timer(%__MODULE__{cleanup_timer_ref: cleanup_timer_ref} = state) when is_reference(cleanup_timer_ref) do
    :erlang.cancel_timer(cleanup_timer_ref)
    ## If the timer has expired before its cancellation, we must empty the
    ## mail-box of the 'timeout'-message.
    receive do
      {:timeout, ^cleanup_timer_ref, :cleanup} -> :ok
    after
      0 -> :ok
    end

    %__MODULE__{state | cleanup_timer_ref: nil}
  end

  defp stop_timer(state) do
    %__MODULE__{state | cleanup_timer_ref: nil}
  end

  defp maybe_encode(value) when is_binary(value) do
    value
  end

  defp maybe_encode(value) do
    Jason.encode!(value)
  end
end
