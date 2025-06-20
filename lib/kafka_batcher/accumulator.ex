defmodule KafkaBatcher.Accumulator do
  @moduledoc """
  Accumulator process is used to accumulate messages until Accumulator.State will go into "ready to producing" status.
  There are many conditions to detect this status, which can be configured through kafka_batcher settings.
  See details how it works in KafkaBatcher.Accumulator.State module
  """

  alias KafkaBatcher.{Accumulator.State, MessageObject, TempStorage}
  alias KafkaBatcher.Behaviours.Collector, as: CollectorBehaviour
  alias KafkaBatcher.Behaviours.Accumulator, as: AccumulatorBehaviour

  @behaviour AccumulatorBehaviour

  @error_notifier Application.compile_env(:kafka_batcher, :error_notifier, KafkaBatcher.DefaultErrorNotifier)
  @producer Application.compile_env(:kafka_batcher, :producer_module, KafkaBatcher.Producers.Kaffe)

  use GenServer
  require Logger

  @impl AccumulatorBehaviour
  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: reg_name(args))
  end

  @doc "Returns a specification to start this module under a supervisor"
  @impl AccumulatorBehaviour
  def child_spec(args) do
    %{
      id: reg_name(args),
      start: {__MODULE__, :start_link, [args]}
    }
  end

  @doc """
  Finds appropriate Accumulator process by topic & partition and dispatches `event` to it
  """
  @impl AccumulatorBehaviour
  def add_event(%MessageObject{} = event, topic_name, partition \\ nil) do
    GenServer.call(reg_name(topic_name: topic_name, partition: partition), {:add_event, event})
  end

  ##
  ## Callbacks
  ##
  @impl GenServer
  def init(args) do
    Process.flag(:trap_exit, true)
    state = build_state(args)

    Logger.debug("""
      KafkaBatcher: Accumulator process started: topic #{state.topic_name} partition #{state.partition} pid #{inspect(self())}
    """)

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:add_event, event}, _from, state) do
    now = System.os_time(:millisecond)

    state
    |> State.add_new_message(event, now)
    |> set_cleanup_timer_if_not_exists()
    |> produce_messages_if_ready()
    |> case do
      {:ok, new_state} -> {:reply, :ok, new_state}
      {:error, reason, new_state} -> {:reply, {:error, reason}, new_state}
    end
  end

  @impl GenServer
  def handle_info({:timeout, cleanup_timer_ref, :cleanup}, %State{cleanup_timer_ref: cleanup_timer_ref} = state) do
    state
    |> State.mark_as_ready()
    |> produce_messages_if_ready()
    |> case do
      {:ok, new_state} ->
        {:noreply, new_state}

      {:error, _reason, new_state} ->
        state.collector.set_lock()
        {:noreply, new_state}
    end
  end

  # handle the trapped exit call
  def handle_info({:EXIT, _from, reason}, state) do
    cleanup(state)
    {:stop, reason, state}
  end

  def handle_info(term, state) do
    Logger.warning("""
      KafkaBatcher: Unknown message #{inspect(term)} to #{__MODULE__}.handle_info/2.
      Current state: #{inspect(drop_sensitive(state))}
    """)

    {:noreply, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    cleanup(state)
  end

  @impl GenServer
  def format_status(_reason, [pdict, state]) do
    [pdict, drop_sensitive(state)]
  end

  defp drop_sensitive(%State{config: config} = state) do
    %State{state | config: Keyword.drop(config, [:sasl])}
  end

  defp cleanup(%{pending_messages: [], messages_to_produce: []}) do
    Logger.info("KafkaBatcher: Terminating #{__MODULE__}: there are no pending messages.")
  end

  defp cleanup(%{pending_messages: pending_messages, messages_to_produce: messages_to_produce} = state) do
    _ = handle_produce(Enum.reverse(pending_messages ++ messages_to_produce), state)
    Logger.info("KafkaBatcher: Terminating #{__MODULE__}")
  end

  defp set_cleanup_timer_if_not_exists(%State{cleanup_timer_ref: nil} = state) do
    ref = :erlang.start_timer(state.max_wait_time, self(), :cleanup)
    %State{state | cleanup_timer_ref: ref}
  end

  defp set_cleanup_timer_if_not_exists(%State{} = state), do: state

  defp produce_messages_if_ready(%State{messages_to_produce: []} = state), do: {:ok, state}

  defp produce_messages_if_ready(%State{messages_to_produce: messages_to_produce} = state) do
    handle_produce(Enum.reverse(messages_to_produce), state)
  end

  defp save_messages_to_temp_storage(messages, state) do
    TempStorage.save_batch(%TempStorage.Batch{
      messages: messages,
      topic: state.topic_name,
      partition: state.partition,
      producer_config: state.config
    })
  end

  defp handle_produce(pending_messages, state) do
    case produce_list(pending_messages, state) do
      :ok ->
        {:ok, State.reset_state_after_produce(state)}

      {:error, reason} ->
        @error_notifier.report(
          type: "KafkaBatcherProducerError",
          message: "event#produce topic=#{state.topic_name} partition=#{state.partition} error=#{inspect(reason)}"
        )

        save_messages_to_temp_storage(pending_messages, state)
        {:error, reason, State.reset_state_after_failure(state)}
    end
  end

  @spec produce_list(messages :: [CollectorBehaviour.event()], state :: State.t()) :: :ok | {:error, any()}
  defp produce_list(messages, state) when is_list(messages) do
    @producer.produce_list(messages, state.topic_name, state.partition, state.config)
  catch
    _, reason ->
      {:error, reason}
  end

  defp build_state(args) do
    config = Keyword.fetch!(args, :config)

    %State{
      topic_name: Keyword.fetch!(args, :topic_name),
      partition: Keyword.get(args, :partition),
      config: config,
      batch_flusher: Keyword.fetch!(config, :batch_flusher),
      batch_size: Keyword.fetch!(config, :batch_size),
      max_wait_time: Keyword.fetch!(config, :max_wait_time),
      min_delay: Keyword.fetch!(config, :min_delay),
      max_batch_bytesize: Keyword.fetch!(config, :max_batch_bytesize),
      collector: Keyword.fetch!(args, :collector)
    }
  end

  defp reg_name(args) do
    topic_name = Keyword.fetch!(args, :topic_name)

    case Keyword.get(args, :partition) do
      nil ->
        :"#{__MODULE__}.#{topic_name}"

      partition ->
        :"#{__MODULE__}.#{topic_name}.#{partition}"
    end
  end
end
