defmodule KafkaBatcher.Accumulator do
  @moduledoc """
  Accumulator process is used to accumulate messages until Accumulator.State will go into "ready to producing" status.
  There are many conditions to detect this status, which can be configured through kafka_batcher settings.
  See details how it works in KafkaBatcher.Accumulator.State module
  """

  alias KafkaBatcher.{
    Accumulator,
    Accumulator.State,
    DataStreamSpec,
    MessageObject,
    Producers,
    TempStorage
  }

  alias KafkaBatcher.Behaviours.Collector, as: CollectorBehaviour

  @error_notifier Application.compile_env(:kafka_batcher, :error_notifier, KafkaBatcher.DefaultErrorNotifier)
  @producer Application.compile_env(:kafka_batcher, :producer_module, KafkaBatcher.Producers.Kaffe)

  use GenServer
  require Logger

  @spec start_link(DataStreamSpec.t()) :: GenServer.on_start()
  def start_link(%DataStreamSpec{} = data_stream_spec) do
    GenServer.start_link(
      __MODULE__,
      data_stream_spec,
      name: reg_name(data_stream_spec)
    )
  end

  @doc "Returns a specification to start this module under a supervisor"
  @spec child_spec(DataStreamSpec.t()) :: Supervisor.child_spec()
  def child_spec(%DataStreamSpec{} = data_stream_spec) do
    %{
      id: reg_name(data_stream_spec),
      start: {
        DataStreamSpec.get_accumulator_mod(data_stream_spec),
        :start_link,
        [data_stream_spec]
      }
    }
  end

  @doc """
  Finds appropriate Accumulator process by topic & partition and dispatches `event` to it
  """
  @spec add_event(MessageObject.t(), DataStreamSpec.t()) :: :ok | {:error, term()}
  def add_event(%MessageObject{} = event, %DataStreamSpec{} = data_stream_spec) do
    GenServer.call(reg_name(data_stream_spec), {:add_event, event})
  catch
    _, _reason ->
      Logger.warning("KafkaBatcher: Couldn't get through to accumulator")
      {:error, :accumulator_unavailable}
  end

  ##
  ## Callbacks
  ##
  @impl GenServer
  def init(%DataStreamSpec{} = data_stream_spec) do
    Process.flag(:trap_exit, true)

    topic_name = DataStreamSpec.get_topic_name(data_stream_spec)
    partition = DataStreamSpec.get_partition(data_stream_spec)

    Logger.debug("""
      KafkaBatcher: Accumulator process started: topic #{topic_name} partition #{partition} pid #{inspect(self())}
    """)

    {:ok, %State{data_stream_spec: data_stream_spec}}
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
        collector_mod = DataStreamSpec.get_collector_mod(state.data_stream_spec)
        collector_mod.set_lock()
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
      Current state: #{inspect(state)}
    """)

    {:noreply, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    cleanup(state)
  end

  @impl GenServer
  def format_status(_reason, [pdict, %State{} = state]) do
    [
      pdict,
      %State{
        state
        | data_stream_spec: DataStreamSpec.drop_sensitive(state.data_stream_spec)
      }
    ]
  end

  defp cleanup(%{pending_messages: [], messages_to_produce: []}) do
    Logger.info("KafkaBatcher: Terminating #{__MODULE__}: there are no pending messages.")
  end

  defp cleanup(%{pending_messages: pending_messages, messages_to_produce: messages_to_produce} = state) do
    _ = handle_produce(Enum.reverse(pending_messages ++ messages_to_produce), state)
    Logger.info("KafkaBatcher: Terminating #{__MODULE__}")
  end

  defp set_cleanup_timer_if_not_exists(%State{cleanup_timer_ref: nil} = state) do
    %DataStreamSpec{
      accumulator_config: %Accumulator.Config{max_wait_time: max_wait_time}
    } = state.data_stream_spec

    ref = :erlang.start_timer(max_wait_time, self(), :cleanup)
    %State{state | cleanup_timer_ref: ref}
  end

  defp set_cleanup_timer_if_not_exists(%State{} = state), do: state

  defp produce_messages_if_ready(%State{messages_to_produce: []} = state), do: {:ok, state}

  defp produce_messages_if_ready(%State{messages_to_produce: messages_to_produce} = state) do
    handle_produce(Enum.reverse(messages_to_produce), state)
  end

  defp save_messages_to_temp_storage(messages, state) do
    %DataStreamSpec{
      accumulator_config: %Accumulator.Config{} = accumulator_config,
      opts: opts
    } = state.data_stream_spec

    TempStorage.save_batch(%TempStorage.Batch{
      messages: messages,
      topic: accumulator_config.topic_name,
      partition: accumulator_config.partition,
      producer_config: opts
    })
  end

  defp handle_produce(pending_messages, state) do
    case produce_list(pending_messages, state) do
      :ok ->
        {:ok, State.reset_state_after_produce(state)}

      {:error, reason} ->
        %DataStreamSpec{
          accumulator_config: %Accumulator.Config{} = accumulator_config
        } = state.data_stream_spec

        @error_notifier.report(
          type: "KafkaBatcherProducerError",
          message:
            "event#produce topic=#{accumulator_config.topic_name} partition=#{accumulator_config.partition} error=#{inspect(reason)}"
        )

        save_messages_to_temp_storage(pending_messages, state)
        {:error, reason, State.reset_state_after_failure(state)}
    end
  end

  @spec produce_list(messages :: [CollectorBehaviour.event()], state :: State.t()) :: :ok | {:error, any()}
  defp produce_list(messages, state) when is_list(messages) do
    %DataStreamSpec{
      accumulator_config: %Accumulator.Config{} = accumulator_config
    } = spec = state.data_stream_spec

    @producer.produce_list(
      spec.producer_config,
      messages,
      accumulator_config.topic_name,
      accumulator_config.partition
    )
  catch
    _, reason ->
      {:error, reason}
  end

  defp reg_name(%DataStreamSpec{} = data_stream_spec) do
    %DataStreamSpec{
      producer_config: %Producers.Config{client_name: client_name}
    } = data_stream_spec

    topic_name = DataStreamSpec.get_topic_name(data_stream_spec)
    partition = DataStreamSpec.get_partition(data_stream_spec)

    case partition do
      nil ->
        :"#{__MODULE__}.#{client_name}.#{topic_name}"

      partition ->
        :"#{__MODULE__}.#{client_name}.#{topic_name}.#{partition}"
    end
  end
end
