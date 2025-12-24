defmodule KafkaBatcher.Accumulator do
  @moduledoc """
  Accumulator process is used to accumulate messages until Accumulator.State will go into "ready to producing" status.
  There are many conditions to detect this status, which can be configured through kafka_batcher settings.
  See details how it works in KafkaBatcher.Accumulator.State module
  """

  alias KafkaBatcher.{
    Accumulator,
    Accumulator.State,
    MessageObject,
    PipelineUnit,
    Producers,
    TempStorage
  }

  alias KafkaBatcher.Behaviours.Collector, as: CollectorBehaviour

  @error_notifier Application.compile_env(:kafka_batcher, :error_notifier, KafkaBatcher.DefaultErrorNotifier)
  @producer Application.compile_env(:kafka_batcher, :producer_module, KafkaBatcher.Producers.Kaffe)

  use GenServer
  require Logger

  @spec start_link(PipelineUnit.t()) :: GenServer.on_start()
  def start_link(%PipelineUnit{} = pipeline_unit) do
    GenServer.start_link(
      __MODULE__,
      pipeline_unit,
      name: reg_name(pipeline_unit)
    )
  end

  @doc "Returns a specification to start this module under a supervisor"
  @spec child_spec(PipelineUnit.t()) :: Supervisor.child_spec()
  def child_spec(%PipelineUnit{} = pipeline_unit) do
    %{
      id: reg_name(pipeline_unit),
      start: {
        PipelineUnit.get_accumulator_mod(pipeline_unit),
        :start_link,
        [pipeline_unit]
      }
    }
  end

  @doc """
  Finds appropriate Accumulator process by topic & partition and dispatches `event` to it
  """
  @spec add_event(MessageObject.t(), PipelineUnit.t()) :: :ok | {:error, term()}
  def add_event(%MessageObject{} = event, %PipelineUnit{} = pipeline_unit) do
    GenServer.call(reg_name(pipeline_unit), {:add_event, event})
  catch
    _, _reason ->
      Logger.warning("KafkaBatcher: Couldn't get through to accumulator")
      {:error, :accumulator_unavailable}
  end

  ##
  ## Callbacks
  ##
  @impl GenServer
  def init(%PipelineUnit{} = pipeline_unit) do
    Process.flag(:trap_exit, true)

    topic_name = PipelineUnit.get_topic_name(pipeline_unit)
    partition = PipelineUnit.get_partition(pipeline_unit)

    Logger.debug("""
      KafkaBatcher: Accumulator process started: topic #{topic_name} partition #{partition} pid #{inspect(self())}
    """)

    {:ok, %State{pipeline_unit: pipeline_unit}}
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
        PipelineUnit.get_collector(state.pipeline_unit).set_lock()
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
        | pipeline_unit: PipelineUnit.drop_sensitive(state.pipeline_unit)
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
    %PipelineUnit{
      accumulator_config: %Accumulator.Config{max_wait_time: max_wait_time}
    } = state.pipeline_unit

    ref = :erlang.start_timer(max_wait_time, self(), :cleanup)
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
    @producer.produce_list(state.config, messages, state.topic_name, state.partition)
  catch
    _, reason ->
      {:error, reason}
  end

  defp reg_name(%PipelineUnit{} = pipeline_unit) do
    %PipelineUnit{
      producer_config: %Producers.Config{client_name: client_name}
    } = pipeline_unit

    topic_name = PipelineUnit.get_topic_name(pipeline_unit)
    partition = PipelineUnit.get_partition(pipeline_unit)

    case partition do
      nil ->
        :"#{__MODULE__}.#{client_name}.#{topic_name}"

      partition ->
        :"#{__MODULE__}.#{client_name}.#{topic_name}.#{partition}"
    end
  end
end
