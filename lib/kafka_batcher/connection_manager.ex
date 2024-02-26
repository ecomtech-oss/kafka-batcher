defmodule KafkaBatcher.ConnectionManager do
  @moduledoc """
  Abstraction layer over Kafka library.

  Manages a connection to Kafka, producers processes and does reconnect if something goes wrong.
  """

  require Logger

  defmodule State do
    @moduledoc "State of ConnectionManager process"
    defstruct client_started: false, client_pid: nil

    @type t :: %State{client_started: boolean(), client_pid: nil | pid()}
  end

  @producer Application.compile_env(:kafka_batcher, :producer_module, KafkaBatcher.Producers.Kaffe)
  @error_notifier Application.compile_env(:kafka_batcher, :error_notifier, KafkaBatcher.DefaultErrorNotifier)
  @reconnect_timeout Application.compile_env(:kafka_batcher, :reconnect_timeout, 5_000)

  use GenServer

  # Public API
  @spec start_link() :: :ignore | {:error, any()} | {:ok, pid()}
  def start_link() do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc "Returns a specification to start this module under a supervisor"
  @spec child_spec(nil) :: map()
  def child_spec(_ \\ nil) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, []},
      type: :worker
    }
  end

  @doc "Checks that Kafka client is already started"
  @spec client_started?() :: boolean()
  def client_started?() do
    GenServer.call(__MODULE__, :client_started?)
  end

  ##
  ## Callbacks
  ##

  @impl GenServer
  def init(_opts) do
    Process.flag(:trap_exit, true)
    {:ok, %State{}, {:continue, :start_client}}
  end

  @impl GenServer
  def handle_continue(:start_client, state) do
    {:noreply, connect(state)}
  end

  @impl GenServer
  def handle_call(:client_started?, _from, %State{client_started: started?} = state) do
    {:reply, started?, state}
  end

  def handle_call(msg, _from, state) do
    Logger.warning("KafkaBatcher: Unexpected call #{inspect(msg)}")
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_cast(_msg, state) do
    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:EXIT, pid, reason}, %State{client_pid: nil} = state) do
    Logger.info("""
      KafkaBatcher: Client was crashed #{inspect(pid)}, but reconnect is already in progress.
      Reason: #{inspect(reason)}
    """)

    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:EXIT, pid, reason}, %State{client_pid: pid} = state) do
    Logger.info("KafkaBatcher: Client was crashed #{inspect(pid)}. Reason #{inspect(reason)}. Trying to reconnect")
    state = connect(%State{state | client_pid: nil, client_started: false})
    {:noreply, state}
  end

  def handle_info(:reconnect, state) do
    Logger.info("KafkaBatcher: Retry connection")
    {:noreply, connect(state)}
  end

  def handle_info(msg, state) do
    Logger.warning("KafkaBatcher: Unexpected info #{inspect(msg)}")
    {:noreply, state}
  end

  @impl GenServer
  def terminate(reason, state) do
    Logger.info("KafkaBatcher: Terminating #{__MODULE__}. Reason #{inspect(reason)}")
    {:noreply, state}
  end

  ##
  ## INTERNAL FUNCTIONS
  ##

  defp connect(state) do
    case prepare_connection() do
      {:ok, pid} ->
        %State{state | client_started: true, client_pid: pid}

      :retry ->
        Process.send_after(self(), :reconnect, @reconnect_timeout)
        %State{state | client_started: false, client_pid: nil}
    end
  end

  defp start_producers() do
    KafkaBatcher.Config.get_configs_by_topic_name()
    |> Enum.reduce_while(:ok, fn {topic_name, config}, _ ->
      case @producer.start_producer(topic_name, config) do
        :ok ->
          {:cont, :ok}

        {:error, reason} ->
          @error_notifier.report(
            type: "KafkaBatcherProducerStartFailed",
            message: "Topic: #{topic_name}. Reason #{inspect(reason)}"
          )

          {:halt, :error}
      end
    end)
  end

  defp prepare_connection() do
    case start_client() do
      {:ok, pid} ->
        case start_producers() do
          :ok -> {:ok, pid}
          :error -> :retry
        end

      {:error, reason} ->
        @error_notifier.report(
          type: "KafkaBatcherClientStartFailed",
          message: "Kafka client start failed: #{inspect(reason)}"
        )

        :retry
    end
  end

  defp start_client() do
    case @producer.start_client() do
      {:ok, pid} ->
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        Logger.debug("KafkaBatcher: Kafka client already started: #{inspect(pid)}")
        {:ok, pid}

      error ->
        error
    end
  end
end
