defmodule KafkaBatcher.Collector do
  @moduledoc """
  Implementation of collector for incoming events.
  The collector accumulates events in accordance with a given strategy using accumulators supervised by AccumulatorsPoolSupervisor.
  The strategy is specified by the following parameters:
  * `:partition_strategy`, allows values: :random, :md5 or function (e.g. `fn _topic, _partitions_count, key, _value -> key end`)
  * `:partition_fn`, a function that takes 4 arguments and returns a number of partition (see example below)
  * `:collect_by_partition`, if set to `true`, producer accumulates messages separately for each partition of topic
  * `:batch_size`, count of messages to be accumulated before producing
  * `:max_wait_time`, max interval between producings in milliseconds. The batch will be produced to Kafka either by `batch_size` or by `max_wait_time` parameter.
  * `:batch_flusher`, a module implementing a `flush?/2` function. If the function returns true, the current batch will be sent to Kafka immediately.
  * `:min_delay` - optional parameter. Set minimal delay before send events. This parameter allowed to increase max throughput
  * `:max_batch_bytesize` - optional parameter. Allows to set a limit on the maximum batch size in bytes.

  A collector can be described as follows (for example):

      defmodule KafkaBatcher.Test.Handler1 do
        use KafkaBatcher.Collector,
          collect_by_partition: true,
          topic_key: :topic1,
          partition_fn: &KafkaBatcher.Test.Handler1.calculate_partition/4,
          required_acks: -1,
          batch_size: 30,
          max_wait_time: 20_000,
          min_delay: 0

        def calculate_partition(_topic, partitions_count, _key, value) do
          val = value["client_id"] || value["device_id"]
          :erlang.phash2(val, partitions_count)
        end
      end

  A collector can save events that cannot be sent to Kafka to external storage, such as a database.
  A storage is specified in the config.exs like this:

      config :kafka_batcher,
        storage_impl: KafkaBatcher.Storage.YourTempStorage

  """
  require Logger

  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      use GenServer
      require Logger

      alias KafkaBatcher.{
        AccumulatorsPoolSupervisor,
        Collector,
        Collector.State,
        DataStreamSpec,
        TempStorage
      }

      @behaviour KafkaBatcher.Behaviours.Collector
      import KafkaBatcher.Collector.Implementation

      @error_notifier Application.compile_env(:kafka_batcher, :error_notifier, KafkaBatcher.DefaultErrorNotifier)
      @compile_opts opts

      # Public API
      @spec start_link(DataStreamSpec.t()) :: GenServer.on_start()
      def start_link(%DataStreamSpec{} = spec) do
        GenServer.start_link(__MODULE__, spec, name: __MODULE__)
      end

      @doc "Returns a specification to start this module under a supervisor"
      def child_spec(%DataStreamSpec{} = spec) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [spec]},
          type: :worker
        }
      end

      def add_event(event), do: add_events([event])

      @impl KafkaBatcher.Behaviours.Collector
      def add_events(events) do
        GenServer.call(__MODULE__, {:add_events, events})
      catch
        _, _reason ->
          Logger.warning("KafkaBatcher: Couldn't get through to collector #{__MODULE__}")
          {:error, :kafka_unavailable}
      end

      @doc """
      Set the lock mode after a produce error in the topic
      """
      def set_lock do
        send(__MODULE__, :set_lock)
      end

      @doc """
      Retrieves the collector config
      """
      def get_config do
        GenServer.call(__MODULE__, :get_config)
      end

      def get_compile_opts do
        @compile_opts
      end

      # Callbacks
      @impl GenServer
      def init(%DataStreamSpec{} = data_stream_spec) do
        Process.flag(:trap_exit, true)

        topic_name = DataStreamSpec.get_topic_name(data_stream_spec)

        Logger.debug("KafkaBatcher: Batch collector started: topic #{topic_name} pid #{inspect(self())}")
        send(self(), :init_accumulators)

        {:ok, %State{data_stream_spec: data_stream_spec}}
      end

      @impl GenServer
      def handle_call({:add_events, events}, _from, %State{ready?: false} = state) when is_list(events) do
        {:reply, {:error, :kafka_unavailable}, state}
      end

      @impl GenServer
      def handle_call({:add_events, events}, from, %State{locked?: true} = state) when is_list(events) do
        # If the temporal storage is empty - then Kafka is available, so we can unlock state and process handling.
        # In another case we don't want accumulate more messages in the memory, so we return an error to the caller.
        case TempStorage.check_storage(state) do
          %State{locked?: false} = new_state ->
            handle_call({:add_events, events}, from, new_state)

          new_state ->
            {:reply, {:error, :kafka_unavailable}, new_state}
        end
      end

      def handle_call({:add_events, events}, _from, %State{} = state) do
        case State.add_events(state, events) do
          {:ok, state} -> {:reply, :ok, state}
          {:error, reason, state} -> {:reply, {:error, reason}, state}
        end
      end

      def handle_call(:get_config, _from, %State{} = state) do
        {:reply, state.data_stream_spec, state}
      end

      def handle_call(unknown, _from, state) do
        @error_notifier.report(
          type: "KafkaBatcherUnknownMessageCall",
          message: "#{__MODULE__} doesn't have a handle_call handler for #{inspect(unknown)}"
        )
      end

      @impl GenServer
      def handle_info(:init_accumulators, state) do
        new_state = store_partition_count(state)

        case start_accumulators(new_state) do
          :ok ->
            Logger.debug("KafkaBatcher: Started accumulators for topic #{__MODULE__}")
            {:noreply, %State{new_state | ready?: true}}

          {:error, reason} ->
            Logger.info("KafkaBatcher: Failed to start accumulators. Topic #{__MODULE__}. Reason #{inspect(reason)}")
            ref = restart_timer(new_state)
            {:noreply, %State{new_state | timer_ref: ref, ready?: false}}
        end
      end

      def handle_info(:set_lock, state) do
        {:noreply, %State{state | locked?: true}}
      end

      @impl GenServer
      def handle_info(msg, state) do
        Logger.error("KafkaBatcher: Unexpected info #{inspect(msg)}")
        {:noreply, state}
      end

      @impl GenServer
      def terminate(reason, state) do
        Logger.info("KafkaBatcher: Terminating #{__MODULE__}. Reason #{inspect(reason)}")
        {:noreply, state}
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

      # Private functions

      defp restart_timer(%State{timer_ref: ref}) when :erlang.is_reference(ref) do
        _ = :erlang.cancel_timer(ref)
        do_restart()
      end

      defp restart_timer(_state) do
        do_restart()
      end

      defp do_restart do
        timeout = Application.get_env(:kafka_batcher, :reconnect_timeout, 5_000)
        :erlang.send_after(timeout, self(), :init_accumulators)
      end
    end
  end
end
