defmodule Test.Support.Behaviours.KafkaEx do
  @moduledoc false
  # FOR TEST ONLY
  @type client_id() :: atom()
  @type config() :: Keyword.t()
  @type metadata_response() :: map()
  @type produce_request() :: map()

  @callback create_worker(client_id(), config()) ::
              Supervisor.on_start_child()
  @callback metadata(Keyword.t()) :: metadata_response()
  @callback get_partitions_count(client_id(), binary()) ::
              {:ok, pos_integer()} | {:error, binary() | atom()}
  @callback produce(produce_request(), Keyword.t()) :: nil | :ok | {:ok, integer} | {:error, any}
end

defmodule Test.Support.Behaviours.KafkaEx.Metadata do
  @moduledoc false

  @callback partitions_for_topic(Test.Support.Behaviours.KafkaEx.metadata_response(), binary()) :: list()
end
