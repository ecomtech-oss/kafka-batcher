defmodule Test.Support.Behaviours.Brod do
  @moduledoc false
  # FOR TEST ONLY
  @type portnum :: pos_integer()
  @type hostname :: binary() | :inet.hostname() | :inet.ip_address()
  @type endpoint :: {hostname(), portnum()}
  @type endpoints :: list(endpoint())
  @type client_id() :: atom()
  @type config() :: :proplists.proplist()

  @callback start_link_client(endpoints(), client_id(), config()) ::
              :ok | {:error, any()}
  @callback start_producer(client_id(), binary(), config()) ::
              :ok | {:error, any()}
  @callback get_partitions_count(client_id(), binary()) ::
              {:ok, pos_integer()} | {:error, binary() | atom()}
  @callback produce_sync(client_id(), binary(), non_neg_integer(), binary(), list({binary(), binary()})) ::
              :ok | {:error, any()}
end
