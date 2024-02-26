defmodule KafkaBatcher.Behaviours.Collector do
  @moduledoc """
  Collector must implement add_events/1 callback to receive events.

  Event could be a MessageObject or a tuple with headers, key and value, but headers and key could be omitted
  """

  @type header_key() :: binary()
  @type header_value() :: binary()
  @type headers() :: [{header_key(), header_value()}]
  @type key :: binary() | nil
  @type value :: map() | binary()
  @type message_object :: KafkaBatcher.MessageObject.t()
  @type event :: {headers(), key(), value()} | {key(), value()} | value() | message_object()
  @type events :: list(event())

  @callback add_events(events :: events()) :: :ok | {:error, term()}
end
