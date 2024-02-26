defmodule KafkaBatcher.TempStorage.Batch do
  @moduledoc """
  The struct used for KafkaBatcher.Behaviours.TempStorage behavior
  """

  defstruct [:messages, :topic, :partition, :producer_config]

  @type message() :: KafkaBatcher.MessageObject.t()

  @type t() :: %__MODULE__{
          messages: [message()],
          topic: String.t(),
          partition: String.t() | nil,
          producer_config: Keyword.t()
        }
end
