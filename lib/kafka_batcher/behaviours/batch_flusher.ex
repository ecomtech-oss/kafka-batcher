defmodule KafkaBatcher.Behaviours.BatchFlusher do
  @moduledoc "Determines whether we have to produce the batch immediately"

  @callback flush?(binary(), map()) :: boolean()
end
