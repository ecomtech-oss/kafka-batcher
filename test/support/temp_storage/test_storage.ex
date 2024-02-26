defmodule KafkaBatcher.TempStorage.TestStorage do
  @moduledoc false

  @behaviour KafkaBatcher.Behaviours.TempStorage
  use KafkaBatcher.ClientHelper, reg_name: __MODULE__
  use KafkaBatcher.MoxHelper, client: __MODULE__

  @impl true
  def empty?(topic_name) do
    process_callback(%{action: :empty?, parameters: topic_name}, true)
  end

  @impl true
  def save_batch(batch) do
    process_callback(%{action: :save_batch, parameters: batch}, :ok)
  end
end
