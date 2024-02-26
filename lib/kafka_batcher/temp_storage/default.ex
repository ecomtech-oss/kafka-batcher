defmodule KafkaBatcher.TempStorage.Default do
  @moduledoc """
  Default implementation of KafkaBatcher.Behaviours.TempStorage

  It just logs the messages. To have more fault tolerant implementation
  you should implement your own logic to save these messages into some persistent storage.
  """

  @behaviour KafkaBatcher.Behaviours.TempStorage

  require Logger

  @impl KafkaBatcher.Behaviours.TempStorage
  def save_batch(%KafkaBatcher.TempStorage.Batch{topic: topic, partition: partition, messages: messages}) do
    Logger.error("""
      KafkaBatcher: Failed to send #{inspect(Enum.count(messages))} messages to the kafka topic #{topic}##{partition}
    """)

    Enum.each(messages, fn message -> Logger.info(inspect(message)) end)

    :ok
  end

  @impl KafkaBatcher.Behaviours.TempStorage
  def empty?(_topic) do
    true
  end
end
