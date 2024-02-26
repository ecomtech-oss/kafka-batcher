defmodule KafkaBatcher.DefaultErrorNotifier do
  @moduledoc """
  Default implementation of the `KafkaBatcher.Behaviours.ErrorNotifier` behaviour.

  It just logs the errors. You can implement your own implementation to send errors to some error monitoring system.
  """
  require Logger
  @behaviour KafkaBatcher.Behaviours.ErrorNotifier

  @impl KafkaBatcher.Behaviours.ErrorNotifier
  def report(exception, options \\ []) do
    Logger.error([inspect(exception), "\n", inspect(options)])
  end
end
