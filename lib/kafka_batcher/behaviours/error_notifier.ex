defmodule KafkaBatcher.Behaviours.ErrorNotifier do
  @moduledoc """
  ErrorNotifier behaviour is used to report errors to external error-monitoring systems.

  In the most base implementation it could just log errors.
  """

  @callback report(exception :: Exception.t() | [type: String.t(), message: String.t()], options :: Keyword.t()) :: :ok
end
