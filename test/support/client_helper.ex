defmodule KafkaBatcher.ClientHelper do
  @moduledoc false
  use Agent

  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      defp reg_name do
        Keyword.fetch!(unquote(Macro.escape(opts)), :reg_name)
      end

      def init do
        start_link()
      end

      def start_link do
        Agent.start_link(fn -> %KafkaBatcher.MoxHelper.State{} end, name: reg_name())
      end

      def set_state(response) do
        Agent.update(reg_name(), fn _ -> response end)
      end

      def get_state do
        Agent.get(reg_name(), & &1)
      end
    end
  end
end
