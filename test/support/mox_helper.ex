defmodule KafkaBatcher.MoxHelper do
  @moduledoc false

  defmodule State do
    @moduledoc false

    defstruct responses: %{},
              owner: nil,
              notifications: %{}

    def fetch(data, key), do: Map.fetch(data, key)

    def get_and_update(data, key, func) do
      Map.get_and_update(data, key, func)
    end

    def pop(data, key), do: Map.pop(data, key)
  end

  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      defp client() do
        Keyword.fetch!(unquote(Macro.escape(opts)), :client)
      end

      def set_response(action, response) do
        client().get_state()
        |> put_in([:responses, action], response)
        |> client().set_state()
      end

      def set_owner() do
        client().get_state()
        |> Map.put(:owner, self())
        |> client().set_state()
      end

      def set_notification_mode(action, mode) when mode in [:on, :off] do
        client().get_state()
        |> put_in([:notifications, action], mode)
        |> client().set_state()

        if mode == :off do
          flush(action)

          client().get_state()
          |> put_in([:responses, action], nil)
          |> client().set_state()
        end
      end

      defp flush(action) do
        receive do
          %{action: ^action} ->
            flush(action)
        after
          0 -> :ok
        end
      end

      defp process_callback(event, response \\ nil) do
        send_notification(event)
        handle_response(event, response)
      end

      defp send_notification(%{action: action} = event) do
        case client().get_state() do
          %{owner: owner, notifications: notifications} ->
            if Map.get(notifications, action) == :on do
              send(owner, event)
            end

          _ ->
            :ok
        end
      end

      defp handle_response(%{action: action} = event, response) do
        case client().get_state() do
          %{responses: responses} ->
            case Map.get(responses, action) do
              nil -> response
              new_response -> new_response
            end

          _ ->
            response
        end
      end
    end
  end
end
