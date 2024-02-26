defmodule KafkaBatcher.Collector.Utils do
  @moduledoc """
  Provides functions for transforming events in different input formats into MessageObject struct
  """
  alias KafkaBatcher.MessageObject

  @spec prepare_events(list(any())) :: list(MessageObject.t())
  def prepare_events(events) do
    List.wrap(events)
    |> Enum.map(&transform_event/1)
  end

  @spec transform_event(binary() | {binary(), binary()} | {list(), binary(), binary()} | map() | MessageObject.t()) ::
          MessageObject.t()
  def transform_event(%MessageObject{} = event) do
    event
  end

  def transform_event(value) when is_map(value) or is_binary(value) do
    %MessageObject{value: value}
  end

  def transform_event({key, value}) do
    %MessageObject{key: key, value: value}
  end

  def transform_event({headers, key, value}) do
    %MessageObject{headers: headers, key: key, value: value}
  end
end
