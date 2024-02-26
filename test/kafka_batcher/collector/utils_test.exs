defmodule KafkaBatcher.Collector.UtilsTest do
  use ExUnit.Case
  alias KafkaBatcher.Collector.Utils
  alias KafkaBatcher.MessageObject

  test "transform events" do
    assert [%MessageObject{key: "key", value: "value"}] == Utils.prepare_events({"key", "value"})

    assert [
             %MessageObject{key: "key1", value: "value1"},
             %MessageObject{key: "key2", value: "value2"}
           ] == Utils.prepare_events([{"key1", "value1"}, {"key2", "value2"}])

    assert [
             %MessageObject{headers: "header1", key: "key1", value: "value1"},
             %MessageObject{key: "key2", value: "value2"}
           ] == Utils.prepare_events([{"header1", "key1", "value1"}, {"key2", "value2"}])

    assert [
             %MessageObject{headers: "header1", key: "key1", value: "value1"},
             %MessageObject{value: "value2"}
           ] == Utils.prepare_events([{"header1", "key1", "value1"}, "value2"])

    assert [
             %MessageObject{headers: "header1", key: "key1", value: "value1"},
             %MessageObject{value: %{some_value: "value2"}}
           ] == Utils.prepare_events([{"header1", "key1", "value1"}, %{some_value: "value2"}])

    assert_raise FunctionClauseError, fn -> Utils.prepare_events([{"header1", "key1", "value1"}, []]) end
  end
end
