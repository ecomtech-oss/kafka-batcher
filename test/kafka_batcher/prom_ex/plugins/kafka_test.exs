defmodule PromEx.Plugins.KafkaTest do
  use ExUnit.Case, async: false

  alias KafkaBatcher.PromEx.Plugins.Kafka
  alias PromEx.Test.Support.{Events, Metrics}

  defmodule WebApp.PromEx do
    use PromEx, otp_app: :elixir

    @impl true
    def plugins, do: [{Kafka, metric_prefix: [:prom_ex, :kafka]}]
  end

  test "The telemetry with Kafka plugin works for start/stop" do
    Application.put_env(:kafka_batcher, :kafka_topic_aliases, %{
      "my.incoming-events.topic-long-name" => "incoming-events"
    })

    start_supervised!(WebApp.PromEx)
    Events.execute_all(:kafka)

    metrics =
      WebApp.PromEx
      |> PromEx.get_metrics()
      |> Metrics.sort()

    assert Metrics.read_expected(:kafka) == metrics
  end

  describe "The event_metrics/1" do
    test "should return the correct number of metrics" do
      assert length(Kafka.event_metrics(otp_app: :prom_ex)) == 2
    end
  end

  describe "The polling_metrics/1" do
    test "should return the correct number of metrics" do
      assert Kafka.polling_metrics([]) == []
    end
  end

  describe "The manual_metrics/1" do
    test "should return the correct number of metrics" do
      assert Kafka.manual_metrics(otp_app: :prom_ex) == []
    end
  end
end
