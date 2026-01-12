defmodule KafkaBatcher.DataStreamSpec do
  @moduledoc false

  alias KafkaBatcher.{
    Accumulator,
    Collector,
    Producers
  }

  @type t :: %__MODULE__{
          collector_config: Collector.Config.t(),
          accumulator_config: Accumulator.Config.t(),
          producer_config: Producers.Config.t(),
          # opts are kept for backwards compatibility
          opts: Keyword.t()
        }

  @derive {
    Inspect,
    only: [:collector_config, :accumulator_config, :producer_config]
  }

  @enforce_keys [
    :collector_config,
    :accumulator_config,
    :producer_config,
    :opts
  ]
  defstruct @enforce_keys

  @spec drop_sensitive(t()) :: t()
  def drop_sensitive(%__MODULE__{} = data_stream_spec) do
    %__MODULE__{
      collector_config: data_stream_spec.collector_config,
      accumulator_config: data_stream_spec.accumulator_config,
      producer_config: Producers.Config.drop_sensitive(data_stream_spec.producer_config),
      opts: []
    }
  end

  @spec get_accumulator_mod(t()) :: module()
  def get_accumulator_mod(%__MODULE__{} = data_stream_spec) do
    %__MODULE__{
      accumulator_config: %Accumulator.Config{accumulator_mod: accumulator_mod}
    } = data_stream_spec

    accumulator_mod
  end

  @spec get_partition(t()) :: pos_integer() | nil
  def get_partition(%__MODULE__{} = data_stream_spec) do
    %__MODULE__{
      accumulator_config: %Accumulator.Config{partition: partition}
    } = data_stream_spec

    partition
  end

  @spec set_partition(t(), pos_integer()) :: t()
  def set_partition(%__MODULE__{} = data_stream_spec, partition) do
    %__MODULE__{
      data_stream_spec
      | accumulator_config: %Accumulator.Config{
          data_stream_spec.accumulator_config
          | partition: partition
        }
    }
  end

  @spec get_topic_name(t()) :: String.t()
  def get_topic_name(%__MODULE__{} = data_stream_spec) do
    %__MODULE__{
      collector_config: %Collector.Config{topic_name: topic_name}
    } = data_stream_spec

    topic_name
  end

  @spec get_collector_mod(t()) :: module()
  def get_collector_mod(%__MODULE__{} = data_stream_spec) do
    %__MODULE__{
      collector_config: %Collector.Config{collector_mod: collector_mod}
    } = data_stream_spec

    collector_mod
  end

  @spec collect_by_partition?(t()) :: boolean()
  def collect_by_partition?(%__MODULE__{} = data_stream_spec) do
    %__MODULE__{
      collector_config: %Collector.Config{
        collect_by_partition: collect_by_partition
      }
    } = data_stream_spec

    collect_by_partition
  end
end
