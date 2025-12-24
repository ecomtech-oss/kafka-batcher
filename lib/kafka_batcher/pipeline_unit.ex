defmodule KafkaBatcher.PipelineUnit do
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
  def drop_sensitive(%__MODULE__{} = pipeline_unit) do
    %__MODULE__{
      collector_config: pipeline_unit.collector_config,
      accumulator_config: pipeline_unit.accumulator_config,
      producer_config: Producers.Config.drop_sensitive(pipeline_unit.producer_config),
      opts: []
    }
  end

  @spec get_accumulator_mod(t()) :: module()
  def get_accumulator_mod(%__MODULE__{} = pipeline_unit) do
    %__MODULE__{
      accumulator_config: %Accumulator.Config{accumulator_mod: accumulator_mod}
    } = pipeline_unit

    accumulator_mod
  end

  @spec get_partition(t()) :: pos_integer() | nil
  def get_partition(%__MODULE__{} = pipeline_unit) do
    %__MODULE__{
      accumulator_config: %Accumulator.Config{partition: partition}
    } = pipeline_unit

    partition
  end

  @spec set_partition(t(), pos_integer()) :: t()
  def set_partition(%__MODULE__{} = pipeline_unit, partition) do
    %__MODULE__{
      pipeline_unit
      | accumulator_config: %Accumulator.Config{
          pipeline_unit.accumulator_config
          | partition: partition
        }
    }
  end

  @spec get_topic_name(t()) :: String.t()
  def get_topic_name(%__MODULE__{} = pipeline_unit) do
    %__MODULE__{
      collector_config: %Collector.Config{topic_name: topic_name}
    } = pipeline_unit

    topic_name
  end

  @spec get_collector(t()) :: module()
  def get_collector(%__MODULE__{} = pipeline_unit) do
    %__MODULE__{
      collector_config: %Collector.Config{collector: collector}
    } = pipeline_unit

    collector
  end

  @spec collect_by_partition?(t()) :: boolean()
  def collect_by_partition?(%__MODULE__{} = pipeline_unit) do
    %__MODULE__{
      collector_config: %Collector.Config{
        collect_by_partition: collect_by_partition
      }
    } = pipeline_unit

    collect_by_partition
  end
end
