defmodule KafkaBatcher do
  @moduledoc """

  ```mermaid
  flowchart TD
      S -->|one for all collectors| CM(ConnectionManager)
      S -->|reg_name by topic| APS[AccumulatorsPoolSupervisor]
      S[Supervisor] -->|reg_name by topic| C(Collector)
      APS --> Accumulator0
      APS -->|starts Accumulator for each partition| Accumulator1
      APS --> Accumulator2
  ```

  or only one Accumulator in case when `collect_by_partition: false`
  ```mermaid
  flowchart TD
      S -->|one for all collectors| CM(ConnectionManager)
      S -->|reg_name by topic| APS[AccumulatorsPoolSupervisor]
      S[Supervisor] -->|reg_name by topic| C(Collector)
      APS --> Accumulator
  ```

  Sequence in case when Kafka is avaliable:
  ```mermaid
  sequenceDiagram
      actor U as LibraryUser
      participant C as Collector
      participant A as Accumulator
      participant P as Producer
      U ->> C: add_events/1
      C->>A: dispatch by partitions & add_event/3
      A->>A: accumulate events
      A->>P: when conditions met call producer
      P->>Kafka: produce_sync
      Kafka->>P: :ok
      P->>A: :ok
      A->>A: reset_state_after_produce/2
  ```

  Sequence in case when Kafka is unavaliable:
  ```mermaid
  sequenceDiagram
      actor U as LibraryUser
      participant C as Collector
      participant A as Accumulator
      participant P as Producer
      U ->> C: add_events/1
      C->>A: dispatch by partitions & add_event/3
      A->>A: accumulate events
      A->>P: when conditions met call producer
      P->>Kafka: produce_sync
      Kafka->>P: unavailable
      P->>A: {:error, reason}
      A->>TempStorage: save_batch/1
  ```
  """

  defmodule MessageObject do
    @moduledoc """
    Contains Kafka message fields

    ## Fields

      * `:key` - Kafka use it for partitioning
      * `:value` - the main payload of the message
      * `:headers` - a keyword list with auxiliary key-value pairs
    """
    defstruct key: "", value: "", headers: []

    @type t :: %MessageObject{key: binary(), value: map() | binary(), headers: list()}
  end
end
