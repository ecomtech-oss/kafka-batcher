[
  %{
    event: [:prom_ex, :plugin, :kafka, :producer],
    measurements: %{
      system_time: 1_654_972_577_479_492_414,
      duration: 100_115_245,
      batch_size: 200,
      batch_byte_size: 3000
    },
    metadata: %{
      topic: "my.incoming-events.topic-long-name",
      partition: 0
    }
  },
  %{
    event: [:prom_ex, :plugin, :kafka, :producer],
    measurements: %{
      system_time: 1_654_972_577_479_692_414,
      duration: 50_115_245,
      batch_size: 100,
      batch_byte_size: 1500
    },
    metadata: %{
      topic: "my.incoming-events.topic-long-name",
      partition: 1
    }
  },
  %{
    event: [:prom_ex, :plugin, :kafka, :producer],
    measurements: %{
      system_time: 1_654_972_577_479_992_414,
      duration: 500_115_100,
      batch_size: 1000,
      batch_byte_size: 5000
    },
    metadata: %{
      topic: "my.incoming-events.topic-long-name",
      partition: 2
    }
  },
  %{
    event: [:prom_ex, :plugin, :kafka, :consumer],
    measurements: %{
      system_time: 1_654_972_577_480_992_414,
      duration: 100_115_245,
      batch_size: 200,
      batch_byte_size: 3000
    },
    metadata: %{
      topic: "my.incoming-events.topic-long-name",
      partition: 0
    }
  },
  %{
    event: [:prom_ex, :plugin, :kafka, :consumer],
    measurements: %{
      system_time: 1_654_972_577_481_992_414,
      duration: 50_115_245,
      batch_size: 100,
      batch_byte_size: 1500
    },
    metadata: %{
      topic: "my.incoming-events.topic-long-name",
      partition: 1
    }
  },
  %{
    event: [:prom_ex, :plugin, :kafka, :consumer],
    measurements: %{
      system_time: 1_654_972_577_482_992_414,
      duration: 500_115_100,
      batch_size: 1000,
      batch_byte_size: 5000
    },
    metadata: %{
      topic: "my.incoming-events.topic-long-name",
      partition: 2
    }
  }
]
