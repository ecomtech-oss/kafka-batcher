# This file is responsible for configuring your application
# and its dependencies with the aid of the Config module.
#
# This configuration file is loaded before any dependency and
# is restricted to this project.

# General application configuration
import Config

config :kafka_batcher,
  producer_module: KafkaBatcher.Producers.Kaffe,
  storage_impl: KafkaBatcher.TempStorage.Default

config :kafka_ex, :disable_default_worker, true

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
#
import_config "#{config_env()}.exs"
