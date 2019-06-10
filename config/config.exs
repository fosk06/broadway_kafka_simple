# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
use Mix.Config
# config :kafka_broadway_simple,
# kafka_host: 'localhost',
# kafka_port: 9092,
# kafka_default_topic: "topic1",
# kafka_default_partition: 0,
# kafka_default_group_id: "group-id"


config :kafka_ex,
brokers: [
  {"localhost", 9092}
],
consumer_group: "group-id",
disable_default_worker: true,
sync_timeout: 3000,
max_restarts: 10,
max_seconds: 60,
commit_interval: 5_000,
commit_threshold: 100,
sleep_for_reconnect: 400,
use_ssl: false

# This configuration is loaded before any dependency and is restricted
# to this project. If another project depends on this project, this
# file won't be loaded nor affect the parent project. For this reason,
# if you want to provide default values for your application for
# third-party users, it should be done in your "mix.exs" file.

# You can configure your application as:
#
#     config :kafka_broadway_simple, key: :value
#
# and access this configuration in your application as:
#
#     Application.get_env(:kafka_broadway_simple, :key)
#
# You can also configure a third-party app:
#
#     config :logger, level: :info
#

# It is also possible to import configuration files, relative to this
# directory. For example, you can emulate configuration per environment
# by uncommenting the line below and defining dev.exs, test.exs and such.
# Configuration from the imported file will override the ones defined
# here (which is why it is important to import them last).
#
#     import_config "#{Mix.env()}.exs"
