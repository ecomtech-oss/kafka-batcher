# Changelog

All notable changes to this image will be documented in this file.
Please refer to https://keepachangelog.com/en/1.0.0/ for format.

## [Unreleased]

## [0.5.2] - 2024-02-20
- Set the lock when producing to Kafka fails during cleanup

## [0.5.1] - 2024-02-05
- Minor bugfixes

## [0.5.0] - 2024-01-16
- Major rewrite and refactoring
- Write documentation
- Add Dialyzer and credo to CI

## [0.4.1] - 2023-09-28
- Allow to set headers for messages produced to Kafka

## [0.4.0] - 2023-09-05
- Add `min_delay` and `max_batch_bytesize` configuration parameters. It allows to increase max throughput
- Implementation save events to external storage (Postgresql, for example), if kafka not available 

## [0.3.0] - 2023-06-19

- Add conditions on max byte size & min wait time for the batches
- Add cache counters & change cleanup timer logic
- Fix batch loss when node is going down
- Conditional compilation of KafkaEx
- Add dyalizer to CI
- Fix config parser without sasl parameters

## [0.2.1] - 2023-04-04

- Bump minimum version of Elixir to 1.14

## [0.2.1] - 2023-03-31

- SASL / SSL support

## [0.2.0] - 2023-01-19

### Changed
- Align metrics with the requirements.
