# Changelog

## Unreleased

### New featues

- Experimental ARM support
- Add `default => {...}` and `default "key" => "value` to patch
- Add `zstd` pre- and post-processors [#1100](https://github.com/tremor-rs/tremor-runtime/issues/1100)
- Remove `rental` from `Event` [#1031](https://github.com/tremor-rs/tremor-runtime/issues/1031) [#1037](https://github.com/tremor-rs/tremor-runtime/issues/1037)
- Put event raw payload into `Arc` to improve cloning perf

### Fixes

- Skip instead of fail EQC on out of repo PRs
- Ensure patch keys are strings to move runtime errors into the compiletime
- Fix issue with the token `"` being presented as a tick in errors
- Fix `heredoc_start` and `heredoc_end` showing up in error messages instead of `"""`
- Fix some errors in otel pb <-> json translation
- Fix windowed queries emitting events with `null` metadata on tick

## 0.11.4

### New features

- Add tests for patch feature in tremor-script [#721](https://github.com/tremor-rs/tremor-runtime/issues/721)
- Add onramp for SSE (Server Sent Events) [#885](https://github.com/tremor-rs/tremor-runtime/issues/885)
- Add raw flag in tremor-cli dbg [#854](https://github.com/tremor-rs/tremor-runtime/issues/854)

### Fixes

- Fix the Release archive and package build process
- Replace preprocess from tremor-cli dbg with --preprocess option [#1085](https://github.com/tremor-rs/tremor-runtime/issues/1085)

## 0.11.3

### New features

- Add tests for merge feature in tremor-script [#721](https://github.com/tremor-rs/tremor-runtime/issues/721)
- Add support for receiving TLS encrypted data via TCP onramp.
- Add support for sending TLS encrypted data via TCP offramp.

### Fixes

- Remove a number of transmutes
- Catalog remaining transmutes with related tickets
- Bump snmalloc-rs to 0.2.26 to ensure builds succeed on systems with libc < 2.25

## 0.11.2

### New features

- Add `op` key to KV offramp responses in order to differentiate responses by the command that triggered them
- Change format of KV offramp responses to a more unified structure.
- Add `KnownKey::map_*` functions to directly work on the `Value::Object`s inner `HashMap`, if available.
- Add `HEALTHCHECK` to Dockerfiles
- Improve printing for dot files
- Scan sub directories of ${CFG_DIR} for config files.
- Add offramp and onramp for [AMQP](https://www.amqp.org) with the [lapin](https://docs.rs/lapin/1.6.8/lapin/) package.

### Fixes

- KV offramp sends error responses for each failed command
- Ensure binding to first bind linked offramps to pipelines, then pipelines to offramps/onramps/pipeline, then onramps to pipelines to ensure events only start flowing when all downstreams are connected.
- Fix empty record pattern to only match records.
- Fix ws offramp not reconnecting after connection loss.
- Run tests in tremor-cli bin
- Switch operations of `tremor dbg lex` and `tremor dbg preprocess` as they did the job of the other.
- Fix heredoc preprocessing, which was messing up error reporting
- Fix false positives in cycle detection
- Include `cncf::otel` stdlib sources in deb package
- Add `/usr/local/share/tremor` to default `TREMOR_PATH` also for all packages as a well-known directory for custom tremor-script libraries and modules.
- Record the partition number assigned during rebalancing when running Kafka.
- Fix bug in HDR histogram implementation when using emit without reset.
- Fix bug in mean that invalid values would be counted as part of the total number of values.
- Avoid possible contraflow cycles via `system::metrics` pipeline, if a pipeline is connected to an output port of `system::metrics` pipeline.

## 0.11.1

### New features

- Add `tremor_value::structurize` convenience fn
- Change `qos::wal` operator to only require one of `max_elements` or `max_bytes` (using both is still possible).
- Add DNS sink
- Add syslog codec.
- Add `$udp.host` and `$udp.port` to allow controling udp packet destinations on a per event basis.
- Deprecate `udp.dst_*` config, introduce `udp.bind.*` config instead.
- Allow insights/contraflow events to traverse through multiple connected pipelines

- Add GCP Cloud Storage linked sink connector.
- Add textual-length-prefix pre and postprocessor.
- Add GCP Pubsub sink and source connector.

### Fixes

- Inform docker builders that lack of resources may crash builds
- Fix CI runners to work with caching
- Fix dependencies for `tremor-value` and `tremor-common`
- Fix docker entrypoint not forwarding arguments to tremor binary, unless `--` is used before them
- Fix match default clause only executing the last statement in the block.
- Kafka back to async with a timeout on waiting
- Fix `qos::wal` operator never cleaning up the last event from its storage
- Fix `generic::batch` operator swallowing the `transactional` status of an Event
- Fix windowed select queries not tracking the `transactional` status of an Event
- Fix windowed select queries not tracking the Events that constitute an outgoing aggregated event
- Avoid several offramps to swallow fail insights.
- Send correlation metadata for send error events in elastic sink
- Record the partition number assigned during rebalancing when running Kafka.

## 0.11.0

### New features

- CNCF OpenTelemetry source, sink and `cncf::otel` tremor-script library adds log, trace, metrics OpenTelemetry support
- Fixes module path function resolution for `tremor-cli doc` tool to use unified path resolution
- Removed the vsn.sh script which checks if the lockfile is up to date and replaces it with the --locked flag [#798](https://github.com/tremor-rs/tremor-runtime/pull/798)
- Allow using '\_' as seperators in numeric literals [#645](https://github.com/tremor-rs/tremor-runtime/issues/645)
- Refactor kafka metadata variables to be under a single record `$kafka`.
- Add support for Kafka message headers, available through the `$kafka.headers` metadata variable.
- Add the `cb` offramp for testing upstream circuit breaker behaviour [#779](https://github.com/tremor-rs/tremor-runtime/pull/779)
- Add the `kafka` onramp config `retry_failed_events` to acoid retrying failed events, and `polling_interval` to control how often kafka is polled for new messages if none were available previously [#779](https://github.com/tremor-rs/tremor-runtime/pull/779)
- Add `kv` connector with the supported operations `put`, `get`, `delete`, `scan`, `cas`.
- Add discord badge to README.md.
- Handle signals and terminate properly on Ctrl+C in docker [#806](https://github.com/tremor-rs/tremor-runtime/pull/806)
- Update to rust 1.50.0
- Emit error events to the `err` port on exceeding concurrent requests limit for `rest` and `elastic` offramps.
- Add the `max_groups` and `emit_empty_windows` settings on window definitions [#828](https://github.com/tremor-rs/tremor-runtime/pull/828)
- Restrict event and event metadata references in the `SELECT` clause of a windowed select statement [#828](https://github.com/tremor-rs/tremor-runtime/pull/828)
- Improve default visibility of tremor info logs from packages [#850](https://github.com/tremor-rs/tremor-runtime/pull/850)
- Include the request info of a response for a linked rest offramp, available through the `$response.request` metadata variable.
- Emit warnings when a window with `emit_empty_windows` without guards is used.
- Extend match to handle top-level `~` extractors in match [#834](https://github.com/tremor-rs/tremor-runtime/issues/834)
- Extend match to allow more assign expressions (i.e. `case a = ~ glob|snot*| =>` or `case v = _ =>`)
- Match pipeline improvements: tree search for `==`, grouping based on shared keys, re-ordering of exclusive case statements
- Optimize glob matches of the form `glob|snot*|` or `glob|*badger|` to cheaper prefix and suffix checks
- Remove warnings for match w/o default if a `_` case or a `v = _` case exists
- Add `--exprs-only` to `dbg ast` to not show metadata
- Add Delete and Update to ES sink [#822](https://github.com/tremor-rs/tremor-runtime/issues/822)
- Add more metadata to Kafka source [#874](https://github.com/tremor-rs/tremor-runtime/issues/874)
- Update to simd-json 0.4
- Add tests covering basic operations and string interpolation for tremor-script[#721](https://github.com/tremor-rs/tremor-runtime/issues/721)
- Add offramp and onramp for [NATS.io](https://nats.io/).
- Add a stdin onramp.
- Add tests covering arrays and records for tremor-script[#721](https://github.com/tremor-rs/tremor-runtime/issues/721)
- Support for non caching UDP sink [#900](https://github.com/tremor-rs/tremor-runtime/issues/900)

### Fixes

- Fix `kafka` onramp hanging with no message in the queue, leading to delayed offset commits [#779](https://github.com/tremor-rs/tremor-runtime/pull/779)
- Fail the `kafka` onramp if any of the configured topics could not be subscribed to [#779](https://github.com/tremor-rs/tremor-runtime/pull/779)
- Tremor no longer requires a home dir for operations that do not need a config [#782](https://github.com/tremor-rs/tremor-runtime/issues/782)
- Add performance section to PR template.
- Fix markdown for discord link.
- Fix lalrpop builds when extra folders exist.
- Refactor operator metrics collection to eliminate clones in most cases.
- Fix systemd spec file to load tremor files in `/etc/tremor/config` [#784](https://github.com/tremor-rs/tremor-runtime/issues/784)
- Do not crash when we can not execute a config file [#792](https://github.com/tremor-rs/tremor-runtime/issues/792)
- Sort the artefacts while running the benchmarks so that the benchmark run is more deterministic [#825](https://github.com/tremor-rs/tremor-runtime/issues/825)
- Remove the bench_pipe_passthrough_csv benchmark [#825](https://github.com/tremor-rs/tremor-runtime/issues/825)
- Fix a bug in the `test bench` command that was giving false negatives when the benchmarks were failing [#816](https://github.com/tremor-rs/tremor-runtime/pull/816)
- Fix time based windows to not emit empty windows anymore and to not error if they contain event references [#828](https://github.com/tremor-rs/tremor-runtime/pull/828)
- Do not commit an empty topic-partition-list in the kafka onramp and improve logging for better debugging
- Fix kafka consumer offset lag of at least `1` continually by using offset + 1 when committing.
- Fix issue where binary not (`!`) was not getting lexed correctly [#833](https://github.com/tremor-rs/tremor-runtime/issues/833)
- Fix missing ack/fail insight events with offramps that dont support guaranteed delivery (e.g. udp, stdout) [#870](https://github.com/tremor-rs/tremor-runtime/pull/870)
- Fix wrong error message for misconfigured UDP sinks

## 0.10.2

### Fixes

- Ensure blaster sends all events from the source [#759](https://github.com/tremor-rs/tremor-runtime/pull/759)
- Allow the use of const and custom functions using const in select queries [#749](https://github.com/tremor-rs/tremor-runtime/issues/749)
- Print hygenic errors when invalid `trickle` files are loaded in `server run -f ...` [#761](https://github.com/tremor-rs/tremor-runtime/issues/761)
- Ensure `elastic` offramp does not issue empty bulk requests.
- Avoid sending empty batches from the `batch` operator.
- Ensure `elastic` offramp includes event `payload` in every error response.

### New features

- Add discord connector to allow communicating with the discord API

## 0.10.1

### Fixes

- Update tremor-value to 0.2 to include binary changes and thus unbreak the 0.10 tremor-script crate

## 0.10.0

### Breaking Changes

- String interpolation is now done via `#{..}` instead of `{}`. `{` no longer needs to be escaped, but `\#{` needs an escape for literal `#{`.
- Emit tumbling windows based on `size` immediately when they are full. [#731](https://github.com/tremor-rs/tremor-runtime/pull/731)
- Emit tumbling windows based on time `interval` also when no event comes in but the interval passed [#731](https://github.com/tremor-rs/tremor-runtime/pull/731)
- Elasticsearch offramp `elastic`: change the config value `endpoints` to `nodes` [#732](https://github.com/tremor-rs/tremor-runtime/pull/732)

### New features

- Default to thin-lto for all builds (prior this was only done in docker)
- Automatically generate rpms and tarballs for releases.
- Update rust to 1.49.0
- Build deb packages
- Statically link openssl
- elastic sink now supports linked transports [#715](https://github.com/tremor-rs/tremor-runtime/pull/715)

### Fixes

- rewrite string interpolation to fix [#726](https://github.com/tremor-rs/tremor-runtime/issues/726)

## 0.9.4

### New features

- Extract simd_json::BorrowedValue into tremor specific tremor-value to allow extension of the type system.
- Introduce Binary datatype and binary semi literals `<< 1, 2, event, (3 + 7) >>`
- Introduce `base64` module for encoding and decoding binary to base64 encoded strings
- introduce `binary` module to work with binary data

### Fixes

- Terminate pipeline creation when a node already exists with the given name [#650](https://github.com/tremor-rs/tremor-runtime/issues/650)
- Fix visibility of pipeline metrics [#648](https://github.com/tremor-rs/tremor-runtime/pull/648)
- Fix panic upon usage of postgres ramps due to incompatbility with tokio and async-std runtime. [#641](https://github.com/tremor-rs/tremor-runtime/pull/641)
- Fix missing newlines when error line doesnt end with a newline [#676](https://github.com/tremor-rs/tremor-runtime/pull/676)
- Fix possible panic in random::integer and random::float when used with values <= 0. [#679](https://github.com/tremor-rs/tremor-runtime/pull/679)

## 0.9.3

### New features

- Add `prefix` and `raw` config options to `stdout` and `stderr` offramps [#637](https://github.com/tremor-rs/tremor-runtime/pull/637)

### Fixes

- Make use of postprocessors in `stdout`, `stderr` and `udp` sinks [#637](https://github.com/tremor-rs/tremor-runtime/pull/637)
- Allow to express minimal value of i64 as int literal [#629](https://github.com/tremor-rs/tremor-runtime/pull/629)
- Fix scientific float literals (e.g. `1.0e-5`) [#629](https://github.com/tremor-rs/tremor-runtime/pull/629)
- Output errors to stderr on `tremor run` invocation. [#629](https://github.com/tremor-rs/tremor-runtime/pull/629)
- Output more helpful errors for runtime errors (like accessing non-existing fields) [#629](https://github.com/tremor-rs/tremor-runtime/pull/629)
- Fix gelf preprocessor to accept valid (unchunked) gelf messages [#628](https://github.com/tremor-rs/tremor-runtime/pull/628)
- Fix memory access issue for large objects

## 0.9.2

### New Features

- Add docker image, and action for training docker image [#576](https://github.com/tremor-rs/tremor-runtime/pull/576)
- Performance enhancements [#608](https://github.com/tremor-rs/tremor-runtime/pull/608)

### Breaking Changes

- Standardize error responses for tremor api [#559](https://github.com/tremor-rs/tremor-runtime/pull/559)
- Remove empty event filtering from [lines](https://docs.tremor.rs/artefacts/preprocessors/#lines) preprocessor logic [#598](https://github.com/tremor-rs/tremor-runtime/pull/598)

### Fixes

- Fix possible crashes from todo macro in tcp sink [#573](https://github.com/tremor-rs/tremor-runtime/pull/573)
- Fix linked offramp not shutting down after binding is deleted [#582](https://github.com/tremor-rs/tremor-runtime/pull/582)
- Fix slow kafka sink when queue.buffering.max.ms is set to > 0 [#585](https://github.com/tremor-rs/tremor-runtime/pull/585)
- Fix string and heredoc errors with and without interpolation [#595](https://github.com/tremor-rs/tremor-runtime/pull/595)
- Add hygienic error feedback for `tremor run` [#620](https://github.com/tremor-rs/tremor-runtime/pull/620)
- Fix Kafka SmolRuntime hang [#558](https://github.com/tremor-rs/tremor-runtime/pull/558)
- Remove array access to prevent possible runtime panics [#574](https://github.com/tremor-rs/tremor-runtime/pull/574), [#598](https://github.com/tremor-rs/tremor-runtime/pull/598)
- Update CLI: -i allows selecting a subset now [#580](https://github.com/tremor-rs/tremor-runtime/pull/580)
- Allow using err for errors in tremor run [#592](https://github.com/tremor-rs/tremor-runtime/pull/592)
- Update to rust toolchain 1.48
