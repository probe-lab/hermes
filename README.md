# hermes

[![ProbeLab](https://img.shields.io/badge/made%20by-ProbeLab-blue.svg)](https://probelab.io)
[![Build status](https://img.shields.io/github/actions/workflow/status/plprobelab/hermes/go-test.yml?branch=main)](https://github.com/plprobelab/hermes/actions)
[![GoDoc](https://pkg.go.dev/badge/github.com/plprobelab/hermes)](https://pkg.go.dev/github.com/plprobelab/hermes)

> A Gossipsub listener and tracer.

## Checkout

Check out the repository:

```shell
git clone git@github.com:probe-lab/hermes.git
```

## Install

```sh
go get github.com/plprobelab/hermes
```

## Developing

### CLI Arguments

We use dot notation to namespace command line arguments instead of hyphens because the CLI library allows
to configuration parameters from a file which will then resolve the command line parameters. For example,
the CLI flag `--log.format` could be read from a yaml file that looks like this:

```yaml
log:
  format: json
```

_However_, Hermes currently doesn't support loading CLI arguments from a file ¯\_(ツ)_/¯.

### Tracing

Run an OpenTelemetry Collector compatible trace collector like Jaeger:

```shell
docker run --name jaeger \                                                                                                                                                                                                                                                                                                    7s 
  -e COLLECTOR_OTLP_ENABLED=true \
  -p 16686:16686 \
  -p 4317:4317 \
  --rm jaegertracing/all-in-one:1.54
```

You can find the UI at [`http://localhost:16686`](http://localhost:16686). Port `4317` is used by Hermes to export traces to.

Run Hermes with the `--tracing` flag. To change the address of the trace collector, you can also specify `--tracing.addr` and `--tracing.port`.

## Maintainers

- [Dennis Trautwein](https://github.com/dennis-tra)
- [Guillaume Michel](https://github.com/guillaumemichel)
- [Ian Davis](https://github.com/iand)

## Contributing

Contributions are welcome!

Please take a look at [the issues](https://github.com/probe-lab/hermes/issues).

This repository is part of the IPFS project and therefore governed by
our [contributing guidelines](https://github.com/ipfs/community/blob/master/CONTRIBUTING.md).

## License

[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)
