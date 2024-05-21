# Hermes

[![ProbeLab](https://img.shields.io/badge/made%20by-ProbeLab-blue.svg)](https://probelab.io)
[![Build status](https://img.shields.io/github/actions/workflow/status/probe-lab/hermes/go-test.yml?branch=main)](https://github.com/probe-lab/hermes/actions)
[![GoDoc](https://pkg.go.dev/badge/github.com/probe-lab/hermes)](https://pkg.go.dev/github.com/probe-lab/hermes)

Hermes is a GossipSub listener and tracer. It subscribes to all relevant pubsub topics
and traces all protocol interactions. As of `2024-05-21`, Hermes supports the Ethereum
network.

## Table of Contents

- [Hermes](#hermes)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
  - [Developing](#developing)
    - [CLI Arguments](#cli-arguments)
  - [Deployment](#deployment)
    - [General](#general)
    - [Ethereum](#ethereum)
  - [Telemetry](#telemetry)
    - [Metrics](#metrics)
    - [Tracing](#tracing)
  - [Differences with other tools](#differences-with-other-tools)
  - [Maintainers](#maintainers)
  - [Contributing](#contributing)
  - [License](#license)

## Installation

```sh
go install github.com/probe-lab/hermes@latest
```

## Developing

Check out the repository:

```shell
git clone git@github.com:probe-lab/hermes.git
```

Start Hermes by running

```shell
go run ./cmd/hermes # requires Go >=1.22
```

<details>
<summary>This should print this help text</summary>

```text
NAME:
   hermes - a gossipsub listener

USAGE:
   hermes [global options] command [command options] 

COMMANDS:
   eth, ethereum  Listen to gossipsub topics of the Ethereum network
   help, h        Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --help, -h  show help

   Kinesis Configuration:

   --kinesis.region value  The region of the AWS Kinesis Data Stream [$HERMES_KINESIS_REGION]
   --kinesis.stream value  The name of the AWS Kinesis Data Stream [$HERMES_KINESIS_DATA_STREAM]

   Logging Configuration:

   --log.format value  Sets the format to output the log statements in: text, json, hlog, tint (default: "hlog") [$HERMES_LOG_FORMAT]
   --log.level value   Sets an explicity logging level: debug, info, warn, error. Takes precedence over the verbose flag. (default: "info") [$HERMES_LOG_LEVEL]
   --log.nocolor       Whether to prevent the logger from outputting colored log statements (default: false) [$HERMES_LOG_NO_COLOR]
   --log.source        Compute the source code position of a log statement and add a SourceKey attribute to the output. Only text and json formats. (default: false) [$HERMES_LOG_SOURCE]
   --verbose, -v       Set logging level more verbose to include debug level logs (default: false) [$HERMES_VERBOSE]

   Telemetry Configuration:

   --metrics             Whether to expose metrics information (default: false) [$HERMES_METRICS_ENABLED]
   --metrics.addr value  Which network interface should the metrics endpoint bind to. (default: "localhost") [$HERMES_METRICS_ADDR]
   --metrics.port value  On which port should the metrics endpoint listen (default: 6060) [$HERMES_METRICS_PORT]
   --tracing             Whether to emit trace data (default: false) [$HERMES_TRACING_ENABLED]
   --tracing.addr value  Where to publish the traces to. (default: "localhost") [$HERMES_TRACING_ADDR]
   --tracing.port value  On which port does the traces collector listen (default: 4317) [$HERMES_TRACING_PORT]
```

</details>

### CLI Arguments

We use dot notation to namespace command line arguments instead of hyphens because [the CLI library](https://github.com/urfave/cli) allows
to configuration parameters from a file which will then resolve the command line parameters. For example,
the CLI flag `--log.format` could be read from a yaml file that looks like this:

```yaml
log:
  format: json
```

_However_, Hermes currently doesn't support loading CLI arguments from a file ¯\_(ツ)_/¯.

## Deployment

Depending on the network you want to trace Hermes requires auxiliary infrastructure.
As of `2024-03-27`, Hermes supports these networks:

- [Ethereum](#ethereum)

### General

Hermes currently only supports emitting events to [AWS Kinesis](https://aws.amazon.com/kinesis/). We're using our [own producer library](https://github.com/dennis-tra/go-kinesis) to do that.

In order to hook up Hermes with AWS Kinesis you need to provide the following command line flags:

- `--kinesis.region=us-east-1` # us-east-1 is the default
- `--kinesis.stream=$YOUR_DATA_STREAM_NAME`

If the name is set, Hermes will start pumping events to that stream.

> It's important to note that the events **will not** be strictly ordered. They will only follow a lose ordering. The reasons are 1) potential retries of event submissions and 2) [record aggregation](https://docs.aws.amazon.com/streams/latest/dev/kinesis-kpl-concepts.html#kinesis-kpl-concepts-aggretation). Depending on the configured number of submission retries the events should be ordered within each 30s time window.

### Ethereum

In order to run Hermes in the Ethereum network you are required to run a Prysm beacon node. Hermes requires Prysm over other implementations because as far as [I](https://github.com/dennis-tra) know it's the only implementation that allows dynamic registration of trusted peers. For example, Lighthouse requires the list of trusted peers at boot time.

Further, Hermes requires beacon node in general because it delegates certain requests to it. In the case of the Ethereum network Hermes forwards all requests for the following protocols:

- `/eth2/beacon_chain/req/beacon_blocks_by_range/2/ssz_snappy`
- `/eth2/beacon_chain/req/beacon_blocks_by_root/2/ssz_snappy`
- `/eth2/beacon_chain/req/blob_sidecars_by_range/1/ssz_snappy`
- `/eth2/beacon_chain/req/blob_sidecars_by_root/1/ssz_snappy`

These request handlers require knowledge of the chain state wherefore Hermes cannot handle them itself. Still, they are required to be a good citizen of the network and not get pruned by other peers.

To run Hermes for the Ethereum network you would need to point it to the beacon node by providing the

- `--prysm.host=1.2.3.4`
- `--prysm.port.http=3500` # 3500 is the default
- `--prysm.port.grpc=4000` # 4000 is the default

command line flags to the `hermes ethereum` subcommand. Check out the help page via `hermes ethereum --help` for configuration options of the libp2p host or devp2p local node (e.g., listen addrs/ports).

<details>
<summary>Hermes' Ethereum Help Page</summary>

```text
NAME:
   hermes eth - Listen to gossipsub topics of the Ethereum network

USAGE:
   hermes eth command [command options] 

COMMANDS:
   ids      generates peer identities in csv format
   chains   List all supported chains
   help, h  Shows a list of commands or help for one command

OPTIONS:
   --key value, -k value      The private key for the hermes libp2p/ethereum node in hex format. [$HERMES_ETH_KEY]
   --chain value              The beacon chain to participate in. (default: "mainnet") [$HERMES_ETH_CHAIN]
   --attnets value, -a value  The attestation network digest. (default: "ffffffffffffffff") [$HERMES_ETH_ATTNETS]
   --dial.concurrency value   The maximum number of parallel workers dialing other peers in the network (default: 16) [$HERMES_ETH_DIAL_CONCURRENCY]
   --dial.timeout value       The request timeout when contacting other network participants (default: 5s) [$HERMES_ETH_DIAL_TIMEOUT]
   --devp2p.host value        Which network interface should devp2p (discv5) bind to. (default: "127.0.0.1") [$HERMES_ETH_DEVP2P_HOST]
   --devp2p.port value        On which port should devp2p (disv5) listen (default: random) [$HERMES_ETH_DEVP2P_PORT]
   --libp2p.host value        Which network interface should libp2p bind to. (default: "127.0.0.1") [$HERMES_ETH_LIBP2P_HOST]
   --libp2p.port value        On which port should libp2p (disv5) listen (default: random) [$HERMES_ETH_LIBP2P_PORT]
   --prysm.host value         The host ip/name where Prysm's (beacon) API is accessible [$HERMES_ETH_PRYSM_HOST]
   --prysm.port.http value    The port on which Prysm's beacon nodes' Query HTTP API is listening on (default: 3500) [$HERMES_ETH_PRYSM_PORT_HTTP]
   --prysm.port.grpc value    The port on which Prysm's gRPC API is listening on (default: 4000) [$HERMES_ETH_PRYSM_PORT_GRPC]
   --max-peers value          The maximum number of peers we want to be connected with (default: 30) [$HERMES_ETH_MAX_PEERS]
   --help, -h                 show help

```

</details>


## Telemetry

### Metrics

When you provide the `--metrics` command line flag Hermes will expose a Prometheus HTTP endpoint at `localhost:6060`. Host and port are configurable via `--metrics.addr` and `--metrics.port`.

### Tracing

Run an OpenTelemetry Collector compatible trace collector like Jaeger:

```shell
docker run --name jaeger \
  -e COLLECTOR_OTLP_ENABLED=true \
  -p 16686:16686 \
  -p 4317:4317 \
  --rm jaegertracing/all-in-one:1.54
```

You can find the UI at [`http://localhost:16686`](http://localhost:16686). Port `4317` is used by Hermes to export traces to.

Run Hermes with the `--tracing` flag. To change the address of the trace collector, you can also specify `--tracing.addr` and `--tracing.port`.

## Differences with other tools
Hermes jumps to the web3/blockchain/libp2p ecosystem with a pretty consolidated tooling around it, such as the existing variety of network crawlers or light clients for most mature networks. Despite it could look competence for other toolings, there was still a large incentive to develop it. 
Hermes was designed to behave as a light node in each supported network, where, on top of being an honest participant supporting all the protocols and RPC endpoints, allowing the streaming of custom internal events (mostly Libp2p-related).

It avoids updating and maintaining a custom fork of existing full/light clients, which complicates the control of events and upgradeability of new features.    

Currently available similar tools:

### Armiarma Crawler from MigaLabs
Although both tools seem to be focusing on the same goals at first sight, they have significant differences in their use cases and their targets in data collection and metrics.

[Armiarma](https://github.com/migalabs/armiarma) is a network crawler that relies on running discv5 peer discovery service and a libp2p host 24/7 to establish connections. However, significant modifications have been made to connect to as many peers as possible (custom peering module). This way, it tries to identify as many peers as possible in the network periodically. Thus, its focus is mainly on opening and identifying as many peers as possible, rather than maintaining stable connections to other peers in the network.

On the other hand, although Hermes also relies on a continuously running discv5 and libp2p host, it uses the libp2p connection manager for a different purpose to Armiarma (which is to connect to as many peers as possible). In the case of Hermes, the connection manager is used to decide who it connects to (i.e., simulating the behaviour of a standard node). Furthermore, it backs up some of the RPC, which requires keeping the chain db calls on a trusted node. This way, it behaves like a light node to the network, which is honest and beneficial for the rest of the network, allowing us to track all desired networking events from stable connections, while at the same time having a highly customizable tracing system.

## Maintainers

- [Dennis Trautwein](https://github.com/dennis-tra)
- [Mikel Cortes](https://github.com/cortze)
- [Guillaume Michel](https://github.com/guillaumemichel)

## Contributing

Contributions are welcome!

Please take a look at [the issues](https://github.com/probe-lab/hermes/issues).

## License

[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)
