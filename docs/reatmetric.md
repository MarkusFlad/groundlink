# Testing the SLE provider with ReatMetric

[ReatMetric](https://github.com/dariol83/reatmetric) is an open-source
monitoring and control system by Dario Lucia. Its spacecraft driver is a
complete SLE user (RAF, RCF, CLTU over ISP1), with TC frames, CLTUs, TM
frames and PUS, all built on his independent Java library
[ccsds](https://github.com/dariol83/ccsds). That makes it a good
counterpart for testing groundlink's SLE provider: it shares no code with
groundlink.

This document shows how to set up ReatMetric for the
[`sle_spacecraft`](../examples/sle_spacecraft.rs) example, which simulated
spacecraft it talks to, and what had to be configured on both sides.

```text
ReatMetric                                   sle_spacecraft
activity TC(17,1) ─▶ TC frame ─▶ CLTU ══ FCLTU, port 5201 ══▶ TcFrameDecoder ─▶ PusTcAcceptor ─▶ PusTestServiceActor
command verification ◀─ TM(1,x) ◀─ TM frames ◀══ RAF, port 5200 ══ TmFrameEncoder ◀─ PusTmStamper ◀─┘
```

ReatMetric itself is used unchanged, as released in version 1.1.2, the
last version (its development is considered completed).

## Quick start

```sh
# Once: JDK 17, Maven, ReatMetric 1.1.2 and the configuration (a few minutes)
tools/reatmetric/setup.sh

# Terminal 1: the simulated spacecraft with its SLE provider
cargo run --example sle_spacecraft -- 5200 5201

# Terminal 2: ReatMetric with its user interface ...
~/Reatmetric/sle_test/start-ui.sh
# ... or without it: sends TC(17,1) and prints PASSED or FAILED
~/Reatmetric/sle_test/check/check.sh
```

Requirements: Linux on x86-64 or ARM64, `curl`, `git`, `python3`, `tar`
and `sha256sum`/`sha512sum`. No root rights are needed.

## What `setup.sh` does

[`tools/reatmetric/setup.sh`](../tools/reatmetric/setup.sh) runs three
steps and skips each one that is done already:

1. It downloads Eclipse Temurin JDK 17 and Maven 3.9.9 into the prefix
   (default `~/opt`) and checks their checksums. `~/opt/reatmetric-env.sh`
   puts them on the `PATH` when sourced; the shell configuration is not
   changed.
2. It clones ReatMetric into `~/opt/src/reatmetric`, checks out tag
   1.1.2, builds it with Maven and installs the spacecraft deployment
   (the user interface with the spacecraft driver) into
   `~/opt/reatmetric-1.1.2`, linked as `~/opt/reatmetric`.
3. It writes the configuration from
   [`tools/reatmetric/config`](../tools/reatmetric/config) to
   `~/Reatmetric/sle_test`, with paths, host and ports filled in. An
   existing configuration folder is moved aside to `sle_test.bak-<time>`
   first.

| Option | Default | |
|---|---|---|
| `--prefix DIR` | `~/opt` | where JDK, Maven and ReatMetric go |
| `--config DIR` | `~/Reatmetric/sle_test` | the configuration folder |
| `--host HOST` | `127.0.0.1` | host of `sle_spacecraft` |
| `--raf-port PORT` | `5200` | RAF port of `sle_spacecraft` |
| `--fcltu-port PORT` | `5201` | FCLTU port of `sle_spacecraft` |
| `--config-only` | | only rewrite the configuration, e.g. for other ports |

## Using the user interface

ReatMetric binds and starts both service instances when it starts
(`autostart-connectors` in the core configuration), so `sle_spacecraft`
must be running first. Its log then shows two `Bound` and two `Started`
events.

- **Connectors**: the RAF and the CLTU service instance, with their state.
  They can be stopped and started again from here.
- **Activities**: TC(17,1) is `SPACE.GROUNDLINK.PUS17.TC17_1` in the system
  model. Execute it and choose the CLTU service instance
  `sagr=1.spack=VST-PASS0001.fsl-fg=1.cltu=cltu1` as route. The activity
  then goes through the stages Release, Ground Station Reception
  (positive CLTU-TRANSFER-DATA return), Ground Station Uplink
  (CLTU-ASYNC-NOTIFY "radiated"), On-board Acceptance, Start and
  Completion (TM(1,1), TM(1,3), TM(1,7)).
- **Events**: each verification report also raises an event under
  `SPACE.GROUNDLINK.PUS1`; failures (TM(1,2), TM(1,4), TM(1,8)) as alarms.
- **Raw data**: all TM packets, named after `spacecraft/tmtc.xml`, among
  them the TM(17,2) Are-You-Alive report.

## The headless check

`check/check.sh` runs [`SleCheck.java`](../tools/reatmetric/config/check/SleCheck.java)
with the same configuration, without user interface: it starts the
ReatMetric core, waits until the CLTU route is available, sends TC(17,1)
and waits up to 60 s until the activity is completed. It prints every
stage and ends with `PASSED` if Ground Station Reception, Ground Station
Uplink and On-board Acceptance, Start and Completion are all OK, with exit
code 0; otherwise `FAILED` and exit code 1. Since it ends ReatMetric
without UNBIND, `sle_spacecraft` reports the associations as aborted at
the end.

## Logs

`sle_spacecraft` logs every telecommand it receives and every telemetry
packet it sends:

```text
INFO sle_spacecraft: TC received: TC(17,1) apid=0x042 seq=1 source=1 ack=AS-C app_data=0 octets
INFO sle_spacecraft: TM sent: TM(1,1) apid=0x042 seq=0 AcceptanceSuccess for TC seq=1
INFO sle_spacecraft: TM sent: TM(1,3) apid=0x042 seq=1 StartSuccess for TC seq=1
INFO sle_spacecraft: TM sent: TM(17,2) apid=0x042 seq=2 are-you-alive report
INFO sle_spacecraft: TM sent: TM(1,7) apid=0x042 seq=3 CompletionSuccess for TC seq=1
```

A packet on the TC link that is not a valid PUS telecommand is logged as
a warning; the acceptance check then answers it with TM(1,2) if it can.

`--log` selects the layers that are logged, `--hex` adds a hex dump of
each data unit:

```sh
cargo run --example sle_spacecraft -- --log all --hex 5200 5201
cargo run --example sle_spacecraft -- --log cltu,tc-frame,tc 5200 5201
```

| Layer | Logged |
|---|---|
| `cltu` | every CLTU received: identification, length, codeblocks |
| `tc-frame` | every TC Transfer Frame: spacecraft, virtual channel, sequence number, type (BD, AD, …) |
| `tc` | every telecommand (the default, with `tm`) |
| `tm` | every telemetry packet |
| `tm-frame` | every TM Transfer Frame: virtual channel, frame counts, first header pointer |

`all` and `none` select all or no layers. With `--log all --hex`, one
TC(17,1) from ReatMetric looks like this (shortened):

```text
DEBUG groundlink::cltu: CLTU received: id=1 34 octets, 3 codeblocks
TRACE groundlink::cltu: CLTU 1:
  0000: eb 90 20 2a 00 13 00 18 42 b4 c0 01 00 06 1b 11 01 a4 00 01 07 d2 af 01 55 88 c5 c5 c5 c5 c5 c5
  0020: c5 79
DEBUG groundlink::tc_frame: TC frame received: scid=0x2a vcid=0 seq=0 type=BD 20 octets
TRACE groundlink::tc_frame: TC frame 0:
  0000: 20 2a 00 13 00 18 42 c0 01 00 06 1b 11 01 00 01 07 d2 af 01
 INFO sle_spacecraft::tc: TC received: TC(17,1) apid=0x042 seq=1 source=1 ack=AS-C app_data=0 octets
TRACE sle_spacecraft::tc: TC seq=1:
  0000: 18 42 c0 01 00 06 1b 11 01 00 01 07 d2
 INFO sle_spacecraft::tm: TM sent: TM(1,1) apid=0x042 seq=0 AcceptanceSuccess for TC seq=1
DEBUG groundlink::tm_frame: TM frame sent: scid=0x2a vcid=0 mc=0 vc=0 fhp=0 1115 octets
```

There are no CADUs to log: RAF carries the TM Transfer Frames as the
ground station's decoder delivers them, after synchronisation marker,
derandomisation and Reed-Solomon decoding. The frame layers come from
groundlink's `TcFrameDecoder` and `TmFrameEncoder`, which log under the
targets `groundlink::cltu`, `groundlink::tc_frame` and
`groundlink::tm_frame` (on the ground side, `TcFrameEncoder` and
`TmFrameDecoder` log the same way); any application can switch them on
with `RUST_LOG`, e.g. `RUST_LOG=info,groundlink::tc_frame=debug`.
`RUST_LOG` sets the levels of everything else, e.g. `RUST_LOG=debug` for
the details of the actors.

ReatMetric logs to the console and to `~/Reatmetric/sle_test/reatmetric.log`
(with the language of the system, e.g. `WARNUNG` for warnings). The SLE
library logs its state transitions and status reports; set
`eu.dariolucia.ccsds.sle.utl.level = FINE` in `log.properties` to see
every SLE operation.

## What is configured, and why

ReatMetric is a generic system: the spacecraft, its data link and its
packets are configuration. These settings make it match `sle_spacecraft`:

| File | Setting | Value | Reason |
|---|---|---|---|
| `configuration.xml` | `autostart-connectors` | `startup="true"` | bind RAF and CLTU at start |
| `spacecraft/configuration.xml` | `id` | `42` | spacecraft ID 0x2a of the TC and TM frames |
| | `tc randomize` | `false` | groundlink does not randomise CLTUs |
| | `tc ad-mode-default` | `false` | type-BD frames: groundlink has no FARM and sends no CLCW, a FOP in AD mode would wait for one |
| | `tc-vc-descriptor segmentation` | `false` | groundlink's `TcFrameConfig` has no segment header by default |
| | `tm fecf`, `ocf`, `frame-length` | `true`, `false`, `1115` | groundlink's `TmFrameConfig::new`; ReatMetric expects an OCF by default |
| | `destination-field-length` | `32` | PUS-C has a 16-bit message type counter and a 16-bit destination ID; ReatMetric knows only an 8-bit sub-counter, so both are read as one field |
| | `obt-cuc-config` | P-field, 4 + 2 octets, no `obt-epoch` | groundlink's default CUC format, epoch 1958 |
| | `tm-pec-present`, `tc-pec-present` | `CRC` | groundlink checks and writes the packet error control field |
| | `pus-source-id-length` | `16` | PUS-C source ID |
| | services | `CommandVerificationService` only | tracks TCs through TM(1,x) |
| `spacecraft/sle/groundlink.xml` | `responder-id` | `PROVIDER` | `SleAssociationConfig::default`; ReatMetric checks it |
| | `auth-mode` | `NONE` | groundlink does not check credentials |
| | `service-version-number` | `5` | groundlink accepts 2 to 5 |
| | `reporting-cycle` (CLTU) | `30` | ReatMetric sends CLTUs only after the first CLTU-STATUS-REPORT has told it the buffer size |
| `spacecraft/tmtc.xml` | TM(1,x), TM(17,2), TC(17,1) | APID 66 | packet identification and TC encoding |
| `processing/model.xml` | activity `#1701`, events `#10101`… | | the TC, and one event per TM(1,x): the command verification raises the event whose ID is the TM packet's `external_id` |

On the groundlink side, two things were needed:

- ReatMetric always writes **PUS version 1** (PUS-A/B) into the TC
  secondary header. Its layout is the same as PUS-C with a 16-bit source
  ID, so `sle_spacecraft` sets `PusConfig::accept_tc_version_1`, which is
  off by default.
- The provider supports **SCHEDULE-STATUS-REPORT, CLTU-ASYNC-NOTIFY and
  CLTU-GET-PARAMETER**: ReatMetric needs the status report for the CLTU
  buffer size and frees its buffer estimate only on the notification
  "radiated". It asks GET-PARAMETER after a rejected CLTU.

## Troubleshooting

- **No TC route becomes available**: `sle_spacecraft` is not running, or
  on other ports. Ports other than 5200/5201: rewrite the configuration
  with `setup.sh --config-only --raf-port … --fcltu-port …`.
- **The activity stays at Ground Station Reception**: no
  CLTU-STATUS-REPORT came; check `reporting-cycle` in the SLE file.
- **TM(1,2) for every TC**: the acceptance check rejected the TC, e.g.
  for its PUS version or its CRC; `sle_spacecraft` logs the reason.
- **Errors about the archive**: only one ReatMetric instance can use the
  Derby archive at a time; delete `~/Reatmetric/sle_test/archive` to start
  with an empty one.

## Removing it

Delete `~/opt/jdk*`, `~/opt/apache-maven*`, `~/opt/maven`,
`~/opt/reatmetric*`, `~/opt/src/reatmetric`, `~/opt/downloads` and
`~/Reatmetric`. If you used `--prefix` or `--config`, delete those folders
instead.
