# Tyr .replay format

Tyr ships completely stock UE 5.6 replay serialization - no custom fields, no
compression, no encryption. Files are `FLocalFileNetworkReplayStreaming`
version 7 with embedded header `networkVersion=19`, which is the same container
Fortnite and every other stock-UE game writes.

Everything here came from hand-decoding hexdumps and cross-checking against
[Shiqan/FortniteReplayDecompressor](https://github.com/Shiqan/FortniteReplayDecompressor)'s
`Unreal.Core`.

## Container

### `ReplayInfo` (offset 0)

| field | type | notes |
|---|---|---|
| magic | uint32 | `0x1CA2E27F` (`NETWORK_DEMO_MAGIC`) |
| fileVersion | uint32 | `7` |
| customVersionCount | int32 | only since `fileVersion >= 7`; skip `count * 20` bytes (16-byte GUID + int32 version each) |
| lengthInMs | uint32 | match length |
| networkVersion | uint32 | `337258096`, a checksum-like value rather than a small int |
| changelist | uint32 | game build, e.g. `31351` or `29906` |
| friendlyName | FString | fixed-size buffer of spaces, the game never sets a match name (`HISTORY_FIXEDSIZE_FRIENDLY_NAME`) |
| isLive | uint32-as-bool | unreliable, see below |
| timestamp | int64 | since `fileVersion >= 3`; .NET `DateTime.FromBinary` ticks, 100ns since `0001-01-01`, top 2 bits are a Kind flag and need masking off |
| isCompressed | uint32-as-bool | false in everything seen so far |
| isEncrypted | uint32-as-bool | false in everything seen so far |
| encryptionKeySize | uint32 | read this unconditionally once `fileVersion >= 6` |

That last row is the one that'll bite you. The size field is always present
regardless of `isEncrypted`, and only the key *bytes* are conditional on
`size > 0`. Get it wrong and every subsequent offset silently drifts by 4
bytes, which produces garbage rather than an error.

`isLive` reads true on most finished files, so don't trust it to tell you
whether a match completed. Check whether the file has stopped growing instead.

### Chunk loop

Straight after `ReplayInfo`, repeating until EOF:

```
uint32 chunkType   0=Header, 1=ReplayData, 2=Checkpoint, 3=Event
int32  chunkSize   payload bytes only
byte[] payload
```

Tyr writes no `Event` chunks at all. UE's `AddEvent`/`FReplayEventList`
bookmark mechanism goes unused, so there's no cheap killfeed or scoreboard
data sitting in the container - all of it lives in the bit-packed net stream.
`probe.py` implements Event chunk parsing anyway in case a future build starts
emitting them.

### `Header` chunk (type 0, always first, ~226 bytes)

| field | type | notes |
|---|---|---|
| magic | uint32 | `0x2CF5A13D`, distinct from the file magic |
| networkVersion | uint32 | `19` |
| customVersionCount + skip | int32 + N*20 | always present at `19 >= HISTORY_USE_CUSTOM_VERSION` |
| networkChecksum | uint32 | |
| engineNetworkVersion | uint32 | `42` |
| gameNetworkProtocolVersion | uint32 | `0` |
| guid | 16 bytes | unique per replay |
| engineVersion + changelist + branch | uint16x3 + uint32 + FString | `5.6.0`, branch `++Tyr+release` |
| ue4Version, ue5Version, packageVersionLicenseeUE | uint32x3 | `522`, `1017`, `0` |
| levelNamesAndTimes | array of (FString, uint32) | the map name, e.g. `/TyrMapFields/Maps/Map_Fields` |
| flags | uint32 | `0x1` |
| gameSpecificData | array of FString | empty |
| minRecordHz, maxRecordHz, frameLimitInMS, checkpointLimitInMS | float32x4 | `0.0, 30.0, -1.0, -1.0` |
| platform | FString | `WindowsClient` |
| buildConfig, buildTargetType | byte, byte | `4, 3` = Shipping / Client |

Decoded timestamps landing on real match dates is a good self-check that your
offsets are right - a single off-by-one produces implausible dates, not
plausible ones.

## Net stream

`ReplayData` and `Checkpoint` chunks hold the actual bit-packed net stream.
This is where everything useful lives: actor channel opens and closes, property
replication, and RPC calls.

The walk goes frames -> packets -> bunches:

- **Frames.** Each `ReplayData` chunk is a run of demo frames, each with a
  float32 timestamp followed by the packets sent that frame.
- **Packets.** Length-prefixed. A sentinel length ends the frame.
- **Bunches.** Each packet carries bunches, which are per-channel. A bunch
  header gives the channel index, whether it's opening or closing the channel,
  and whether the payload has a replication layout.
- **Export table.** Channel opens carry NetGUID exports mapping numeric guids to
  object paths, and those paths are plaintext. That's how you get class names
  like `BP_TyrPlayerState_C` and RPC names like `Multicast_SendEndGameStats`
  without any schema.
- **Property replication.** Inside a bunch, properties arrive as handle/value
  pairs against the class layout. RPC parameter payloads are different - no
  handle framing, just parameters in declaration order, which is why
  `decode.py` snapshots those raw and decodes them separately.

Everything is bit-packed LSB-first and does not byte-align between fields, so
a plain string scan recovers class and property *names* but never property
*values*. Player names, damage numbers and positions all need the full walk.

## Gotchas

- **Positions are raw world coordinates in centimetres**, not map-relative. See
  the README for the minimap projection.
- **GameplayTag indices are build specific.** They get renumbered whenever tags
  are added, so match by name.
- **Tank names are internal codenames** in the scoreboard RPC, not display
  names.
- **A partially written file decodes partially, or not at all.** Wait for the
  file to stop changing before reading it.

## Files

- `probe.py` - header and chunk-list decode, plus `--strings` for a whole-file
  string scan. Useful for looking at a new build before touching `decode.py`.
- `decode.py` - the full net stream walk.
