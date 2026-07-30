"""Decode the net stream inside Tyr (UE 5.6) .replay ReplayData chunks.

Builds on probe.py (which parses the top-level ReplayInfo +
chunk list). This tool goes one level deeper, porting the minimum of
Shiqan/FortniteReplayDecompressor's Unreal.Core (MIT) needed to:

  1. walk every ReplayData chunk  -> DemoFrames (time, export data, packets)
  2. walk every packet            -> bunches (channel index, open/close, ...)
  3. collect the NetGUID export table (netguid -> object path) and the
     NetFieldExportGroup tables (class path -> [replicated property names])
  4. record actor-channel opens (actor/archetype guid + quantized spawn
     location/rotation/velocity from SerializeNewActor), closes (with
     close reason), per-property replication events, and RPC names seen
     on each class's ClassNetCache.
  5. (stretch) decode FRepMovement payloads for `ReplicatedMovement`
     properties -> world-space location/rotation/velocity samples over time.

decode() also derives, after the main pass (see each function's docstring
for exactly how / what's validated):
  - state.endgame_stats  -- the end-of-match scoreboard (decode_end_game_stats)
  - state.death_events   -- kill/death timeline: {t, victim, victimTeam,
    victimTank, killer, killerTeam, killerTank, deathLocation} per death,
    time-sorted (derive_death_events). Killer attribution and death location
    are each independently reverse-engineered RPC-parameter bit offsets,
    validated against two complete matches -- see that function's docstring.
  - state.survival       -- {playerName: {team, diedAt, survived,
    survivalSec}} for every roster player (derive_survival).
  - state.damage_events  -- per-hit damage-by-who. Currently always []; the
    only plausible source RPC's field layout could NOT be validated -- see
    decode_damage_events's docstring for the (negative) investigation instead
    of a guessed mapping.

Version facts for THIS build, confirmed against real files by
probe.py (see docs/replay-format.md):
    fileVersion            = 7   (top-level ReplayInfo; >= HISTORY_ENCRYPTION,
                                  so ReplayData chunks carry start/end/length +
                                  memorySizeInBytes prefixes)
    networkVersion (demo)  = 19  (HISTORY_USE_CUSTOM_VERSION; >= 6 so frames
                                  start with CurrentLevelIndex, >= 10 so frames
                                  carry per-frame export data)
    engineNetworkVersion   = 42  (>= CustomExports=36: bunch headers carry the
                                  bPartialCustomExportsFinal bit; >= 23:
                                  packed vectors use the LWC quantized format;
                                  >= 22: unquantized vectors are doubles)
    header flags           = 0x1 (ClientRecorded ONLY -- NO HasStreamingFixes,
                                  NO GameSpecificFrameData, NO DeltaCheckpoints.
                                  So frames have NO per-packet seenLevelIndex
                                  and NO external-data offset fields)
    isCompressed=False, isEncrypted=False -> chunk payloads are raw bytes.

Reference code paths mirrored (Shiqan/FortniteReplayDecompressor, MIT):
    ReplayReader.ReadReplayData / ReadDemoFrameIntoPlaybackPackets /
    ReadExportData / ReadNetFieldExports / ReadNetFieldExport /
    ReadNetExportGuids / ReadExternalData / ReadPacket / ReceivedRawPacket /
    ReceivedPacket / ReceivedNextBunch / ProcessBunch / ReadContentBlockHeader /
    ReadContentBlockPayload / ReceiveProperties / ReadFieldHeaderAndPayload /
    InternalLoadObject / ReceiveNetGUIDBunch, plus BitReader / BinaryReader /
    NetBitReader.SerializeRepMovement.

Usage:
    python -m tools.replay_decode <path.replay> [--max-chunks N]
        [--max-frames N] [--guids N] [--json out.json] [--quiet]

stdlib only. Defensive: a malformed packet/bunch aborts only that
packet/bunch (they are independently length-framed), never the whole run.
"""
import argparse
import json
import re
import math
import struct
import sys
from collections import Counter, defaultdict
from pathlib import Path

# constants (values mirror Unreal.Core enums)

ENGINE_NET_VER_NEW_ACTOR_OVERRIDE_LEVEL = 5
ENGINE_NET_VER_CHANNEL_NAMES = 6
ENGINE_NET_VER_CHANNEL_CLOSE_REASON = 7
ENGINE_NET_VER_ACKS_IN_HEADER = 8
ENGINE_NET_VER_NETEXPORT_SERIALIZATION = 9
ENGINE_NET_VER_NETEXPORT_SERIALIZE_FIX = 10
ENGINE_NET_VER_OPTIONALLY_QUANTIZE_SPAWN = 13
ENGINE_NET_VER_CLASSNETCACHE_FULLNAME = 15
ENGINE_NET_VER_SUBOBJECT_OUTER_CHAIN = 18
ENGINE_NET_VER_DOUBLE_VECTORS = 22
ENGINE_NET_VER_PACKED_VECTOR_LWC = 23
ENGINE_NET_VER_REPMOVE_SERVERFRAME = 25
ENGINE_NET_VER_SUBOBJECT_DESTROY_FLAG = 30
ENGINE_NET_VER_REPMOVE_OPTIONAL_ACCEL = 35
ENGINE_NET_VER_CUSTOM_EXPORTS = 36

NETWORK_VER_MULTIPLE_LEVELS = 6
NETWORK_VER_LEVEL_STREAMING_FIXES = 10

HEADER_FLAG_CLIENT_RECORDED = 1 << 0
HEADER_FLAG_STREAMING_FIXES = 1 << 1
HEADER_FLAG_GAME_SPECIFIC_FRAME_DATA = 1 << 3

MAX_PACKET_SIZE_IN_BITS = 16384      # 2 * 1024 * 8, bunch size num-max
MAX_GUID_COUNT = 2048
CHANNEL_CLOSE_REASON_MAX = 15
CLOSE_REASONS = {0: "Destroyed", 1: "Dormancy", 2: "LevelUnloaded",
                 3: "Relevancy", 4: "TearOff"}

EXPORT_FLAG_HAS_PATH = 1
EXPORT_FLAG_HAS_NETWORK_CHECKSUM = 4

# Classes whose PlayerName(Private)/TeamId properties identify a real player
# (see State.player_identity / collect_player_roster). A player is represented
# by two separate actors on the wire - a BP_TyrPlayerState_C and a sibling
# BP_PlayerRecord_C - both replicate the same name+team, just via different
# channels/netguids, so identity must be deduped by name (see
# collect_player_roster), not by channel.
PLAYER_IDENTITY_CLASSES = {"BP_TyrPlayerState_C", "BP_PlayerRecord_C"}

# BP_TyrGameState_C fields that identify the match itself (rather than any
# player in it) -> the key they land under in State.match_meta. Used to work out
# whether a replay is an official matchmade game on the current build, and
# which match it is.
MATCH_META_FIELDS = {
    "bCreatedViaMatchmaking": "createdViaMatchmaking",
    "Gamemodetag": "gameModeTag",
    "Matchdetailsid": "matchDetailsId",
    "bAllowAlphaVehicles": "allowAlphaVehicles",
    "CurrentMatchPhase": "maxMatchPhase",
}

# Per-player loadout GameplayTags -> the key they're stored under. All three
# replicate as a tag net index (a 17/25-bit int), not a name: the value is only
# meaningful relative to other values from the same game build. VehicleTag
# rides BP_TyrPlayerState_C and BP_PlayerRecord_C, SkinTag only the former,
# Keystonetalenttag only the latter - collect_player_roster merges a player's
# two channels by name, so all three land on one roster row regardless.
LOADOUT_TAG_FIELDS = {
    "VehicleTag": "vehicle_tag",
    "SkinTag": "skin_tag",
    "Keystonetalenttag": "keystone_tag",
}

# ClassNetCache (RPC) fields whose raw payload bytes we keep around (see
# State.rpc_payloads / decode_end_game_stats) for parameter decoding, beyond
# the plain occurrence counting every ClassNetCache field gets in
# state.rpc_counter. Multicast_SendEndGameStats is BP_TyrGameState_C's
# end-of-match RPC that hands the whole scoreboard to every client in one
# call - see docs/replay-format.md.
RPC_CAPTURE_NAMES = {"Multicast_SendEndGameStats"}
RPC_CAPTURE_CLASSES = {"BP_TyrGameState_C"}
RPC_CAPTURE_CAP = 50   # max samples kept per (class, field) key

# Combat-event RPCs whose parameter payloads are kept in State.event_rpcs for
# the death/damage derivations below (derive_death_events / decode_damage_events).
# Unlike RPC_CAPTURE_NAMES these are keyed by field name only (they fire from
# many classes - every ammo type fires BroadcastBlockedMessage, every tank
# class fires Multicast_OnDeathEffects) and each sample records the carrying
# channel's actor guid, which is the event subject (the dying pawn / the
# projectile that was blocked).
EVENT_RPC_NAMES = {
    "Multicast_OnDeathEffects",          # tank death: DeathLocation + TeamMembersRemaining
    "NetMulticastBroadcastMessage",      # gameplay message: Instigator/Target/Magnitude/Location
    "NetMulticastBroadcastEffectDurationMessage",
    "BroadcastBlockedMessage",           # blocked hit: Instigator/Target/Magnitude/Location
}
EVENT_RPC_CAP = 5000   # per field name; a full match fires each well under this

# subset of hardcoded engine FNames (UnrealNames.inl) we ever expect in a
# bunch header channel-name or netfield FName
UNREAL_NAMES = {
    0: "None", 102: "Actor", 200: "State", 215: "Role", 216: "RemoteRole",
    227: "MoveActor", 248: "Location", 249: "Rotation", 255: "Control",
    256: "Voice", 282: "GameNetDriver",
}


class DecodeError(Exception):
    """Any structural read failure. Message carries offset context."""


TRACE_CHANNEL = None   # set via --trace-ch to dump content-block parsing


# readers

class ByteReader:
    """Byte-aligned FArchive (mirrors Unreal.Core BinaryReader)."""

    __slots__ = ("data", "off")

    def __init__(self, data):
        self.data = data
        self.off = 0

    def remaining(self):
        return len(self.data) - self.off

    def at_end(self):
        return self.off >= len(self.data)

    def rd(self, n):
        if n < 0 or self.off + n > len(self.data):
            raise DecodeError(f"ByteReader: want {n}B at off {self.off}, have {self.remaining()}")
        b = self.data[self.off:self.off + n]
        self.off += n
        return b

    def u8(self):
        return self.rd(1)[0]

    def u16(self):
        return struct.unpack_from("<H", self.rd(2))[0]

    def u32(self):
        return struct.unpack_from("<I", self.rd(4))[0]

    def i32(self):
        return struct.unpack_from("<i", self.rd(4))[0]

    def u64(self):
        return struct.unpack_from("<Q", self.rd(8))[0]

    def f32(self):
        return struct.unpack_from("<f", self.rd(4))[0]

    def f64(self):
        return struct.unpack_from("<d", self.rd(8))[0]

    def bool8(self):
        return self.rd(1)[0] != 0

    def int_packed(self):
        """Byte-aligned FIntPacked: per byte, bit0=continuation, bits1-7=value."""
        value = 0
        count = 0
        while True:
            b = self.u8()
            value |= (b >> 1) << (7 * count)
            count += 1
            if not (b & 1):
                return value
            if count > 5:
                raise DecodeError(f"ByteReader: runaway int_packed at off {self.off}")

    def fstring(self):
        n = self.i32()
        if n == 0:
            return ""
        if n > 0:
            if n > 100_000:
                raise DecodeError(f"ByteReader: implausible FString len {n} at off {self.off}")
            return self.rd(n).decode("utf-8", "replace").rstrip("\x00")
        n = -n
        if n > 100_000:
            raise DecodeError(f"ByteReader: implausible UTF16 FString len {n} at off {self.off}")
        return self.rd(n * 2).decode("utf-16-le", "replace").rstrip("\x00")

    def fname(self):
        """Byte-aligned FName (engineNetworkVersion >= CHANNEL_NAMES path)."""
        if self.bool8():                       # bHardcoded
            idx = self.int_packed()
            return UNREAL_NAMES.get(idx, f"HardcodedName_{idx}")
        s = self.fstring()
        self.i32()                             # InNumber, dropped
        return s


class BitReader:
    """LSB-first bit archive (mirrors Unreal.Core BitReader).

    Bit i of the stream is bit (i & 7) of byte (i >> 3) -- i.e. within each
    byte the LEAST significant bit comes first.
    """

    __slots__ = ("data", "pos", "nbits")

    def __init__(self, data, nbits=None):
        self.data = data
        self.pos = 0
        self.nbits = len(data) * 8 if nbits is None else nbits

    def at_end(self):
        return self.pos >= self.nbits

    def bits_left(self):
        return self.nbits - self.pos

    def can_read(self, n):
        return self.pos + n <= self.nbits

    def read_bit(self):
        if self.pos >= self.nbits:
            raise DecodeError(f"BitReader: read past end (pos {self.pos}/{self.nbits})")
        r = (self.data[self.pos >> 3] >> (self.pos & 7)) & 1
        self.pos += 1
        return r == 1

    def read_bits_int(self, n):
        """Read n bits LSB-first into an int (bit k of result = k-th bit read)."""
        if n == 0:
            return 0
        if n < 0 or not self.can_read(n):
            raise DecodeError(f"BitReader: want {n} bits at pos {self.pos}, have {self.bits_left()}")
        start = self.pos
        self.pos += n
        sb, off = divmod(start, 8)
        eb = (start + n + 7) >> 3
        val = int.from_bytes(self.data[sb:eb], "little") >> off
        return val & ((1 << n) - 1)

    def read_bits_bytes(self, n):
        """Read n bits, returned as bytes (little-endian packing, LSB-first)."""
        val = self.read_bits_int(n)
        return val.to_bytes((n + 7) >> 3, "little")

    def u8(self):
        return self.read_bits_int(8)

    def u16(self):
        return self.read_bits_int(16)

    def u32(self):
        return self.read_bits_int(32)

    def i32(self):
        v = self.read_bits_int(32)
        return v - (1 << 32) if v & (1 << 31) else v

    def f32(self):
        return struct.unpack("<f", self.read_bits_bytes(32))[0]

    def f64(self):
        return struct.unpack("<d", self.read_bits_bytes(64))[0]

    def int_packed(self):
        """Bit-level FIntPacked: groups of 8 bits; bit0=continuation, bits1-7=value."""
        value = 0
        for it in range(5):
            b = self.read_bits_int(8)
            value |= (b >> 1) << (7 * it)
            if not (b & 1):
                break
        return value

    def serialized_int(self, max_value):
        """FBitReader::SerializeInt: write as many bits as needed for max_value,
        LSB first, stopping once the next mask bit could exceed max_value-1."""
        value = 0
        mask = 1
        while value + mask < max_value:
            if self.read_bit():
                value |= mask
            mask <<= 1
        return value

    def fstring(self):
        n = self.i32()
        if n == 0:
            return ""
        if n > 0:
            if n > 100_000:
                raise DecodeError(f"BitReader: implausible FString len {n} at pos {self.pos}")
            return self.read_bits_bytes(8 * n).decode("utf-8", "replace").rstrip("\x00")
        n = -n
        if n > 100_000:
            raise DecodeError(f"BitReader: implausible UTF16 FString len {n} at pos {self.pos}")
        return self.read_bits_bytes(16 * n).decode("utf-16-le", "replace").rstrip("\x00")

    def fname(self):
        """Bit-level FName (UPackageMap::StaticSerializeName)."""
        if self.read_bit():                    # bHardcoded
            idx = self.int_packed()
            return UNREAL_NAMES.get(idx, f"HardcodedName_{idx}")
        s = self.fstring()
        self.i32()                             # InNumber
        return s

    # - packed vectors ----------------------------------------------------

    def read_quantized_vector(self, scale_factor):
        """LWC FVectorNetQuantize path (engineNetworkVersion >= 23)."""
        cb_and_extra = self.serialized_int(1 << 7)
        component_bits = cb_and_extra & 63
        extra_info = cb_and_extra >> 6
        if component_bits > 0:
            sign_bit = 1 << (component_bits - 1)

            def comp():
                v = self.read_bits_int(component_bits)
                v = (v ^ sign_bit) - sign_bit
                return v / scale_factor if extra_info else float(v)

            return (comp(), comp(), comp())
        if extra_info == 0:
            return (self.f32(), self.f32(), self.f32())
        return (self.f64(), self.f64(), self.f64())

    def read_packed_vector(self, scale_factor, max_bits):
        """Classic template<ScaleFactor, MaxBitsPerComponent> SerializePackedVector
        (NetSerialization.h) -- NOT the LWC FVectorNetQuantize component-count+extra
        scheme (that one is read_quantized_vector, confirmed correct elsewhere for
        FRepMovement). SerializeNewActor's Location/Scale/Velocity quantized branch
        uses this older int-bias template instead."""
        bits = self.serialized_int(max_bits)
        bias = 1 << (bits + 1)
        maxv = 1 << (bits + 2)
        dx = self.serialized_int(maxv)
        dy = self.serialized_int(maxv)
        dz = self.serialized_int(maxv)
        return ((dx - bias) / scale_factor, (dy - bias) / scale_factor, (dz - bias) / scale_factor)

    def read_rotation_byte(self):
        p = self.u8() * 360 / 256 if self.read_bit() else 0.0
        y = self.u8() * 360 / 256 if self.read_bit() else 0.0
        r = self.u8() * 360 / 256 if self.read_bit() else 0.0
        return (p, y, r)

    def read_rotation_short(self):
        p = self.u16() * 360 / 65536 if self.read_bit() else 0.0
        y = self.u16() * 360 / 65536 if self.read_bit() else 0.0
        r = self.u16() * 360 / 65536 if self.read_bit() else 0.0
        return (p, y, r)


# state

class Channel:
    __slots__ = ("index", "name", "actor_guid", "archetype_guid", "opened_time",
                 "partial_data", "partial_bits", "partial_reliable", "partial_open")

    def __init__(self, index):
        self.index = index
        self.name = None
        self.actor_guid = None
        self.archetype_guid = None
        self.opened_time = None
        self.partial_data = None       # bytearray while accumulating a partial bunch
        self.partial_bits = 0
        self.partial_reliable = False
        self.partial_open = False


class State:
    def __init__(self):
        self.guid_paths = {}                     # netguid -> path (leaf name)
        self.guid_outer = {}                     # netguid -> outer netguid
        self.groups_by_index = {}                # pathNameIndex -> group dict
        self.groups_by_path = {}                 # full path -> group dict
        self.groups_by_class = {}                # class key -> group dict
        self.channels = {}                       # chIndex -> Channel
        self.channel_opens = []                  # dicts
        self.channel_closes = []                 # dicts
        self.prop_counter = Counter()            # (class, prop) -> updates
        self.rpc_counter = Counter()             # (class, field) -> count
        # Raw RPC/custom-delta payloads for fields named in RPC_CAPTURE_NAMES
        # (see decode_end_game_stats below) - (class, field) -> [(time, ch,
        # numBits, bytes), ...], capped per key so a chatty RPC can't blow up
        # memory on a long match.
        self.rpc_payloads = defaultdict(list)
        # Full raw bytes of any (RPC_CAPTURE_CLASSES-class, hasRepLayout=False)
        # content block, before the field-handle loop below consumes it
        # used to bisect the RPC parameter wire format from scratch (see
        # decode_end_game_stats). class -> [(time, ch, numBits, bytes), ...]
        self.rpc_raw_blocks = defaultdict(list)
        self.movement_samples = defaultdict(list)  # actorGuid -> [(t, loc, rot, vel, rotShort)]
        # AbilitySystemComponent.AvatarActor object-ref updates:
        # (t, ascCarrierActorGuid, avatarActorGuid). The ASC rides its owner's
        # channel as a stably-named subobject, so ch.actor_guid at decode time
        # is the actor that owns the ASC. For the 16 BP_TyrPlayerState_C
        # channels the carrier guid is the PlayerState actor and AvatarActor is
        # the player's current vehicle pawn guid - re-pointed on every
        # (re)spawn and set to 0 on death/unspot. This is the exact
        # pawn-track -> player -> real-team link the map renderer uses
        # (vehicle pawns list PlayerState/TeamId in their NetFieldExportGroup
        # but never actually replicate them in these client replays, so the
        # PlayerState-side ASC ref is the only owner link on the wire).
        self.avatar_links = []
        # experimental, nothing downstream reads this - see the squad/clan preview
        # for the one
        # consumer, a local-only preview. BP_Ammunition_*.Instigator UObject-
        # ref updates: (t, ammoActorGuid, instigatorGuid). Resolved via the
        # same InternalLoadObject wire shape as AvatarActor/KillerRef below.
        # Coverage is low (~11% of shots measured across 5 real replays,
        # 2026-07-21 research pass) - the property only replicates in a
        # projectile's initial bunch when the shooter pawn's netguid was
        # already mapped to the recording client, which is relevance-
        # dependent and not recoverable for the other ~89%. When present it
        # is high quality (100% parse exactly, 101/103 resolved to a real
        # roster player in that pass, 82% agreement against the independent
        # kill-timeline ground truth) - ship only as an explicitly-partial
        # "shots attributed (~1 in 9)" stat, never as "total shots fired".
        self.ammo_instigators = []
        # TyrTeamPublicInfo replication, keyed by the instance's actor guid.
        # Each match spawns exactly two TyrTeamPublicInfo actors (one per
        # team); each replicates its own TeamId (u32) plus the team's shared
        # health pool (CurrentTeamHealth / MaxTeamHealth, u32) - the team
        # whose pool hits 0 loses the match. actorGuid -> {"teamId": int,
        # "health": [(t, v)], "maxHealth": [(t, v)]}; reduced by
        # collect_team_health()/derive_match_result() below.
        self.team_info = defaultdict(dict)
        # (class, prop) -> [(t, chIndex, actorGuid, numBits, bytes)], only
        # populated when CAPTURE_ALL_PROPS is on (research; see below).
        self.raw_prop_samples = defaultdict(list)
        self.raw_prop_seen = {}                  # (class, prop) -> updates seen (capture stride)
        # reload/clip telemetry: field -> [(t, channel, value)]
        self.ammo_state = defaultdict(list)
        # objective/resource zone activity: (t, zoneClass, field, value)
        self.zone_events = []
        # BP_TyrGameState_C match-identity fields (see MATCH_META_FIELDS).
        # {"createdViaMatchmaking": bool, "gameModeTag": int,
        #  "matchDetailsId": str, "allowAlphaVehicles": bool,
        #  "maxMatchPhase": int}
        self.match_meta = {}
        # channels that received a COND_OwnerOnly property -> the recorder's
        # own actors (see the "Owner" branch in receive_properties)
        self.owner_only_channels = set()
        self.string_props = defaultdict(set)     # (class, prop) -> {decoded strings}
        self.attr_samples = []                   # (t, ch, class, handle, name, float)
        # per-player end-of-match scoreboard, decoded from the
        # Multicast_SendEndGameStats RPC (see decode_end_game_stats); filled at
        # the end of decode(), [] until then / if the RPC was never captured.
        self.endgame_stats = []
        # Raw combat-event RPC parameter payloads (see EVENT_RPC_NAMES):
        # fieldName -> [(t, chIndex, carrierActorGuid, class, numBits, bytes)].
        # carrierActorGuid = the actor whose channel carried the RPC (the dying
        # tank pawn for Multicast_OnDeathEffects, the projectile actor for
        # BroadcastBlockedMessage, a PlayerState for NetMulticastBroadcast*).
        self.event_rpcs = defaultdict(list)
        # BP_TyrPlayerState_C.KillerRef object-ref updates:
        # (t, chIndex, playerStateActorGuid, killerGuid). killerGuid = 0 is a
        # reset/clear, otherwise a netguid resolved by derive_death_events.
        self.killer_refs = []
        # bIsAlive bool updates on PLAYER_IDENTITY_CLASSES channels:
        # (t, chIndex, actorGuid, class, aliveBool). Only changes replicate, so
        # in practice this is one False per death (initial True is the default).
        self.alive_flags = []
        # Derived at the end of decode() (see derive_death_events /
        # derive_survival / decode_damage_events): [] / {} until then.
        self.death_events = []      # sorted list of per-death dicts
        self.survival = {}          # player name -> survival record
        self.damage_events = []     # per-hit damage/blocked message dicts
        # BP_CaptureZone_C.{bIsPointCaptured,NumAllies,NumEnemies,CapturePoints}
        # updates: (t, chIndex, actorGuid, fieldName, intValue). NumAllies/
        # NumEnemies are inherently recorder-relative - a client-recorded
        # replay only ever contains the property values that were replicated
        # to that one client, so "Allies" always means "the recording
        # player's team" here regardless of how the server labels teams
        # internally.
        #
        # confirmed this is not the match-winner signal: each
        # map spawns exactly 2 BP_CaptureZone_C actors at fixed positions
        # (same archetype-guid suffix recurs across matches on the same
        # map), and either side can occupy/capture either zone - there's
        # no fixed "your zone" vs "their zone". Worse, on all 3 known
        # capture-decided matches the zone that actually flips
        # bIsPointCaptured=1 is one the recording player's team visibly
        # abandons in the first ~20s (NumAllies drops to 0 and never
        # updates again) while the enemy slowly builds NumEnemies/
        # CapturePoints and caps it in the match's final third - i.e. the
        # zone-capture event tracks *local* zone contest, not overall match
        # outcome, and naively trusting "whoever's presence is higher when
        # bIsPointCaptured flips" is confidently backwards (see git history
        # for the original buggy derive_capture_winner). The actual winner
        # signal is state.last_stand_events / derive_last_stand_winner
        # below. This field is kept purely as descriptive zone-contest
        # telemetry (e.g. a future capture-progress bar), not for win
        # classification.
        self.capture_zone_events = []
        # BP_TyrGameState_C.TeamWithLastStandId updates: (t, teamId|None).
        # Raw wire value is a uint32 that's 0/1 for a real team id, or the
        # int32(-1) sentinel (0xFFFFFFFF = 4294967295) when no team is
        # currently in "Last Stand" (cleared/recovered) - stored as None in
        # that case so consumers don't have to know the sentinel. See
        # derive_last_stand_winner for what this means and the evidence it
        # reliably identifies the losing team.
        self.last_stand_events = []
        # ch.index -> {"name": str, "team_id": int, "class": str, "actorGuid": int}.
        # Populated only for PLAYER_IDENTITY_CLASSES so the roster tool can
        # attribute a decoded gamertag to a specific team, per-channel (see
        # collect_player_roster below for the dedup-by-name step this feeds).
        self.player_identity = defaultdict(dict)
        self._group_lookup_cache = {}
        self.external_data = Counter()           # netguid -> occurrences
        self.warn = Counter()
        self.frames = 0
        self.packets = 0
        self.bunches = 0
        self.time = 0.0
        self.log_lines = []

    def log(self, msg):
        self.log_lines.append(msg)

    def _lookup_class(self, key, raw_path=None):
        """Resolve an object's NetFieldExportGroup. Mirrors
        NetGuidCache.GetNetFieldExportGroup(netguid) from Shiqan/
        FortniteReplayDecompressor's Unreal.Core (the two Contains() passes,
        tried in both directions), plus two extra fallbacks (C, D below) this
        port needs because Tyr's stably-named subobject instance names often
        have NO textual relationship to their real class name at all -- see
        no_group_for_repobject in the module docstring / decode notes.

        A. exact match on the class_key()-normalized name.
        B. longest suffix match (existing: 'VehicleMovementComponent' instance
           name vs real class 'PrvVehicleMovementComponent').
        C. substring Contains() in either direction against the RAW
           (un-normalized) object path -- this is what the reference actually
           does (NetGuidCache.cs: `path.Contains(groupPathFixed)` then
           `groupPathFixed.Contains(cleanedPath)`), and it recovers cases our
           class_key() normalization drops, e.g. 'WorldSettings_1' contains
           'WorldSettings' but class_key() won't strip a single-digit numeric
           suffix (guards against stripping meaningful short suffixes like
           '_01'), so the exact/suffix passes above miss it.
        D. token-set match: split both names on word boundaries (PascalCase /
           underscore / digit runs) and require the instance name's token set
           to be a full subset of a candidate class's token set. This is
           strictly more permissive than C -- it matches even when a word is
           INSERTED in the middle of the class name relative to the instance
           name (e.g. instance 'TurretComponent' vs class
           'BPC_TurretBaseComponent_C', which has 'Base' spliced in, so no
           substring relation exists either direction). Only accepted when
           exactly one candidate qualifies, to avoid guessing between two
           legitimately different classes that happen to share a word (e.g.
           'BP_CrashedSpaceShip_01_Turret_01_03_C' also contains 'turret' but
           lacks the 'component' token, so it does not qualify)."""
        # note: unlike the reference (NetGuidCache._failedPaths, which caches
        # a miss permanently - its own comment admits "some export groups
        # are added later though"), we only cache hits. NetFieldExportGroups
        # can legitimately register mid-stream (a class's group often isn't
        # exported until the first frame that actually replicates one of its
        # properties), so a miss now may become a hit once more of the file
        # has been consumed. We have the whole file in memory (this is an
        # offline batch decode, not a live connection with the reference's
        # perf constraints), so always retrying misses is strictly more
        # correct and the group table stays small (tens of entries).
        cache_key = (key, raw_path)
        cached = self._group_lookup_cache.get(cache_key)
        if cached is not None:
            return cached
        # _ClassNetCache groups are a separate RPC/custom-delta field table,
        # never a property-replication group - only classnetcache_for_guid()
        # (which explicitly appends the suffix to an already-resolved class
        # key) may return one. Excluded from every fuzzy pass below: the
        # plain class name is always a literal prefix of its own
        # "<Class>_ClassNetCache" entry, so the substring/token passes would
        # otherwise happily (and wrongly) match an object straight to its
        # class's RPC table instead of its property table.
        candidate_keys = [k for k in self.groups_by_class if not k.endswith("_ClassNetCache")]
        g = self.groups_by_class.get(key) if not key.endswith("_ClassNetCache") else None
        if g is None and len(key) >= 6:
            candidates = [k for k in candidate_keys if k.endswith(key)]
            if candidates:
                g = self.groups_by_class[max(candidates, key=len)]
        if g is None and raw_path and len(key) >= 4:
            candidates = [k for k in candidate_keys
                          if len(k) >= 4 and (k in raw_path or raw_path in k)]
            if candidates:
                g = self.groups_by_class[max(candidates, key=len)]
        if g is None and len(key) >= 4:
            norm = _normalize_for_match(key)
            candidates = [k for k in candidate_keys
                          if len(k) >= 4 and
                          (norm in _normalize_for_match(k) or _normalize_for_match(k) in norm)]
            if len(candidates) == 1:
                g = self.groups_by_class[candidates[0]]
        if g is None:
            my_tokens = _tokenize(key)
            if my_tokens:
                candidates = [k for k in candidate_keys
                              if my_tokens <= _tokenize(k)]
                if len(candidates) == 1:
                    g = self.groups_by_class[candidates[0]]
        if g is not None:
            self._group_lookup_cache[cache_key] = g
        return g

    def group_for_guid(self, guid):
        path = self.guid_paths.get(guid)
        if not path:
            return None
        return self._lookup_class(class_key(path), path)

    def classnetcache_for_guid(self, guid):
        path = self.guid_paths.get(guid)
        if not path:
            return None
        g = self._lookup_class(class_key(path), path)
        if g is None:
            return None
        return self.groups_by_class.get(class_key(g["path"]) + "_ClassNetCache")

    def channel_class(self, ch):
        for guid in (ch.archetype_guid, ch.actor_guid):
            if guid is not None and guid in self.guid_paths:
                return class_key(self.guid_paths[guid])
        return f"?ch{ch.index}"


def class_key(path):
    """Normalize an object path to a bare class-ish name for matching, e.g.
    '/Game/Blueprints/Vehicles/BP_Ram.BP_Ram_C'         -> 'BP_Ram_C'
    'Default__BP_Ram_C'                                 -> 'BP_Ram_C'
    'BP_CaptureZone_C_UAID_A036BCBB4ACC..._1706575279'  -> 'BP_CaptureZone_C'
    'BP_Ram_C_2147433254'                               -> 'BP_Ram_C'
    """
    name = path.rsplit("/", 1)[-1].rsplit(".", 1)[-1].rsplit(":", 1)[-1]
    if name.startswith("Default__"):
        name = name[len("Default__"):]
    i = name.find("_UAID_")
    if i > 0:
        name = name[:i]
    if name.endswith("_GEN_VARIABLE"):
        name = name[:-len("_GEN_VARIABLE")]
    # dynamic-instance numeric suffix: BP_Ram_C_2147433254 -> BP_Ram_C
    head, _, tail = name.rpartition("_")
    if head and tail.isdigit() and len(tail) >= 2:
        name = head
    return name


_WORD_RE = re.compile(r"[A-Z]+(?=[A-Z][a-z])|[A-Z]?[a-z0-9]+|[A-Z]+")


def _tokenize(name):
    """Split a PascalCase/underscore identifier into lowercase word tokens for
    State._lookup_class's last-resort fuzzy match. Drops the single-letter
    'C' token (near-universal Blueprint-generated-class suffix, carries no
    disambiguating information)."""
    return {w.lower() for w in _WORD_RE.findall(name)} - {"c"}


def _normalize_for_match(name):
    """Lowercase, separator-stripped form for State._lookup_class's substring
    fallback (catches case-only mismatches like instance name 'MinimapComponent'
    vs class 'TyrMiniMapComponent', where class_key()/suffix matching fails
    only because of an internal capitalization difference)."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


# NetGUID / export table

def internal_load_object(r, state, is_exporting, depth=0):
    """UPackageMapClient::InternalLoadObject. Works on ByteReader or BitReader.
    Returns the netguid (int); registers guid->path in the cache when the
    stream carries an export entry."""
    if depth > 16:
        raise DecodeError("InternalLoadObject recursion limit")
    guid = r.int_packed()
    if guid == 0:                                     # !IsValid()
        return guid
    if guid == 1 or is_exporting:                     # IsDefault() or exporting
        flags = r.u8()
        if flags & EXPORT_FLAG_HAS_PATH:
            outer = internal_load_object(r, state, True, depth + 1)
            path = r.fstring()
            if flags & EXPORT_FLAG_HAS_NETWORK_CHECKSUM:
                r.u32()                               # networkChecksum
            state.guid_paths[guid] = path
            if outer:
                state.guid_outer[guid] = outer
    return guid


def receive_net_guid_bunch(bits, state):
    """UPackageMapClient::ReceiveNetGUIDBunch (bunch with bHasPackageMapExports)."""
    b_has_replayout_export = bits.read_bit()
    if b_has_replayout_export:
        # Legacy compat path (ReceiveNetFieldExportsCompat) - not expected on
        # engineNetworkVersion 42. Log + abort this bunch defensively.
        state.warn["netguid_bunch_compat_path"] += 1
        raise DecodeError("unexpected ReceiveNetFieldExportsCompat path")
    num = bits.i32()
    if not (0 <= num <= MAX_GUID_COUNT):
        raise DecodeError(f"NumGUIDsInBunch {num} out of range")
    for _ in range(num):
        internal_load_object(bits, state, True)


# demo-frame export data (byte-aligned)

def read_net_field_export(r, state):
    """FNetFieldExport (byte archive; engineNetworkVersion >= 10 -> FName name)."""
    if not r.bool8():                                 # bExported
        return None
    handle = r.int_packed()
    r.u32()                                           # compatibleChecksum
    name = r.fname()
    return (handle, name)


def read_net_field_exports(r, state):
    """UPackageMapClient::SerializeNetFieldExportDelta (demo frame path)."""
    n = r.int_packed()
    if n > 100_000:
        raise DecodeError(f"numLayoutCmdExports {n} implausible")
    for _ in range(n):
        path_name_index = r.int_packed()
        is_exported = r.int_packed() == 1
        if is_exported:
            path_name = r.fstring()
            num_exports = r.int_packed()
            if num_exports > 100_000:
                raise DecodeError(f"numExports {num_exports} implausible")
            group = state.groups_by_path.get(path_name)
            if group is None:
                group = {"path": path_name, "index": path_name_index,
                         "num": num_exports, "fields": {}}
                state.groups_by_path[path_name] = group
                state.groups_by_class[class_key(path_name)] = group
            group["num"] = max(group["num"], num_exports)
            state.groups_by_index[path_name_index] = group
        else:
            group = state.groups_by_index.get(path_name_index)
            if group is None:
                state.warn["netfield_export_unknown_group_index"] += 1
        field = read_net_field_export(r, state)
        if field is not None and group is not None:
            group["fields"][field[0]] = field[1]


def read_net_export_guids(r, state):
    n = r.int_packed()
    if n > 100_000:
        raise DecodeError(f"numGuids {n} implausible")
    for _ in range(n):
        size = r.i32()
        if not (0 <= size <= 1 << 20):
            raise DecodeError(f"export guid blob size {size} implausible")
        sub = ByteReader(r.rd(size))
        internal_load_object(sub, state, True)


def read_external_data(r, state):
    while True:
        num_bits = r.int_packed()
        if num_bits == 0:
            return
        guid = r.int_packed()
        payload = r.rd((num_bits + 7) >> 3)
        state.external_data[guid] += 1
        del payload  # not yet interpreted


# property / content-block layer (bit-level, inside bunches)

LOCATION_QUANTIZATION_SCALE = 100   # Tyr vehicle pawns: RoundTwoDecimals


def try_decode_rep_movement(payload, nbits):
    """FRepMovement::NetSerialize with quantization-combo guessing.

    The wire format depends on per-actor quantization settings we can't see,
    but the payload is length-framed, so try all combos and keep those that
    consume exactly nbits. The LWC quantized-vector path's scale factor does
    NOT change the bit count (only the rotator choice and flag bits do), so we
    can't tell the scale from bit-alignment -- but empirically Tyr's vehicle
    pawns quantize Location at RoundTwoDecimals (scale 100): the raw component
    ints run ~7-13 million for on-map positions (65000-128000 cm), i.e. the
    real cm value times 100. Decoding Location with scale 1 leaves every
    position 100x too large (what the map renderer used to patch
    up after the fact); scale 100 yields true-cm positions at the source. See
    LOCATION_QUANTIZATION_SCALE. Velocity/angular-velocity stay at scale 1
    (RoundWholeNumber -- their raw ints already read as plausible cm/s).

    NOTE ON "SPIKES": earlier sessions chased position spikes as a bit-decode
    bug. They are not. They came from channel-index REUSE -- when an actor
    (a tank) is destroyed its channel closes and the same index is later
    reopened for a brand-new actor (ammunition, a respawned/other tank). All
    those actors' ReplicatedMovement samples were being concatenated under the
    one channel index, so a consumer drew teleport lines between unrelated
    actors. receive_properties now keys movement_samples by ACTOR guid, so each
    key holds exactly one actor's trajectory and the spikes vanish at the
    source (no velocity filter needed downstream)."""
    results = []
    for rot_short in (False, True):
        try:
            b = BitReader(payload, nbits)
            b_simulated_sleep = b.read_bit()
            b_rep_physics = b.read_bit()
            # engineNetworkVersion 42 >= 25
            b_server_frame = b.read_bit()
            b_server_handle = b.read_bit()
            loc = b.read_quantized_vector(LOCATION_QUANTIZATION_SCALE)
            rot = b.read_rotation_short() if rot_short else b.read_rotation_byte()
            vel = b.read_quantized_vector(1)
            if b_rep_physics:
                b.read_quantized_vector(1)            # angular velocity
            if b_server_frame:
                b.int_packed()
            if b_server_handle:
                b.int_packed()
            # engineNetworkVersion 42 >= 35 (RepMoveOptionalAcceleration)
            if b.read_bit():
                b.read_quantized_vector(1)            # acceleration
            if b.at_end():
                results.append({"loc": loc, "rot": rot, "vel": vel,
                                "rotShort": rot_short, "physics": b_rep_physics})
        except DecodeError:
            continue
    return results


def decode_unique_net_id(payload):
    """Decode an FUniqueNetIdRepl (BP_TyrPlayerState_C.UniqueID) property
    payload to a canonical 17-digit SteamID64 decimal string, or None if it
    isn't a decodable Steam id.

    Wire format (FUniqueNetIdRepl::NetSerialize "Encoded" path -- confirmed
    empirically against all 16 players in TyrReplay3, every sample 88 bits /
    11 bytes, header 0x1d 0x09):
        byte 0  EncodingFlags: bit0 Encoded, bit1 EmptyId, bit2 IsPadded; the
                upper 5 bits (>>3) are the OSS type-hash index (== 3, Steam, in
                every sample). We only require the Encoded bit.
        byte 1  EncodedSize: count of following data bytes (9 => an 18-digit,
                zero-padded SteamID64).
        bytes 2.. EncodedSize bytes of packed BCD -- each byte is two decimal
                digits, high nibble first (0x76 -> "76"). Concatenate, strip the
                pad/leading zero, and that's the decimal id.
    Returns the SteamID64 string, or None on any malformed field (empty id,
    non-BCD nibble, short buffer, out-of-range result) so a bad record is just
    skipped, never fatal."""
    if not payload or len(payload) < 2:
        return None
    flags = payload[0]
    if not (flags & 0x01):                # NotEncoded / EmptyId -> not our path
        return None
    size = payload[1]
    if size == 0 or len(payload) < 2 + size:
        return None
    digits = []
    for b in payload[2:2 + size]:
        hi, lo = b >> 4, b & 0x0F
        if hi > 9 or lo > 9:              # not valid packed BCD -> reject
            return None
        digits.append(str(hi))
        digits.append(str(lo))
    dec = "".join(digits).lstrip("0")
    # SteamID64 individual-account universe: 76561197xxxxxxxxx..76561199xxxxxxxxx
    if len(dec) != 17 or not dec.startswith("7656119"):
        return None
    return dec


# research-only raw property capture, see probe.py
# off by default: the production pipeline (replay_site/replay_to_site) never
# turns this on, so the decode path below is byte-for-byte unchanged for it.
# When enabled, every (class, prop) update also keeps its raw payload + bit
# width, capped per key, so a format-detection pass can work out what the
# UNdecoded properties on the wire actually are without re-implementing the
# whole channel walk. Purely additive: nothing reads raw_prop_samples unless
# a research script asks for it.
CAPTURE_ALL_PROPS = False
CAPTURE_PER_KEY = 60
CAPTURE_STRIDE = 37   # coprime-ish with typical update cadences, so the
# every-Nth tail doesn't lock onto one actor's replication rhythm


def receive_properties(bits, group, state, ch, enable_checksum=True):
    """FRepLayout::ReceiveProperties -- packed property-handle loop."""
    if enable_checksum:
        bits.read_bit()                               # doChecksum
    cls = class_key(group["path"])
    while True:
        handle = bits.int_packed()
        if handle == 0:
            break
        handle -= 1                                   # 1-based on the wire
        if handle > group["num"]:
            raise DecodeError(f"handle {handle} > group size {group['num']} ({cls})")
        num_bits = bits.int_packed()
        if num_bits > bits.bits_left():
            raise DecodeError(f"prop numBits {num_bits} > left {bits.bits_left()} ({cls})")
        name = group["fields"].get(handle, f"handle{handle}")
        state.prop_counter[(cls, name)] += 1
        payload = bits.read_bits_bytes(num_bits) if num_bits else b""

        if CAPTURE_ALL_PROPS:
            key = (cls, name)
            seen = state.raw_prop_seen[key] = state.raw_prop_seen.get(key, 0) + 1
            bucket = state.raw_prop_samples[key]
            # Reservoir-ish: keep the first CAPTURE_PER_KEY, then every Nth
            # after, so a high-frequency property is sampled across the whole
            # match instead of only its opening seconds (a first-N-only cap
            # made every observed value range look artificially narrow).
            if len(bucket) < CAPTURE_PER_KEY or seen % CAPTURE_STRIDE == 0:
                bucket.append((round(state.time, 3), ch.index, ch.actor_guid, num_bits, payload))

        # --- opportunistic value decodes (never fatal) ---
        if name == "ReplicatedMovement" and num_bits:
            decoded = try_decode_rep_movement(payload, num_bits)
            # Key by actor guid, not channel index: a channel index is recycled
            # across many actors over a match (each closes on death and the
            # index is reused for the next actor), so keying by ch.index would
            # concatenate unrelated actors' trajectories -> the "teleport
            # spikes". ch.actor_guid is unique per actor and stable for that
            # actor's lifetime, so each key holds exactly one trajectory.
            if decoded and ch.actor_guid is not None \
                    and len(state.movement_samples[ch.actor_guid]) < 5000:
                state.movement_samples[ch.actor_guid].append(
                    (round(state.time, 3), decoded[0]["loc"], decoded[0]["rot"],
                     decoded[0]["vel"], decoded[0]["rotShort"]))
        elif name in ("PlayerName", "PlayerNamePrivate", "PlayerTitle",
                      "PlayerClan") and num_bits >= 40:
            try:
                sub = BitReader(payload, num_bits)
                s = sub.fstring()
                if sub.at_end() and s:
                    state.string_props[(cls, name)].add(s)
                    if name in ("PlayerName", "PlayerNamePrivate") and cls in PLAYER_IDENTITY_CLASSES:
                        rec = state.player_identity[ch.index]
                        rec["name"] = s
                        rec["class"] = cls
                        rec["actorGuid"] = ch.actor_guid
                    elif name == "PlayerClan" and cls in PLAYER_IDENTITY_CLASSES:
                        # same channel as PlayerName/TeamId above - ch.index
                        # is how collect_player_roster joins this back to a
                        # real player. Only a nonempty tag ever reaches here
                        # (the outer "and s" guard) - an unclanned player
                        # simply never sets this key, rather than storing "".
                        state.player_identity[ch.index]["clan"] = s
            except DecodeError:
                pass
        elif (name == "TeamId" and cls in PLAYER_IDENTITY_CLASSES
              and 0 < num_bits <= 32):
            # small fixed-width int (uint8 on the wire in every sample seen so
            # far, but decode generically up to 32 bits in case it widens).
            rec = state.player_identity[ch.index]
            rec["team_id"] = int.from_bytes(payload, "little")
            rec["class"] = cls
            rec["actorGuid"] = ch.actor_guid
        elif (name == "CompressedPing" and cls in PLAYER_IDENTITY_CLASSES
              and num_bits == 8):
            # UE stores ping*4 in a byte, so the real millisecond value is
            # value*4. Attributed per channel like name/team.
            state.player_identity[ch.index].setdefault("ping_samples", []).append(
                payload[0] * 4)
        elif (name in ("CurrentClipSize", "Duration") and cls == "TyrAmmunitionComponent"
              and num_bits == 32):
            # reload cadence: clip size counts down as shells are fired,
            # Duration is that weapon's reload time.
            val = struct.unpack("<f", payload[:4])[0]
            if math.isfinite(val):
                # keep the actor guid, not just the channel: the ammunition
                # component rides its owning vehicle's channel, and channel
                # indices get recycled across actors during a match, so the
                # guid is what reliably ties a reload to a tank.
                state.ammo_state[name].append(
                    (round(state.time, 3), ch.index, ch.actor_guid, val))
        elif (name in ("CurrentCaptureTimer", "CurrentCooldownTimer")
              and cls.endswith("Zone_C") and num_bits == 64):
            val = struct.unpack("<d", payload[:8])[0]
            if math.isfinite(val):
                # channel matters: a map has several zones of the same class,
                # and their timers interleave. Grouping by class alone makes
                # every switch between instances look like a reset.
                state.zone_events.append(
                    (round(state.time, 3), ch.index, cls, name, round(val, 2)))
        elif (name in LOADOUT_TAG_FIELDS and cls in PLAYER_IDENTITY_CLASSES
              and 0 < num_bits <= 32):
            # FGameplayTag replicated as a net index. The raw int isn't a
            # stable public id - it's an index into the game's own tag table,
            # so it only means anything relative to other values in the same
            # build. Stored raw and resolved to a readable name downstream by
            # correlation (see the loadout mapping downstream):
            # VehicleTag can be learned from the tank we already identify
            # independently, which is what proves the decode is right.
            # Attributed per-channel like name/team so collect_player_roster
            # can join it to this channel's gamertag.
            rec = state.player_identity[ch.index]
            rec[LOADOUT_TAG_FIELDS[name]] = int.from_bytes(payload, "little")
            rec["class"] = cls
        elif (name in ("UniqueID", "UniqueId") and cls in PLAYER_IDENTITY_CLASSES
              and num_bits >= 16):
            # FUniqueNetIdRepl -> SteamID64 (see decode_unique_net_id). Attributed
            # per-channel like name/team so collect_player_roster can tie it to
            # this channel's decoded gamertag; the property only rides the
            # BP_TyrPlayerState_C channel (not BP_PlayerRecord_C).
            sid = decode_unique_net_id(payload)
            if sid:
                rec = state.player_identity[ch.index]
                rec["steam_id"] = sid
                rec["class"] = cls
                rec["actorGuid"] = ch.actor_guid
        elif (name == "KillerRef" and cls == "BP_TyrPlayerState_C"
              and 0 < num_bits <= 64 and ch.actor_guid is not None):
            # UObject reference -> the killer's netguid (resolved to a player by
            # derive_death_events). Same one-InternalLoadObject wire shape as
            # AvatarActor below; require an exact-length parse.
            try:
                sub = BitReader(payload, num_bits)
                killer_guid = internal_load_object(sub, state, False)
                if sub.at_end() and len(state.killer_refs) < 5000:
                    state.killer_refs.append(
                        (round(state.time, 3), ch.index, ch.actor_guid, killer_guid))
            except DecodeError:
                pass
        elif (name == "bIsAlive" and cls in PLAYER_IDENTITY_CLASSES
              and 0 < num_bits <= 8 and ch.actor_guid is not None):
            # bool: 1 bit on the wire (accept up to a byte defensively)
            if len(state.alive_flags) < 5000:
                state.alive_flags.append(
                    (round(state.time, 3), ch.index, ch.actor_guid, cls,
                     bool(int.from_bytes(payload, "little") & 1)))
        elif (name in ("bIsPointCaptured", "NumAllies", "NumEnemies", "CapturePoints")
              and cls == "BP_CaptureZone_C" and 0 < num_bits <= 32
              and ch.actor_guid is not None):
            if len(state.capture_zone_events) < 20000:
                state.capture_zone_events.append(
                    (round(state.time, 3), ch.index, ch.actor_guid, name,
                     int.from_bytes(payload, "little")))
        elif (name == "Owner" and cls in PLAYER_IDENTITY_CLASSES and num_bits):
            # AActor::Owner is COND_OwnerOnly, so in a client-recorded replay
            # the server only ever sent it for the recording player's own
            # actors. Exactly one of the 16 players carries it, which is what
            # identifies who recorded the file (the upload validator uses
            # this to require that an uploader is the recorder, not merely
            # someone who happened to be in the match).
            state.owner_only_channels.add(ch.index)
        elif (name in MATCH_META_FIELDS and cls == "BP_TyrGameState_C"
              and num_bits):
            key = MATCH_META_FIELDS[name]
            if name == "Matchdetailsid":
                # FString: int32 count, then count bytes including the NUL
                # (negative count would mean UTF-16; never observed here).
                try:
                    n = struct.unpack("<i", payload[:4])[0]
                    if 0 < n <= len(payload) - 4:
                        state.match_meta[key] = (payload[4:4 + n]
                                                 .decode("utf-8", "replace")
                                                 .rstrip("\x00"))
                except (struct.error, IndexError):
                    pass
            elif name == "CurrentMatchPhase":
                # keep the highest phase seen - a completed match reaches 3,
                # so this doubles as "did the match actually finish".
                v = int.from_bytes(payload, "little")
                if v > state.match_meta.get(key, -1):
                    state.match_meta[key] = v
            elif num_bits == 1:
                state.match_meta[key] = bool(int.from_bytes(payload, "little") & 1)
            else:
                state.match_meta[key] = int.from_bytes(payload, "little")
        elif (name == "TeamWithLastStandId" and cls == "BP_TyrGameState_C"
              and 0 < num_bits <= 32):
            if len(state.last_stand_events) < 5000:
                raw = int.from_bytes(payload, "little")
                team_id = None if raw == 0xFFFFFFFF else raw
                state.last_stand_events.append((round(state.time, 3), team_id))
        elif (name == "AvatarActor" and cls == "AbilitySystemComponent"
              and 0 < num_bits <= 64 and ch.actor_guid is not None):
            # UObject reference -> netguid (see State.avatar_links). Payload is
            # one InternalLoadObject; require it to consume the field exactly.
            try:
                sub = BitReader(payload, num_bits)
                avatar_guid = internal_load_object(sub, state, False)
                if sub.at_end() and len(state.avatar_links) < 20_000:
                    state.avatar_links.append(
                        (round(state.time, 3), ch.actor_guid, avatar_guid))
            except DecodeError:
                pass
        elif (name == "Instigator" and cls.startswith("BP_Ammunition_")
              and 0 < num_bits <= 64 and ch.actor_guid is not None):
            # experimental - see State.ammo_instigators for coverage/quality
            # notes. Same one-InternalLoadObject wire shape as AvatarActor.
            try:
                sub = BitReader(payload, num_bits)
                instigator_guid = internal_load_object(sub, state, False)
                if sub.at_end() and len(state.ammo_instigators) < 20_000:
                    state.ammo_instigators.append(
                        (round(state.time, 3), ch.actor_guid, instigator_guid))
            except DecodeError:
                pass
        elif (cls == "TyrTeamPublicInfo" and num_bits == 32
              and name in ("TeamId", "CurrentTeamHealth", "MaxTeamHealth")
              and ch.actor_guid is not None):
            # Team health pool + which team owns it (see State.team_info).
            # All three replicate as plain 32-bit little-endian ints
            # (validated on TyrReplay4: TeamId in {0,1}; pools count down
            # 9984->1178 / 9681->0 across the match).
            v = int.from_bytes(payload, "little")
            rec = state.team_info[ch.actor_guid]
            if name == "TeamId":
                rec["teamId"] = v
            else:
                key = "health" if name == "CurrentTeamHealth" else "maxHealth"
                rec.setdefault(key, []).append((round(state.time, 3), v))
        elif (num_bits == 32 and name in ("CurrentValue", "BaseValue")
              and cls.startswith("TyrAttributeSet")):
            v = struct.unpack("<f", payload)[0]
            if len(state.attr_samples) < 200_000:
                state.attr_samples.append(
                    (round(state.time, 3), ch.index, cls, handle, name, round(v, 3)))


def read_content_block_header(bits, state, ch):
    """UActorChannel::ReadContentBlockHeader. Returns (repObjGuid|None,
    bHasRepLayout, bObjectDeleted)."""
    b_has_replayout = bits.read_bit()
    b_is_actor = bits.read_bit()
    if b_is_actor:
        return (ch.archetype_guid or ch.actor_guid), b_has_replayout, False
    # sub-object
    obj_guid = internal_load_object(bits, state, False)
    b_stably_named = bits.read_bit()
    if b_stably_named:
        # stably-named subobject (e.g. a component): repObject is the object
        # itself; its group is found via its (class-ish) instance name
        return obj_guid, b_has_replayout, False
    # engineNetworkVersion 42 >= SUBOBJECT_DESTROY_FLAG (30)
    b_delete = False
    b_serialize_class = True
    if bits.read_bit():                               # bIsDestroyMessage
        b_delete = True
        b_serialize_class = False
        bits.u8()                                     # destroy flags
    class_guid = None
    if b_serialize_class:
        class_guid = internal_load_object(bits, state, False)
        b_delete = class_guid == 0
    if b_delete:
        return None, b_has_replayout, True
    # engineNetworkVersion 42 >= SUBOBJECT_OUTER_CHAIN (18)
    b_actor_is_outer = bits.at_end() or bits.read_bit()
    if not b_actor_is_outer:
        internal_load_object(bits, state, False)      # outer object
    return class_guid, b_has_replayout, False


def process_bunch_payload(bits, ch, b_open, state, b_has_must_be_mapped=False):
    """UActorChannel::ReceivedActorBunch + ProcessBunch on the (merged) bunch
    content. NOTE: the MustBeMapped guid list belongs to the MERGED bunch (for
    partials, the flag is taken from the final fragment), per
    UActorChannel::ReceivedActorBunch."""
    if b_has_must_be_mapped:
        n = bits.u16()
        for _ in range(n):
            bits.int_packed()
    if ch.actor_guid is None:
        if not b_open:
            state.warn["nonopen_bunch_on_actorless_channel"] += 1
            return
        # UPackageMapClient::SerializeNewActor
        actor_guid = internal_load_object(bits, state, False)
        ch.actor_guid = actor_guid
        ch.opened_time = state.time
        dynamic = actor_guid > 1 and (actor_guid & 1) == 0
        loc = rot = vel = None
        if bits.at_end() and dynamic:
            state.channel_opens.append(_open_rec(state, ch, loc, rot, vel))
            return
        if dynamic:
            ch.archetype_guid = internal_load_object(bits, state, False)
            internal_load_object(bits, state, False)  # level
            loc = conditionally_quantized_vector(bits, (0.0, 0.0, 0.0))
            rot = bits.read_rotation_short() if bits.read_bit() else (0.0, 0.0, 0.0)
            scale = conditionally_quantized_vector(bits, (1.0, 1.0, 1.0))  # scale
            vel = conditionally_quantized_vector(bits, (0.0, 0.0, 0.0))
        state.channel_opens.append(_open_rec(state, ch, loc, rot, vel, scale if dynamic else None))
        # PlayerController channels carry one extra netPlayerIndex byte
        arch_path = state.guid_paths.get(ch.archetype_guid or 0, "")
        akey = class_key(arch_path) if arch_path else ""
        if akey.startswith("PC_") or "PlayerController" in akey:
            if not bits.at_end():
                bits.u8()

    # content blocks
    trace = ch.index == TRACE_CHANNEL
    while not bits.at_end():
        pos0 = bits.pos
        rep_obj, b_has_replayout, b_deleted = read_content_block_header(bits, state, ch)
        if trace:
            print(f"    [trace ch={ch.index} t={state.time:.3f}] block@bit{pos0}: "
                  f"repObj={rep_obj} ({state.guid_paths.get(rep_obj or 0)}) "
                  f"hasRepLayout={b_has_replayout} deleted={b_deleted} "
                  f"left={bits.bits_left()}")
        if b_deleted:
            continue
        payload_bits = bits.int_packed()
        if trace:
            print(f"      payloadBits={payload_bits} left={bits.bits_left()}")
        if payload_bits > bits.bits_left():
            raise DecodeError(f"content-block payload {payload_bits} > left {bits.bits_left()}")
        payload = bits.read_bits_bytes(payload_bits)
        if rep_obj is None or payload_bits == 0:
            continue
        sub = BitReader(payload, payload_bits)
        group = state.group_for_guid(rep_obj)
        if group is not None and b_has_replayout:
            try:
                receive_properties(sub, group, state, ch)
            except DecodeError:
                state.warn["receive_properties_failed"] += 1
                continue
        elif group is None:
            state.warn["no_group_for_repobject"] += 1
            continue
        if sub.at_end():
            continue
        # remaining data = ClassNetCache fields (RPCs / custom-delta properties)
        cnc = state.classnetcache_for_guid(rep_obj)
        if cnc is None:
            state.warn["no_classnetcache"] += 1
            continue
        cls = class_key(group["path"]) if group else class_key(state.guid_paths.get(rep_obj, "?"))
        if cls in RPC_CAPTURE_CLASSES and not b_has_replayout:
            samples = state.rpc_raw_blocks[cls]
            if len(samples) < RPC_CAPTURE_CAP:
                samples.append((round(state.time, 3), ch.index,
                                sub.bits_left(), sub.read_bits_bytes(sub.bits_left())))
                sub = BitReader(samples[-1][3], samples[-1][2])
        try:
            while not sub.at_end():
                handle = sub.serialized_int(max(cnc["num"] + 1, 2))
                field_bits = sub.int_packed()
                if field_bits > sub.bits_left():
                    raise DecodeError("field payload too big")
                field_payload = sub.read_bits_bytes(field_bits)
                name = cnc["fields"].get(handle, f"field{handle}")
                state.rpc_counter[(cls, name)] += 1
                if name in RPC_CAPTURE_NAMES:
                    samples = state.rpc_payloads[(cls, name)]
                    if len(samples) < RPC_CAPTURE_CAP:
                        samples.append((round(state.time, 3), ch.index, field_bits, field_payload))
                if name in EVENT_RPC_NAMES:
                    samples = state.event_rpcs[name]
                    if len(samples) < EVENT_RPC_CAP:
                        samples.append((round(state.time, 3), ch.index,
                                        ch.actor_guid, cls, field_bits, field_payload))
        except DecodeError:
            state.warn["classnetcache_fields_failed"] += 1


def _open_rec(state, ch, loc, rot, vel, scale=None):
    return {
        "time": round(state.time, 3),
        "ch": ch.index,
        "actorGuid": ch.actor_guid,
        "actorPath": state.guid_paths.get(ch.actor_guid),
        "archetypeGuid": ch.archetype_guid,
        "archetypePath": state.guid_paths.get(ch.archetype_guid or 0),
        "location": loc, "rotation": rot, "velocity": vel, "scale": scale,
    }


def conditionally_quantized_vector(bits, default):
    """UPackageMapClient::SerializeNewActor helper (engineNetworkVersion >= 13).

    Location, Scale and Velocity each read their own bWasSerialized bit, then
    if set their own bShouldQuantize bit, before the vector payload.

    Worth writing down because I got this wrong twice. A chunk of Velocity
    reads come out implausible (>5000 cm/s) and fall through to the unquantized
    f64 branch as garbage, which looks like Scale/Velocity might not carry
    their own quantize bit. They do. I tried both alternatives over the whole
    corpus - dropping the per-field bit entirely, and reusing Location's
    decision for all three - and both made implausible-scale counts and the
    stream-desync counters much worse. Fewer bad velocities but more desync
    means you've shifted the bit stream, not decoded it better.

    So the layout below is right and the leftover bad velocities are something
    else. Either the unquantized f64 fallback has a wrong field order for
    engineNetworkVersion=42, or some of them are real launch/explosion physics
    velocities and the 5000 threshold is just too tight. Histogram them before
    assuming they're bugs.
    """
    if not bits.read_bit():                           # bWasSerialized
        return default
    b_quantize = bits.read_bit()                      # bShouldQuantize
    if b_quantize:
        return bits.read_packed_vector(10, 24)
    # unquantized: engineNetworkVersion 42 >= 22 -> doubles
    return (bits.f64(), bits.f64(), bits.f64())


# packet / bunch layer

def process_packet(data, state):
    """UNetConnection::ReceivedRawPacket + ReceivedPacket (InternalAck path)."""
    if not data or data[-1] == 0:
        state.warn["packet_no_termination_bit"] += 1
        return
    # strip the terminating 1-bit: bit length = index of last set bit
    last = data[-1]
    nbits = len(data) * 8 - 1
    while not (last & 0x80):
        last = (last << 1) & 0xFF
        nbits -= 1
    bits = BitReader(data, nbits)
    state.packets += 1

    while not bits.at_end():
        # engineNetworkVersion 42 >= ACKS_IN_HEADER -> no legacy ack bit
        b_control = bits.read_bit()
        b_open = b_control and bits.read_bit()
        b_close = b_control and bits.read_bit()
        # engineNetworkVersion 42 >= CHANNEL_CLOSE_REASON
        close_reason = bits.serialized_int(CHANNEL_CLOSE_REASON_MAX) if b_close else 0
        bits.read_bit()                               # bIsReplicationPaused
        b_reliable = bits.read_bit()
        ch_index = bits.int_packed()                  # >= MAX_ACTOR_CHANNELS_CUSTOMIZATION
        if ch_index > 32767:
            raise DecodeError(f"chIndex {ch_index} implausible")
        b_has_exports = bits.read_bit()
        b_has_must_be_mapped = bits.read_bit()
        b_partial = bits.read_bit()
        b_partial_initial = b_partial and bits.read_bit()
        # engineNetworkVersion 42 >= CustomExports(36) -> extra bit
        b_partial and bits.read_bit()                 # bHasPartialCustomExportsFinalBit
        b_partial_final = b_partial and bits.read_bit()
        ch_name = None
        if b_reliable or b_open:
            ch_name = bits.fname()
        bunch_bits = bits.serialized_int(MAX_PACKET_SIZE_IN_BITS)
        if bunch_bits > bits.bits_left():
            raise DecodeError(f"bunch size {bunch_bits} > bits left {bits.bits_left()}")
        payload = BitReader(bits.read_bits_bytes(bunch_bits), bunch_bits)
        state.bunches += 1

        ch = state.channels.get(ch_index)
        if ch is None:
            ch = Channel(ch_index)
            state.channels[ch_index] = ch
        if ch_name:
            ch.name = ch_name

        if b_has_exports:
            try:
                receive_net_guid_bunch(payload, state)
            except DecodeError:
                state.warn["netguid_bunch_failed"] += 1
                continue

        if ch.name in ("Control", "Voice"):
            continue                                  # NMT control messages, not actors

        try:
            if b_partial:
                handle_partial(payload, ch, state, b_partial_initial, b_partial_final,
                               b_reliable, b_open, b_close, close_reason,
                               b_has_must_be_mapped)
            else:
                process_bunch_payload(payload, ch, b_open, state, b_has_must_be_mapped)
                if b_close:
                    close_channel(state, ch, close_reason)
        except DecodeError as e:
            state.warn["bunch_content_failed"] += 1
            if len(state.log_lines) < 60:
                state.log(f"  bunch-content error t={state.time:.1f}s ch={ch_index} "
                          f"cls={state.channel_class(ch)}: {e}")


def handle_partial(payload, ch, state, b_initial, b_final, b_reliable, b_open,
                   b_close, close_reason, b_has_must_be_mapped):
    """UChannel::ReceivedNextBunch partial-bunch merging (per channel)."""
    if b_initial:
        ch.partial_data = bytearray(payload.read_bits_bytes(payload.bits_left())
                                    if payload.bits_left() else b"")
        ch.partial_bits = len(ch.partial_data) * 8
        ch.partial_reliable = b_reliable
        ch.partial_open = b_open
        # non-final partials must be byte-aligned; payload.bits_left() was a
        # multiple of 8 by construction here only if the recorder aligned it
        return
    if ch.partial_data is None:
        state.warn["partial_merge_without_initial"] += 1
        return
    n = payload.bits_left()
    if n:
        if ch.partial_bits % 8 != 0:
            state.warn["partial_append_unaligned"] += 1
            ch.partial_data = None
            return
        ch.partial_data += payload.read_bits_bytes(n)
        ch.partial_bits += n
    if b_final:
        merged = BitReader(bytes(ch.partial_data), ch.partial_bits)
        ch.partial_data = None
        # MustBeMapped flag is carried over from the final fragment
        process_bunch_payload(merged, ch, ch.partial_open, state, b_has_must_be_mapped)
        if b_close:
            close_channel(state, ch, close_reason)


def close_channel(state, ch, close_reason):
    state.channel_closes.append({
        "time": round(state.time, 3),
        "ch": ch.index,
        "actorGuid": ch.actor_guid,
        "class": state.channel_class(ch),
        "reason": CLOSE_REASONS.get(close_reason, str(close_reason)),
        "openedAt": ch.opened_time,
    })
    state.channels.pop(ch.index, None)


# frame / chunk layer

def parse_demo_frame(r, state, max_packet_dump=0, prescan=False):
    """UDemoNetDriver::ReadDemoFrameIntoPlaybackPackets (no streaming fixes).

    prescan=True: only extract per-frame export data (NetFieldExportGroups +
    NetGUID paths) and skip packet/bunch decoding entirely (packets are read
    as opaque byte blobs and discarded). Used by decode()'s first pass -- see
    its docstring for why groups need to be known ahead of the real pass."""
    r.i32()                                           # currentLevelIndex (nv >= 6)
    t = r.f32()
    if not (0.0 <= t < 100_000.0):
        raise DecodeError(f"frame time {t} implausible at off {r.off}")
    state.time = t
    # nv >= 10: per-frame export data
    read_net_field_exports(r, state)
    read_net_export_guids(r, state)
    # no HasStreamingFixes -> old streaming-level path (UDemoNetDriver's
    # pre-HasLevelStreamingFixes ReadDemoFrameIntoPlaybackPackets): per
    # entry, two FStrings (package name, package name to load) then an
    # FTransform serialized as Rotation(FQuat X,Y,Z,W) + Translation(FVector
    # X,Y,Z) + Scale3D(FVector X,Y,Z) - 10 components, doubles under UE5 LWC
    # (matches the doubles-for-unquantized-vectors fact already established
    # above for engineNetworkVersion 42 >= 22). Only observed on WindValley
    # so far (its map streams in a sub-level; Divide/Scorch/Ravine/Fields
    # never exercised this path, hence "count expected 0" being wrong).
    n_streaming = r.int_packed()
    for _ in range(n_streaming):
        r.fstring()                                   # PackageName
        r.fstring()                                   # PackageNameToLoad
        for _ in range(10):                            # FTransform
            r.f64()
    # no HasStreamingFixes -> no externalOffset u64 here
    read_external_data(r, state)
    # no GameSpecificFrameData -> no skipExternalOffset
    while True:
        size = r.i32()
        if size == 0:
            break
        if size < 0 or size > 2048:
            raise DecodeError(f"packet size {size} out of range at off {r.off}")
        pkt = r.rd(size)
        if prescan:
            continue
        try:
            process_packet(pkt, state)
        except DecodeError as e:
            state.warn["packet_failed"] += 1
            if len(state.log_lines) < 60:
                state.log(f"  packet error t={state.time:.1f}s: {e}")
    state.frames += 1


def parse_replay_data_chunk(payload, state, chunk_index, max_frames=None, prescan=False):
    """FLocalFileNetworkReplayStreaming ReplayData chunk (fileVersion 7)."""
    r = ByteReader(payload)
    time1 = r.u32()
    time2 = r.u32()
    length = r.i32()
    r.i32()                                           # memorySizeInBytes (v >= 6)
    if length != r.remaining():
        if not prescan:
            state.warn["replaydata_length_mismatch"] += 1
            state.log(f"chunk {chunk_index}: declared stream length {length} != "
                      f"remaining {r.remaining()}")
    stream = ByteReader(r.rd(min(length, r.remaining())))
    while not stream.at_end():
        if max_frames is not None and state.frames >= max_frames:
            return False
        try:
            parse_demo_frame(stream, state, prescan=prescan)
        except DecodeError as e:
            if not prescan:
                state.warn["frame_failed"] += 1
                state.log(f"chunk {chunk_index} (t={time1}..{time2}ms): frame parse "
                          f"failed at stream off {stream.off}: {e}")
            return True                               # abort rest of this chunk
    return True


def collect_player_roster(state):
    """Reduce State.player_identity (per-CHANNEL, two channels per real player --
    see PLAYER_IDENTITY_CLASSES) to one row per real player, deduped by decoded
    name. A player's BP_TyrPlayerState_C and BP_PlayerRecord_C channels both
    replicate the same PlayerName(Private)/TeamId, generally arriving at
    slightly different frame times, so either or both may have a team_id by
    the time decode() finishes -- last-write-wins per name is fine here since
    a player's team never changes mid-match.

    Returns [{"name": str, "team_id": int, "channels": [chIndex, ...],
    "steam_id": str|None, "clan": str|None}, ...], ONLY for entries that
    resolved both a name and a team_id (a channel that only got one of the
    two, e.g. cut off mid-stream, isn't actionable for the roster tool's
    join and is silently dropped -- this is a lossy net-stream, not every
    actor necessarily replicates both fields before the replay ends). steam_id
    is the decoded SteamID64 (decode_unique_net_id) when the player's
    BP_TyrPlayerState_C channel replicated a decodable UniqueID before the
    replay ended, else None -- it is a permanent, stable per-player id (see
    the roster tool for the format note), keyed to the same name. clan
    is the decoded PlayerClan tag, or None for an unclanned player / one whose
    clan tag never replicated before the replay ended.
    """
    by_name = {}
    steam_by_name = {}
    clan_by_name = {}
    loadout_by_name = defaultdict(dict)
    ping_by_name = {}
    for ch_index, rec in state.player_identity.items():
        name = rec.get("name")
        # steam_id/clan ride the PlayerState channel and may (rarely) land on
        # a channel whose team_id never replicated - capture them
        # independently of the name+team gate below so they aren't lost.
        if name and rec.get("steam_id") and name not in steam_by_name:
            steam_by_name[name] = rec["steam_id"]
        if name and rec.get("clan") and name not in clan_by_name:
            clan_by_name[name] = rec["clan"]
        # a player's loadout tags are split across their two channels
        # (vehicle/skin on PlayerState, keystone on PlayerRecord), so merge
        # rather than last-write-wins - otherwise whichever channel is seen
        # second wipes the other's tags.
        if name and rec.get("ping_samples"):
            ping_by_name.setdefault(name, []).extend(rec["ping_samples"])
        if name:
            for key in ("vehicle_tag", "skin_tag", "keystone_tag"):
                if rec.get(key) is not None:
                    loadout_by_name[name].setdefault(key, rec[key])
        team_id = rec.get("team_id")
        if not name or team_id is None:
            continue
        entry = by_name.setdefault(name, {"name": name, "team_id": team_id, "channels": []})
        entry["channels"].append(ch_index)
        # last-write-wins on team_id only if a later channel disagrees (should
        # not happen in practice; keep the first if so and don't crash on it)
    for name, entry in by_name.items():
        entry["steam_id"] = steam_by_name.get(name)
        entry["clan"] = clan_by_name.get(name)
        entry.update(loadout_by_name.get(name) or {})
        samples = sorted(ping_by_name.get(name) or [])
        # median, not mean: ping spikes are common and would drag an average
        entry["ping_ms"] = samples[len(samples) // 2] if samples else None
    return list(by_name.values())


def collect_team_health(state):
    """Reduce State.team_info (per TyrTeamPublicInfo actor) to one record per
    team id: {teamId: {"initial": int, "final": int, "max": int|None,
    "zeroAtSec": float|None}}.

    Each match spawns two TyrTeamPublicInfo actors; each replicates its own
    TeamId property, so the instance -> team mapping is direct (validated on
    TyrReplay4/Scorch: guid 888 TeamId=0, guid 870 TeamId=1, and the TeamId=0
    pool's 0-crossing at t=450.5 coincides with the last team-0 player's
    bIsAlive=false at t=450.4). An instance that never replicated a TeamId or
    any CurrentTeamHealth sample is dropped (lossy/cut stream), never fatal.
    """
    out = {}
    for rec in state.team_info.values():
        tid = rec.get("teamId")
        series = sorted(rec.get("health") or [])
        if tid is None or not series:
            continue
        maxs = sorted(rec.get("maxHealth") or [])
        zero_at = next((t for t, v in series if v == 0), None)
        out[tid] = {
            "initial": series[0][1],
            "final": series[-1][1],
            "max": maxs[-1][1] if maxs else None,
            "zeroAtSec": round(zero_at, 1) if zero_at is not None else None,
        }
    return out


def derive_match_result(state):
    """Derive the match winner from the two team health pools.

    Returns (winning_team_id | None, team_health) where team_health is
    collect_team_health()'s dict. The team whose pool is recorded hitting 0
    LOSES; the winner is only declared when BOTH teams' pools decoded and
    EXACTLY ONE of them ended at 0 (i.e. the replay actually captured the
    match-ending pool depletion). Anything else -- a cut/aborted replay, a
    missing instance, or (impossible in practice, but guarded) both pools at
    0 -- yields winner None rather than a guess.
    """
    team_health = collect_team_health(state)
    if set(team_health) != {0, 1}:
        return None, team_health
    dead = [tid for tid in (0, 1) if team_health[tid]["final"] == 0]
    if len(dead) != 1:
        return None, team_health
    return 1 - dead[0], team_health


def derive_last_stand_winner(state):
    """Derive a winner for matches derive_match_result() can't call (neither
    team's health pool decoded hitting 0 -- i.e. the match ended some other
    way than Elimination) from BP_TyrGameState_C.TeamWithLastStandId (see
    State.last_stand_events).

    REPLACES the original derive_capture_winner (see git history), which
    tried to read the winner off BP_CaptureZone_C's bIsPointCaptured/
    NumAllies/NumEnemies and was confirmed BACKWARDS on all 3 known capture
    matches -- the team with more kills/damage/remaining health was marked
    the loser every time. Root cause: each map spawns 2 capture-zone actors
    at fixed positions with no fixed team ownership, and the zone that
    actually gets captured in these 3 matches is one the recording player's
    team visibly abandons in the first ~20s while the enemy slowly builds
    up and caps it very late -- that is real LOCAL zone-contest history, but
    it reflects who wanted/won that one zone at the end, not who won the
    match overall (see State.capture_zone_events' updated docstring for the
    full autopsy). Capturing a zone is evidently a means to the real win
    condition, not the win condition itself, so no fix to the zone-reading
    logic alone could have been correct -- a different signal was needed.

    TeamWithLastStandId is that signal: it is a plain GameState property (no
    per-zone ambiguity at all) that names the team currently in "Last
    Stand" -- 0/1 for a real roster team id, or cleared (no team currently
    in it; see State.last_stand_events) once >=1 s after the sentinel would
    otherwise persist. Method: take the LAST non-cleared (team_id is not
    None) entry chronologically -- ignores intermediate clears (a team can
    enter Last Stand, claw back out, and re-enter; only the final state
    before the match ends matters) -- and declare the OTHER team the
    winner.

    Evidence (2026-07-19 validation, all 7 replays with decodable capture-
    zone AND team-health data): the last non-cleared TeamWithLastStandId
    team matches the independently-known LOSING team in all 7/7 cases --
    the 3 known-Capture matches (TyrReplay1/10/2: last-stand team 1 in all
    3, matching the enemy/team-1 being the confirmed-weaker side by kills/
    damage/remaining-health) AND, as a sanity check, all 4 known-Elimination
    matches (TyrReplay4/6/7/9: last-stand team always equals the team whose
    health pool derive_match_result() independently found hitting 0, e.g.
    TyrReplay4 last-stand team 0 vs its team-0 pool zeroing at t=450.5s).
    100% agreement, 0 exceptions -- not a curve fit to the 3 capture cases
    alone. This function is still only INTENDED to be consulted when
    derive_match_result() returns no winner, so the Elimination path (which
    already has a clean, independent signal) is never overridden by it; the
    cross-check above is there to prove this signal is real, not to justify
    replacing the health-pool check.

    Returns (winning_team_id | None, last_stand_time_sec | None). None when
    no TeamWithLastStandId update was ever decoded (lossy/cut stream, or a
    genuinely different end condition this signal doesn't cover) -- never a
    guess.
    """
    events = sorted(state.last_stand_events, key=lambda e: e[0])
    last_team = last_t = None
    for t, team_id in events:
        if team_id in (0, 1):
            last_team, last_t = team_id, t
    if last_team is None:
        return None, None
    return 1 - last_team, round(last_t, 1)


# Multicast_SendEndGameStats RPC parameter decode
#
# BP_TyrGameState_C.Multicast_SendEndGameStats is the one end-of-match RPC that
# hands the whole scoreboard to every client in a single call. It's a UFUNCTION
# parameter payload, not a normal property block, so there's no per-property
# handle framing - just the parameters in declaration order. The generic
# ClassNetCache handle loop in process_bunch_payload desyncs on it, hence
# snapshotting raw bytes into state.rpc_raw_blocks instead. One packed array of
# fixed-layout per-player records, everything bit-packed LSB-first.
#
# Worked the format out by anchoring on the player-name FStrings, since the
# roster already gives those independently, then reading fixed-width fields at
# constant bit offsets from the end of each name. Checked against three
# complete matches, 48 player records total.
#
# team (+49, 1 bit) matches the roster team_id on all 48. kills (+241) balances
# against deaths per team in every match. tankIdx (+18, 5 bits) is a stable
# tank enum, consistent with the AvatarActor-linked pawn class on all of them
# (codes in _EGS_TANK_CODENAMES). partyId is an optional trailing UUID FString,
# only there for players who queued as a squad, and always same-team.
#
# damage (+97) is inferred rather than proven - magnitudes look right and team
# damage tracks the enemy HP pool, but nothing conserves exactly. blocked
# (+145) and assist (+193) were pinned by Blueprint member order
# (DamageStat_10, BlockedStat_12, SpottingAssistStat_14, DestroyedStat_16 lines
# up with +97/+145/+193/+241), plus assist being nonzero far more often and
# frequently landing on multiples of 100, and blocked only showing up on
# armored and support chassis.
#
# +337 (5 bits, 0..25) and +345 (32 bits) are still unmapped and get preserved
# under "unmapped". +345 is unique per player and stable across matches, but
# it isn't SteamID64-derived and isn't a CRC32 of the name. Probably an
# internal account id.
#
# Widths: the four stat fields sit on a uniform 48-bit stride, 32-bit LE value
# then 16 bits of constant framing. All read as 32-bit. Narrower reads agreed
# on every record except one clipped assist of 2298 that an 11-bit read wrapped
# to 250, which is why they got widened.

# bit offsets of each per-record field, measured from the end of the name FString
_EGS_TEAM_OFF = 49
_EGS_STAT_BITS = 32                  # damage/blocked/assist/kills value width
_EGS_DMG_OFF = 97
_EGS_BLOCKED_OFF = 145               # BlockedStat (blocked damage)
_EGS_ASSIST_OFF = 193                # SpottingAssistStat (assist damage)
_EGS_KILLS_OFF = 241
_EGS_F337_OFF, _EGS_F337_BITS = 337, 5
_EGS_F345_OFF, _EGS_F345_BITS = 345, 32
_EGS_TANK_OFF, _EGS_TANK_BITS = 18, 5

# tankIdx -> internal tank codename (matches the site's TANK_PRETTY /
# TANK_DISPLAY naming). Every value confirmed against >=1 AvatarActor-linked
# pawn class in TyrReplay4/TyrReplay6, most against several, with zero
# conflicts across the two matches. Unlisted codes are tanks not yet seen
# with a resolvable pawn link - decode_end_game_stats emits tank=None for
# them while preserving the raw tankIdx.
_EGS_TANK_CODENAMES = {
    5: "Blink",          # display: Alecto
    7: "Bush",           # display: Ark
    8: "CanOpener",      # display: Arbalest (confirmed in-game by user)
    10: "Drone",         # display: Kestrel
    11: "Healer",        # display: Valor
    12: "Ram",           # display: Maul
    13: "Ranger",
    14: "Tempest",       # identified by exact max-HP match (1275) vs tyrhq.com's stat sheet
    15: "SentinelTank",  # display: Tricera (confirmed in-game by user)
    16: "Helio",         # identified by exact max-HP match (1750) vs tyrhq.com's stat sheet
    17: "Sonar",         # display: Atlas
    19: "Stealth",       # display: Phantom
    20: "Vanguard",
    21: "Vtol",          # display: Ikarus (confirmed in-game by user)
    22: "Rook",          # identified by exact max-HP match (1425) vs tyrhq.com's stat sheet
    6: "Brawler",        # display: Fortis (user asked after it in-game; confirmed
                          # 2026-07-20 by AvatarActor track-link in match
                          # 64778fe83436 - player Crudealisk's pawn resolved to
                          # BP_Brawler_Proxy_C with tankIdx=6 on the wire)
    9: "Deadeye",         # already had a confirmed display name (TANK_DISPLAY) but
                          # no tankIdx until now; confirmed 2026-07-20 the same way
                          # (player Stoney's pawn -> BP_Deadeye_C, tankIdx=9)
}
# 14/16/22 have no distinct internal Blueprint codename discovered yet (no
# player's pawn was ever track-linked for these - their AvatarActor only
# ever pointed at Wall/Recall ability props, see VEHICLE_CLASSES' comment in
# replay_site.py), so the tyrhq display name doubles as the codename here.

_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


def _egs_try_fstring(reader, pos):
    """Attempt to read a net-serialized FString at bit `pos` of BitReader
    `reader`. Returns (text, end_bit) on a clean parse (plausible length +
    valid encoding + NUL terminator), else None. Does not mutate `reader`."""
    nbits = reader.nbits
    if pos + 32 > nbits:
        return None
    save = reader.pos
    try:
        reader.pos = pos
        ln = reader.i32()
        if ln == 0:
            return ("", reader.pos)
        if 0 < ln <= 256:
            if pos + 32 + ln * 8 > nbits:
                return None
            raw = reader.read_bits_int(ln * 8).to_bytes(ln, "little")
            if raw[-1] != 0:
                return None
            return (raw[:-1].decode("utf-8"), reader.pos)
        if -256 <= ln < 0:
            cnt = -ln
            if pos + 32 + cnt * 16 > nbits:
                return None
            raw = reader.read_bits_int(cnt * 16).to_bytes(cnt * 2, "little")
            if raw[-2:] != b"\x00\x00":
                return None
            return (raw[:-2].decode("utf-16-le"), reader.pos)
        return None
    except (DecodeError, UnicodeDecodeError, ValueError):
        return None
    finally:
        reader.pos = save


def decode_end_game_stats(state):
    """Decode the captured Multicast_SendEndGameStats payload into a per-player
    list of end-game stats. Returns [] when the RPC was not captured (e.g. a
    replay that ends before the end-of-match multicast).

    Each entry:
        {"name": str, "team": int, "kills": int, "damage": int,
         "blocked": int, "assist": int,
         "tankIdx": int, "tank": str|None,   # codename, e.g. "Ram" (= Maul)
         "partyId": str|None,
         "unmapped": {"field337": int, "field345": int}}

    blocked = BlockedStat (blocked damage), assist = SpottingAssistStat --
    see the mapping evidence in the module comment block above.

    Defensive by record: a field read that runs off the end aborts only that
    one player (its stats come back None), never the whole scoreboard.
    """
    blocks = state.rpc_raw_blocks.get("BP_TyrGameState_C")
    if not blocks:
        return []
    # The end-of-match multicast is the largest content block (the whole
    # scoreboard); earlier same-class blocks, if any, are ordinary replication.
    _, _, nbits, raw = max(blocks, key=lambda b: b[2])
    reader = BitReader(raw, nbits)

    roster = collect_player_roster(state)
    team_by_name = {p["name"]: p["team_id"] for p in roster}
    if not team_by_name:
        return []

    # One forward pass over every bit offset, collecting each clean FString that
    # is either a known roster name or a UUID-form squad id. Anchoring on names
    # we already know (from the per-channel identity stream) sidesteps having to
    # model the packed-int header/field widths that precede the array.
    name_hits = defaultdict(list)   # name -> [(start_bit, end_bit), ...]
    uuid_hits = []                  # [(start_bit, end_bit, text), ...]
    pos = 0
    while pos < nbits - 32:
        got = _egs_try_fstring(reader, pos)
        if got is not None:
            text, end = got
            if text in team_by_name:
                name_hits[text].append((pos, end))
            elif _UUID_RE.match(text):
                uuid_hits.append((pos, end, text))
        pos += 1

    def _read_int(name_end, off, bits):
        reader.pos = name_end + off
        return reader.read_bits_int(bits)

    def _read_bit(name_end, off):
        reader.pos = name_end + off
        return int(reader.read_bit())

    # Resolve each roster name to a single occurrence: prefer the one whose
    # team bit matches the roster (guards against a short name accidentally
    # forming a valid FString elsewhere in the packed stream), else the first.
    records = []
    for name, expected_team in team_by_name.items():
        hits = name_hits.get(name)
        if not hits:
            continue
        chosen = None
        for (s, e) in hits:
            try:
                if _read_bit(e, _EGS_TEAM_OFF) == expected_team:
                    chosen = (s, e)
                    break
            except DecodeError:
                continue
        if chosen is None:
            chosen = hits[0]
        records.append((name, chosen[0], chosen[1]))
    records.sort(key=lambda r: r[1])   # by start bit == on-wire player order

    out = []
    for i, (name, start, end) in enumerate(records):
        next_start = records[i + 1][1] if i + 1 < len(records) else nbits
        # squad id: the UUID FString (if any) that falls inside this record
        party = None
        for (us, ue, text) in uuid_hits:
            if end <= us < next_start:
                party = text
                break
        entry = {"name": name, "team": team_by_name[name], "partyId": party,
                 "kills": None, "damage": None, "blocked": None, "assist": None,
                 "tankIdx": None, "tank": None, "unmapped": {}}
        try:
            entry["team"] = _read_bit(end, _EGS_TEAM_OFF)
            entry["damage"] = _read_int(end, _EGS_DMG_OFF, _EGS_STAT_BITS)
            entry["blocked"] = _read_int(end, _EGS_BLOCKED_OFF, _EGS_STAT_BITS)
            entry["assist"] = _read_int(end, _EGS_ASSIST_OFF, _EGS_STAT_BITS)
            entry["kills"] = _read_int(end, _EGS_KILLS_OFF, _EGS_STAT_BITS)
            entry["tankIdx"] = _read_int(end, _EGS_TANK_OFF, _EGS_TANK_BITS)
            entry["tank"] = _EGS_TANK_CODENAMES.get(entry["tankIdx"])
            entry["unmapped"] = {
                "field337": _read_int(end, _EGS_F337_OFF, _EGS_F337_BITS),
                "field345": _read_int(end, _EGS_F345_OFF, _EGS_F345_BITS),
            }
        except DecodeError:
            state.warn["endgame_record_truncated"] += 1
        out.append(entry)
    return out


# Kill/death timeline, survival, damage-by-who
#
# Three combat RPCs get captured raw into state.event_rpcs (see EVENT_RPC_NAMES
# and the ClassNetCache handle loop in process_bunch_payload):
#
#   TyrPlayerStateBase:NetMulticastBroadcastMessage fires once per player
#       death, on the victim's own BP_TyrPlayerState_C channel. Fire count
#       equals death count in every match I checked, so it's the death message
#       and not a general damage log - as far as I can tell no per-hit damage
#       broadcast exists on the wire at all. Its Instigator parameter points at
#       the killer's tank pawn, at a constant bit offset.
#
#   BP_BaseTank_C:Multicast_OnDeathEffects fires once per tank destroyed, on
#       the dying pawn's channel (different actor to the PlayerState above).
#       First parameter DeathLocation is an unquantized FVector, three LE
#       float64s, also at a constant offset.
#
#   TyrAmmunition:BroadcastBlockedMessage fires per blocked hit. Looked into
#       it, didn't ship it - decode_damage_events's docstring has the negative
#       result.
#
# Both offsets were found the same way as the end-game-stats fields: anchor on
# a value you already know from somewhere else, brute-force every bit offset
# looking for a decode that reproduces it, then check the same offset works on
# every other known case.
#
#   _DEATHMSG_INSTIGATOR_BIT: anchored on the killer's pawn guid from
#       BP_TyrPlayerState_C.KillerRef, which only covers about two thirds of
#       deaths. Bit 83 was the only offset that reproduced it, clean across 20
#       cases in two matches.
#   _DEATHFX_LOCATION_BIT: anchored on the dying tank's last known
#       ReplicatedMovement position. Only usable for deaths where the movement
#       track was still fresh - a tank that goes unspotted stops replicating
#       position, so stale tracks got excluded from the search rather than
#       counted as failures. Scanned LWC-quantized (scales 1 and 100), classic
#       packed vector, float32x3 and float64x3 at every offset; bit 25 with
#       float64x3 was the only thing that landed all the anchors within a few
#       tens of cm, which is about what you'd expect from the one frame gap
#       between the last movement sample and the actual death. Same coordinate
#       space as state.movement_samples.
_DEATHMSG_INSTIGATOR_BIT = 83
_DEATHFX_LOCATION_BIT = 25


def _resolve_pawn_owner(state, pawn_guid, ref_time):
    """Resolve a tank-pawn netguid to the PlayerState actor guid that owns it,
    via State.avatar_links (AbilitySystemComponent.AvatarActor -- see that
    field's docstring), picking whichever link record for this pawn_guid is
    CLOSEST in time to ref_time (avatar links are static for a pawn's whole
    lifetime in these single-life matches, so any record for the right guid
    resolves correctly; nearest-in-time is just defensive against a guid
    being reused by an unrelated later actor). Returns None if pawn_guid never
    appeared as an AvatarActor value (lossy stream, or not a player's tank)."""
    best, best_dt = None, None
    for t, carrier, avatar in state.avatar_links:
        if avatar != pawn_guid:
            continue
        dt = abs(t - ref_time)
        if best_dt is None or dt < best_dt:
            best_dt, best = dt, carrier
    return best


def derive_death_events(state):
    """Reconstruct the kill/death timeline (state.death_events).

    Death MOMENT: the earliest BP_TyrPlayerState_C.bIsAlive -> False update
    per victim (state.alive_flags) -- one per real death, PLAYER_IDENTITY_
    CLASSES-scoped so a channel-index reuse can't misattribute it.

    Killer: the NetMulticastBroadcastMessage sample captured on that same
    victim actor's channel (state.event_rpcs), Instigator field decoded at
    _DEATHMSG_INSTIGATOR_BIT and resolved pawn-guid -> PlayerState -> roster
    name via _resolve_pawn_owner + collect_player_roster. See the module
    comment above for the offset's derivation/validation. A victim with no
    captured NetMulticastBroadcastMessage sample (lossy stream) or whose
    Instigator resolves to guid 0 (environmental/self death -- observed once
    in TyrReplay4) gets killer=None; this is a real, expected outcome, not a
    decode failure -- see the module report's kills-vs-scoreboard reconciliation.

    Death location: the Multicast_OnDeathEffects sample on the victim's own
    tank-pawn channel (resolved via the most recent nonzero AvatarActor link
    at/before the death), DeathLocation decoded at _DEATHFX_LOCATION_BIT.
    None if that RPC wasn't captured for this death.

    Returns a time-sorted list of:
        {"t": float, "victim": str|None, "victimTeam": int|None,
         "victimTank": str|None, "killer": str|None, "killerTeam": int|None,
         "killerTank": str|None, "deathLocation": (x,y,z)|None}

    Defensive: a single record's killer/location resolution failure never
    drops the death itself or raises -- those fields just stay None.
    """
    roster = collect_player_roster(state)
    team_by_name = {p["name"]: p["team_id"] for p in roster}
    name_by_ps_guid = {}
    for rec in state.player_identity.values():
        if (rec.get("class") == "BP_TyrPlayerState_C" and rec.get("name")
                and rec.get("actorGuid") is not None):
            name_by_ps_guid[rec["actorGuid"]] = rec["name"]
    tank_by_name = {e["name"]: e["tank"] for e in state.endgame_stats if e.get("tank")}

    death_t_by_guid = {}
    for t, ch, guid, cls, alive in state.alive_flags:
        if cls != "BP_TyrPlayerState_C" or alive or guid is None:
            continue
        if guid not in death_t_by_guid or t < death_t_by_guid[guid]:
            death_t_by_guid[guid] = t

    deathmsg_by_guid = defaultdict(list)
    for t, ch, guid, cls, nb, payload in state.event_rpcs.get("NetMulticastBroadcastMessage", []):
        deathmsg_by_guid[guid].append((t, nb, payload))
    deathfx_by_guid = defaultdict(list)
    for t, ch, guid, cls, nb, payload in state.event_rpcs.get("Multicast_OnDeathEffects", []):
        deathfx_by_guid[guid].append((t, nb, payload))

    events = []
    for victim_guid, death_t in death_t_by_guid.items():
        victim_name = name_by_ps_guid.get(victim_guid)
        rec = {
            "t": death_t, "victim": victim_name,
            "victimTeam": team_by_name.get(victim_name),
            "victimTank": tank_by_name.get(victim_name),
            "killer": None, "killerTeam": None, "killerTank": None,
            "deathLocation": None,
        }

        # --- killer: nearest NetMulticastBroadcastMessage on this victim ---
        killer_pawn_guid = None
        best_dt = None
        for t, nb, payload in deathmsg_by_guid.get(victim_guid, []):
            if nb <= _DEATHMSG_INSTIGATOR_BIT:
                continue
            try:
                b = BitReader(payload, nb)
                b.pos = _DEATHMSG_INSTIGATOR_BIT
                v = b.int_packed()
            except DecodeError:
                continue
            dt = abs(t - death_t)
            if v and (best_dt is None or dt < best_dt):
                best_dt, killer_pawn_guid = dt, v
        if killer_pawn_guid:
            killer_ps_guid = _resolve_pawn_owner(state, killer_pawn_guid, death_t)
            killer_name = name_by_ps_guid.get(killer_ps_guid) if killer_ps_guid is not None else None
            if killer_name:
                rec["killer"] = killer_name
                rec["killerTeam"] = team_by_name.get(killer_name)
                rec["killerTank"] = tank_by_name.get(killer_name)

        # --- death location: victim's own tank pawn's Multicast_OnDeathEffects ---
        victim_pawn_guid, best_dt = None, None
        for t, carrier, avatar in state.avatar_links:
            if carrier == victim_guid and avatar and t <= death_t + 1.0:
                dt = death_t - t
                if best_dt is None or dt < best_dt:
                    best_dt, victim_pawn_guid = dt, avatar
        if victim_pawn_guid:
            best, best_dt2 = None, None
            for t, nb, payload in deathfx_by_guid.get(victim_pawn_guid, []):
                if nb <= _DEATHFX_LOCATION_BIT + 192:      # need 3 float64s
                    continue
                dt = abs(t - death_t)
                if best_dt2 is None or dt < best_dt2:
                    best_dt2, best = dt, (nb, payload)
            if best:
                nb, payload = best
                try:
                    b = BitReader(payload, nb)
                    b.pos = _DEATHFX_LOCATION_BIT
                    loc = (b.f64(), b.f64(), b.f64())
                    rec["deathLocation"] = tuple(round(x, 1) for x in loc)
                except DecodeError:
                    pass

        events.append(rec)
    events.sort(key=lambda r: r["t"])
    return events


def derive_survival(state, death_events, match_length=None):
    """Per-player survival time (state.survival), derived from
    derive_death_events(). Every roster player is included (survivors never
    appear in death_events, so they need collect_player_roster for coverage).

    Returns {name: {"team": int|None, "diedAt": float|None,
                     "survived": bool, "survivalSec": float}}.
    survivalSec = diedAt for players who died; for survivors it's
    match_length if given, else the last decoded frame time (state.time) --
    i.e. "how long they were confirmed alive for", not necessarily the exact
    official match-end timestamp.
    """
    roster = collect_player_roster(state)
    end_t = match_length if match_length is not None else state.time
    died_at = {e["victim"]: e["t"] for e in death_events if e["victim"]}
    out = {}
    for p in roster:
        name = p["name"]
        dt = died_at.get(name)
        out[name] = {
            "team": p["team_id"],
            "diedAt": round(dt, 1) if dt is not None else None,
            "survived": dt is None,
            "survivalSec": round(dt, 1) if dt is not None else round(end_t, 1),
        }
    return out


def decode_damage_events(state):
    """Per-hit damage-by-who (state.damage_events). CURRENTLY UNSHIPPED --
    always returns [] -- see below for the investigation and why.

    NetMulticastBroadcastMessage (the only PlayerStateBase broadcast RPC that
    fires more than a handful of times) turns out to be the DEATH message
    only -- see derive_death_events's module comment: its fire count equals
    the death count exactly in both test matches, never more. There is no
    other captured RPC that fires at general per-hit cadence except
    TyrAmmunition:BroadcastBlockedMessage (blocked/armor-absorbed hits only,
    ~15-20 firings per match -- a small fraction of total hits, but still a
    legitimate "who blocked whose shot" dataset if decodable).

    BroadcastBlockedMessage's Instigator field was NOT found: two independent
    anchor attempts, both negative --
      1. Brute-force int_packed() scan of every "clean" (503-bit) captured
         payload for ANY of the ~65 known tank-pawn guids (state.avatar_links)
         at any bit offset: zero hits in 10 of 12 samples, and the 2 hits that
         did occur (in different samples, at DIFFERENT offsets 55 and 469)
         did not agree with each other -- not a real signal.
      2. Cross-checked against the firing ammo actor's OWN already-reliable
         "Instigator" replicated PROPERTY (same InternalLoadObject decode as
         AvatarActor/KillerRef, captured independently of the RPC blob): the
         one sample where that property WAS captured (ammo guid 2804, shooter
         pawn guid 2810) produced zero matches for 2810 anywhere in that
         sample's BroadcastBlockedMessage payload at any bit offset.
    Also: a chunk of "BroadcastBlockedMessage"-named captures arrive with
    implausibly small sizes (0/5/10/32 bits, several at the exact identical
    timestamp on one channel) -- almost certainly ClassNetCache handle
    collisions between different ammo subclasses' RPC/custom-delta tables
    (see State._lookup_class's fuzzy-match docstring), not real
    BroadcastBlockedMessage firings at all. That noise was excluded from both
    scans above (only bits==503 samples were tried), but its existence is
    itself evidence this RPC's group resolution is not trustworthy enough to
    ship a field mapping from.

    Per this module's rule (do not ship a mapping that isn't validated
    against ground truth): no Magnitude/Location/Instigator decode is
    attempted for BroadcastBlockedMessage. Returns [] unconditionally.
    """
    return []


# driver

def load_chunks(path):
    """Reuse probe.py for the outer container."""
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from tyr_replay.probe import probe
    res = probe(path, verbose=False)
    return res


def decode(path, max_chunks=None, max_frames=None, progress=None):
    """progress, if given, is called as progress(done, total) after each
    ReplayData chunk of BOTH passes. Purely observational -- it exists so a
    caller can show a real progress bar over what is otherwise a ~10 second
    opaque wait. Never affects decoding."""
    res = load_chunks(path)
    data = Path(path).read_bytes()

    _n_rd = sum(1 for c in res["chunks"] if c["type"] == "ReplayData")
    if max_chunks is not None:
        _n_rd = min(_n_rd, max_chunks)
    _total = 2 * _n_rd + 1
    _done = [0]

    def _tick():
        _done[0] += 1
        if progress:
            try:
                progress(_done[0], _total)
            except Exception:
                pass          # a broken observer must never break a decode

    # Pass 1 (prescan): walk the same chunk/frame range extracting only
    # NetFieldExportGroups + NetGUID paths (packets are skipped, not
    # bit-parsed - this is just a byte-level walk, cheap). A class's
    # NetFieldExportGroup is exported the first time the stream actually
    # needs to replicate one of its properties, which can be well after
    # objects of that class were already opened/referenced earlier in the
    # file. State.group_for_guid's retry-on-miss cache (see State._lookup_class)
    # only fixes *future* look-ups once a group exists; it can't retroactively
    # fix bunches already processed in a single forward pass. Prescanning
    # first means the real pass below has the (near-)complete group table
    # from frame 0 onward. Confirmed empirically: this pass alone cut
    # no_group_for_repobject roughly in half on top of the lookup-algorithm
    # fix (see module change notes / decode report).
    prescan_state = State()
    n_prescan = 0
    for ch in res["chunks"]:
        if ch["type"] != "ReplayData":
            continue
        if max_chunks is not None and n_prescan >= max_chunks:
            break
        n_prescan += 1
        _tick()
        payload = data[ch["offset"] + 8: ch["offset"] + 8 + ch["size"]]
        try:
            if not parse_replay_data_chunk(payload, prescan_state, ch["index"],
                                           max_frames, prescan=True):
                break
        except DecodeError:
            break  # best-effort -- the real pass below is independent anyway

    state = State()
    state.groups_by_path = prescan_state.groups_by_path
    state.groups_by_class = prescan_state.groups_by_class
    state.groups_by_index = prescan_state.groups_by_index

    n_data_chunks = 0
    for ch in res["chunks"]:
        if ch["type"] != "ReplayData":
            continue
        if max_chunks is not None and n_data_chunks >= max_chunks:
            break
        n_data_chunks += 1
        _tick()
        payload = data[ch["offset"] + 8: ch["offset"] + 8 + ch["size"]]
        if not parse_replay_data_chunk(payload, state, ch["index"], max_frames):
            break
    _tick()
    # end-of-match scoreboard RPC (needs the completed roster + captured block)
    state.endgame_stats = decode_end_game_stats(state)
    # kill/death timeline + survival (needs endgame_stats for tank names) +
    # damage-by-who (see decode_damage_events - currently always [])
    state.death_events = derive_death_events(state)
    state.survival = derive_survival(state, state.death_events)
    state.damage_events = decode_damage_events(state)
    return res, state, n_data_chunks


def report(path, res, state, n_chunks, guid_limit=60, movement_limit=6):
    info = res["info"]
    header = res["header"] or {}
    print(f"file: {path}")
    print(f"  map={ [l['name'] for l in header.get('levels', [])] } "
          f"lengthMs={info.get('lengthInMs')} engine={header.get('engineVersion')}")
    print(f"  decoded: {n_chunks} ReplayData chunks -> {state.frames} frames, "
          f"{state.packets} packets, {state.bunches} bunches")
    print(f"  netguid table: {len(state.guid_paths)} entries; "
          f"netfield groups: {len(state.groups_by_path)}")
    print(f"  channels opened: {len(state.channel_opens)}, closed: {len(state.channel_closes)}")
    print(f"  external-data blobs: {sum(state.external_data.values())} "
          f"(guids: {len(state.external_data)})")

    if state.warn:
        print("\n=== warnings (count) ===")
        for k, v in state.warn.most_common():
            print(f"  {k}: {v}")
    if state.log_lines:
        print("\n=== first logged errors ===")
        for line in state.log_lines[:20]:
            print(f"  {line}")

    print(f"\n=== NetGUID export table (first {guid_limit} of {len(state.guid_paths)}) ===")
    for i, (guid, p) in enumerate(sorted(state.guid_paths.items())):
        if i >= guid_limit:
            break
        outer = state.guid_outer.get(guid)
        print(f"  {guid:>6} -> {p}" + (f"  (outer {outer})" if outer else ""))

    print("\n=== interesting guids (BP_/GA_/GE_/PS/GameState/Controller) ===")
    for guid, p in sorted(state.guid_paths.items()):
        k = class_key(p)
        if any(t in k for t in ("BP_", "GA_", "GE_", "PlayerState", "GameState",
                                "Controller", "PS_", "PC_")):
            print(f"  {guid:>6} -> {p}")

    print(f"\n=== NetFieldExportGroups ({len(state.groups_by_path)}) ===")
    for p, g in sorted(state.groups_by_path.items()):
        fields = ", ".join(g["fields"][h] for h in sorted(g["fields"])[:12])
        more = "" if len(g["fields"]) <= 12 else f", ... +{len(g['fields']) - 12}"
        print(f"  [{g['index']:>3}] {p} ({g['num']} slots): {fields}{more}")

    print(f"\n=== actor-channel opens ({len(state.channel_opens)}) ===")
    for o in state.channel_opens:
        loc = o["location"]
        loc_s = f" loc=({loc[0]:.1f},{loc[1]:.1f},{loc[2]:.1f})" if loc else ""
        arch = class_key(o["archetypePath"]) if o["archetypePath"] else o["actorPath"]
        print(f"  t={o['time']:>8.3f} ch={o['ch']:>3} guid={o['actorGuid']:>6} "
              f"{arch}{loc_s}")

    print(f"\n=== actor-channel closes ({len(state.channel_closes)}) ===")
    for c in state.channel_closes:
        print(f"  t={c['time']:>8.3f} ch={c['ch']:>3} guid={c['actorGuid']} "
              f"{c['class']} reason={c['reason']} (opened t={c['openedAt']})")

    print("\n=== replicated property updates (class, property) -> count ===")
    for (cls, prop), n in state.prop_counter.most_common(50):
        print(f"  {n:>6}  {cls}.{prop}")

    print("\n=== ClassNetCache fields seen (RPCs / custom-delta) ===")
    for (cls, name), n in state.rpc_counter.most_common(50):
        print(f"  {n:>6}  {cls}.{name}")

    if state.endgame_stats:
        print(f"\n=== end-game scoreboard "
              f"(Multicast_SendEndGameStats, {len(state.endgame_stats)} players) ===")
        print(f"  {'name':<22}{'tm':>3}{'tank':<13}{'kills':>6}{'dmg':>7}"
              f"{'block':>7}{'assist':>7}  partyId")
        for e in state.endgame_stats:
            print(f"  {e['name'][:21]:<22}{e['team']:>3}"
                  f"{(e.get('tank') or ''):<13}"
                  f"{('' if e['kills'] is None else e['kills']):>6}"
                  f"{('' if e['damage'] is None else e['damage']):>7}"
                  f"{('' if e.get('blocked') is None else e['blocked']):>7}"
                  f"{('' if e.get('assist') is None else e['assist']):>7}  "
                  f"{e['partyId'] or ''}")

    if state.death_events:
        print(f"\n=== kill/death timeline ({len(state.death_events)} deaths) ===")
        for d in state.death_events:
            loc = d["deathLocation"]
            loc_s = f" loc=({loc[0]:.0f},{loc[1]:.0f},{loc[2]:.0f})" if loc else ""
            print(f"  t={d['t']:>8.3f} {(d['victim'] or '?'):<22} (team {d['victimTeam']}) "
                  f"killed by {(d['killer'] or '?'):<22} (team {d['killerTeam']}){loc_s}")

    if state.survival:
        print(f"\n=== survival ({len(state.survival)} players) ===")
        for name, s in sorted(state.survival.items(), key=lambda kv: -kv[1]["survivalSec"]):
            status = "survived" if s["survived"] else f"died t={s['diedAt']}"
            print(f"  {name:<22} team={s['team']} aliveFor={s['survivalSec']:>7.1f}s ({status})")

    if state.string_props:
        print("\n=== decoded string properties ===")
        for (cls, prop), vals in state.string_props.items():
            print(f"  {cls}.{prop}: {sorted(vals)}")

    if state.attr_samples:
        health = [s for s in state.attr_samples if s[2] == "TyrAttributeSetHealth"]
        print(f"\n=== GAS attribute float samples ({len(state.attr_samples)} total, "
              f"{len(health)} health) ===")
        for t, chn, cls, handle, name, v in health[:10]:
            print(f"  t={t:>8.3f} ch={chn:>3} {cls}[{handle}].{name} = {v}")
        if len(health) > 10:
            print(f"  ... +{len(health) - 10} more health samples")

    if state.movement_samples:
        print("\n=== ReplicatedMovement samples (per actor, first+last few) ===")
        for ch_idx, samples in sorted(state.movement_samples.items()):
            apath = state.guid_paths.get(ch_idx)
            acls = class_key(apath) if apath else "?"
            print(f"  actor {ch_idx} ({acls}): {len(samples)} samples")
            shown = samples[:movement_limit // 2] + samples[-movement_limit // 2:] \
                if len(samples) > movement_limit else samples
            for t, loc, rot, vel, rot_short in shown:
                print(f"    t={t:>8.3f} loc=({loc[0]:.1f},{loc[1]:.1f},{loc[2]:.1f}) "
                      f"rot=({rot[0]:.0f},{rot[1]:.0f},{rot[2]:.0f}) "
                      f"vel=({vel[0]:.1f},{vel[1]:.1f},{vel[2]:.1f})")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("path")
    ap.add_argument("--max-chunks", type=int, default=None,
                    help="only decode the first N ReplayData chunks")
    ap.add_argument("--max-frames", type=int, default=None)
    ap.add_argument("--guids", type=int, default=60, help="guid table rows to print")
    ap.add_argument("--json", default=None, help="write full event dump to this file")
    ap.add_argument("--trace-ch", type=int, default=None,
                    help="print content-block parse trace for this channel index")
    args = ap.parse_args(argv)
    global TRACE_CHANNEL
    TRACE_CHANNEL = args.trace_ch

    res, state, n_chunks = decode(args.path, args.max_chunks, args.max_frames)
    report(args.path, res, state, n_chunks, guid_limit=args.guids)

    if args.json:
        out = {
            "file": str(args.path),
            "frames": state.frames, "packets": state.packets, "bunches": state.bunches,
            "guidPaths": {str(k): v for k, v in state.guid_paths.items()},
            "groups": {p: {"index": g["index"], "num": g["num"],
                           "fields": {str(h): n for h, n in g["fields"].items()}}
                       for p, g in state.groups_by_path.items()},
            "channelOpens": state.channel_opens,
            "channelCloses": state.channel_closes,
            "propUpdates": [{"class": c, "prop": p, "count": n}
                            for (c, p), n in state.prop_counter.most_common()],
            "classNetCacheFields": [{"class": c, "field": f, "count": n}
                                    for (c, f), n in state.rpc_counter.most_common()],
            "stringProps": [{"class": c, "prop": p, "values": sorted(v)}
                            for (c, p), v in state.string_props.items()],
            "playerRoster": collect_player_roster(state),
            "endGameStats": state.endgame_stats,
            "deathEvents": state.death_events,
            "survival": state.survival,
            "damageEvents": state.damage_events,
            "avatarLinks": [{"t": t, "carrierGuid": c, "avatarGuid": a}
                            for t, c, a in state.avatar_links],
            "movementSamples": {str(k): [{"t": t, "loc": loc, "rot": rot, "vel": vel}
                                         for t, loc, rot, vel, _ in v]
                                for k, v in state.movement_samples.items()},
            "attrSamples": [{"t": t, "ch": c, "class": cls, "handle": h,
                             "name": n, "value": v}
                            for t, c, cls, h, n, v in state.attr_samples],
            "warnings": dict(state.warn),
        }
        Path(args.json).write_text(json.dumps(out, indent=1), encoding="utf-8")
        print(f"\nwrote {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
