# tyr-replay-tools

Read [Tyr](https://store.steampowered.com/app/2862420/) `.replay` files in
Python. Stdlib only, no dependencies, and the game doesn't need to be running.

Tyr dumps a full Unreal Engine replay for every match into
`%LOCALAPPDATA%\Tyr\Saved\Demos` and keeps roughly ten before it starts
overwriting. There's a lot more in those files than the post-match screen shows
you: every player's position over time, the kill timeline, team health, the
end of game scoreboard. This decodes them.

```python
from tyr_replay import decode

info, state, frames = decode("TyrReplay1.replay")

for p in state.endgame_stats:
    print(p["name"], p["tank"], p["kills"], p["damage"])
```

```
python examples/dump_match.py TyrReplay1.replay
```

## What you get

`decode()` returns `(info, state, frames)`.

`info` is the header: engine version, game build (`changelist`), match GUID,
level name, chunk table.

`state` is the decoded match. The useful parts:

| attribute | what it is |
|---|---|
| `endgame_stats` | the scoreboard: name, tank, team, kills, damage, assist, blocked |
| `death_events` | kill timeline with timestamps, killer and victim |
| `movement_samples` | per player position tracks through the match |
| `team_info` | each team's shared health pool over time |
| `survival` | how long each player lasted |
| `zone_events` | capture zone activity |
| `attr_samples` | raw gameplay attribute replication, including per player health |
| `channel_opens` | actor spawns with quantised location and rotation |

`frames` is the number of demo frames walked.

## Things that'll save you time

**Positions are raw Unreal world coordinates in centimetres.** Nothing is
relative to the map, so the numbers look arbitrary and vary wildly from one map
to the next. To put them on a minimap you need that map's capture centre and
the world distance the image covers:

```
pixelX = (0.5 + (worldX - centreX) / worldSize) * imageWidth
pixelY = (0.5 + (worldY - centreY) / worldSize) * imageHeight
```

World +X is right and world +Y is **down**. Easiest way to get `centreX` and
`centreY` is the `NavMeshBoundsVolume` in the map's package. For `worldSize`,
line up a spawn - take a player's position at the very start of the match and
match it against the spawn marker on the minimap image. Don't bother fitting
against scattered map geometry, it doesn't converge. I wasted a while on that.

**Tank names in `endgame_stats` are internal codenames**, not what players see.
`Bush` is Ark, `Healer` is Valor, and so on. You'll have to build your own
mapping.

**Not every replay is a complete match.** Copy a file while the game's still
writing it and you'll get a partial decode, or nothing at all. If you're
archiving replays yourself, wait for the file to stop changing first - the
game's own "is live" header flag lies, and reads live on most finished files.

**`GameplayTag` numeric indices are build specific.** They get renumbered any
time tags are added, so `Gameplay.Mode.Standard` is 6247 on build 31351 and
5551 on 29906. Match tags by name or your code breaks every patch.

## Format notes

[`docs/replay-format.md`](docs/replay-format.md) covers the container layout,
the chunk types, and how the net stream gets walked: frames to packets, packets
to bunches, then the NetGUID export table and property replication.

## Credit

The net stream reader ports the minimum it needed from
[Shiqan/FortniteReplayDecompressor](https://github.com/Shiqan/FortniteReplayDecompressor)'s
`Unreal.Core`, MIT licensed. Would've taken a lot longer without it to
reference.

## Licence

MIT, see [LICENSE](LICENSE).

## Uploading replays

If you just want your matches to show up on
[TYR.pages](https://tyrpages.legomaster188.workers.dev) rather than decode
anything yourself, that's a separate tool:
[tyr-uploader](https://github.com/legomaster188/tyr-uploader). It watches your
replay folder and sends new ones up. One file, stdlib only, not a mod.

## Scope

This is the replay reading half of a bigger stats project, split out on its own
because a couple of people asked how to do it. It isn't a general Unreal replay
library - it targets Tyr on UE 5.6 specifically and a patch can move things.
PRs welcome, especially replays from builds other than 31351, which I don't
have any of.
