"""Read Tyr (Unreal Engine 5.6) .replay files.

    from tyr_replay import decode
    state = decode("TyrReplay1.replay")

`decode` walks the replay's net stream and returns the match: players,
positions, kills, health over time and the end of game scoreboard. `probe`
is the lighter half, reading just the header and chunk table.

No dependencies, standard library only.
"""
from tyr_replay.decode import decode
from tyr_replay.probe import probe, parse_replay_info, parse_header_chunk

__all__ = ["decode", "probe", "parse_replay_info", "parse_header_chunk"]
__version__ = "0.1.0"
