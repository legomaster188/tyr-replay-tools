"""Print what one replay contains.

    python examples/dump_match.py path/to/TyrReplay1.replay

Shows the header, the roster with the end of game scoreboard, the kill
timeline, and the team health pools over the match.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tyr_replay import decode


def main(argv=None):
    argv = argv if argv is not None else sys.argv[1:]
    if not argv:
        print(__doc__)
        return 1

    info, state, frames = decode(argv[0])

    hdr = info.get("header") or {}
    print(f"file      {Path(argv[0]).name}")
    print(f"engine    {hdr.get('engineVersion')}  build {(info.get('info') or {}).get('changelist')}")
    print(f"guid      {hdr.get('guid')}")
    levels = [lv.get("name") for lv in (hdr.get("levels") or [])]
    print(f"map       {levels[0] if levels else 'unknown'}")
    print(f"length    {state.time:.0f}s over {frames} frames")

    # Scoreboard, handed over as one struct at match end.
    if state.endgame_stats:
        print(f"\nscoreboard ({len(state.endgame_stats)} players)")
        print(f"  {'player':<22}{'tank':<14}{'K':>4}{'dmg':>8}{'asst':>7}{'blkd':>7}")
        for p in state.endgame_stats:
            print(f"  {str(p.get('name'))[:21]:<22}{str(p.get('tank') or '')[:13]:<14}"
                  f"{p.get('kills', 0):>4}{p.get('damage', 0):>8}"
                  f"{p.get('assist', 0):>7}{p.get('blocked', 0):>7}")
    else:
        print("\nno end of game stats in this replay "
              "(the match probably did not run to the end)")

    if state.death_events:
        print(f"\nkills ({len(state.death_events)})")
        for e in state.death_events:
            t = e.get("t") or 0
            print(f"  {int(t)//60}:{int(t)%60:02d}  {e.get('killer') or 'unknown'}"
                  f"  ->  {e.get('victim')}")

    # Team health is the shared pool each side loses as its tanks take damage.
    if state.team_info:
        print("\nteam health")
        for guid, rec in state.team_info.items():
            series = rec.get("health") or []
            if not series:
                continue
            print(f"  team {rec.get('teamId')}: {series[0][1]} -> {series[-1][1]}"
                  f"  ({len(series)} samples)")

    print(f"\nalso available on the returned state: {len(state.movement_samples)} "
          f"players' position tracks, {len(state.attr_samples)} attribute samples, "
          f"{len(state.zone_events)} zone events")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
