from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class Paths:
    root: Path
    raw: Path
    cache: Path
    processed: Path
    external: Path

def make_paths(root: Path) -> Paths:
    data = root / "data"
    paths = Paths(
        root=root,
        raw=data / "raw",
        cache=data / "cache",
        processed=data / "processed",
        external=data / "external",
    )
    # التأكد من وجود المجلدات فعلياً على القرص
    for path in [paths.raw, paths.cache, paths.processed, paths.external]:
     path.mkdir(parents=True, exist_ok=True)
    return paths


ROOT_DIR = Path(__file__).resolve().parents[2]
PROJ_PATHS = make_paths(ROOT_DIR)