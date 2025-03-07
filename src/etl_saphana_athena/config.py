from pathlib import Path
import json

HOME = Path.home()
FILE = HOME.joinpath(".export.json")


def create_config(config: dict):
    with FILE.open("w", encoding="utf_8") as f:
        json.dump(config, f, indent=4)


def load_config() -> dict:
    if FILE.exists():
        with FILE.open("r", encoding="utf_8") as f:
            return json.load(f)

    return dict()
