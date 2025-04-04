import json
from pathlib import Path

import yaml


def get_config_local(filename: Path) -> dict:
    with open(filename) as file:
        return yaml.safe_load(file)


def get_first_config() -> dict:
    files: list[Path] = [
        Path('/config/config.yaml'),
        Path('config.yaml'),
        Path('config.example.yaml')
    ]
    for file in files:
        if file.exists():
            loaded_config: dict = get_config_local(file)
            break
    else:
        raise FileNotFoundError
    options_files: list[Path] = [
        Path('/data/options.json'),
        Path('/data/options.yaml'),
    ]
    for options_file in options_files:
        if options_file.exists():
            if options_file.suffix == '.json':
                with open(options_file) as file:
                    options: dict = json.load(file)
            else:
                options: dict = get_config_local(options_file)
            for key, option in options.items():
                if isinstance(option, str) and option:
                    loaded_config[key] = option
                elif isinstance(option, int) or isinstance(option, bool):
                    loaded_config[key] = option
            break
    return loaded_config
