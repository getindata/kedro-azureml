from pathlib import Path

import yaml


def identity(x):
    return x


def create_kedro_conf_dirs(tmp_path: Path):
    config_path: Path = tmp_path / "conf" / "base"
    config_path.mkdir(parents=True)

    dummy_data_path = tmp_path / "file.txt"
    dummy_data_path.write_text(":)")

    with (config_path / "catalog.yml").open("wt") as f:
        yaml.safe_dump(
            {
                "input_data": {
                    "filepath": str(dummy_data_path.absolute()),
                    "type": "text.TextDataSet",
                },
            },
            f,
        )
    return config_path
