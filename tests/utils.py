from pathlib import Path
from shutil import copy

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
                    "type": "text.TextDataset",
                },
            },
            f,
        )

    copy(
        Path(__file__).absolute().parent / "conf" / "base" / "azureml.yml",
        config_path / "azureml.yml",
    )
    copy(
        Path(__file__).absolute().parent / "conf" / "base" / "parameters.yml",
        config_path / "parameters.yml",
    )

    return config_path
