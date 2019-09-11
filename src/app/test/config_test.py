# -*- coding: utf-8 -*-
import os
from pathlib import Path

import pytest
import yapconf
from box import Box
from mock import Mock, patch
from ruamel import yaml
from yapconf import YapconfSpec

import beer_garden.config
from beer_garden.log import default_app_config


class TestLoadConfig(object):
    def test_no_config_file(self):
        beer_garden.config.load([], force=True)
        spec = YapconfSpec(beer_garden.config._SPECIFICATION)
        assert beer_garden.config._CONFIG == spec.defaults

    @pytest.mark.parametrize(
        "config",
        [
            # (file extension, file type, file contents)
            ("yaml", "yaml", "log_level: DEBUG"),
            ("json", "json", '{"log_level": "DEBUG"}'),
            ("", "yaml", "log_level: DEBUG"),
            ("yaml", None, "log_level: DEBUG"),
            ("json", None, '{"log_level": "DEBUG"}'),
            ("", None, "log_level: DEBUG"),
        ],
    )
    def test_config_file(self, tmpdir, config):
        config_file = Path(tmpdir, "config." + config[0])

        cli_args = ["--configuration-file", str(config_file)]
        if config[1]:
            cli_args += ["--configuration-type", config[1]]

        with open(config_file, "w") as f:
            f.write(config[2])

        beer_garden.config.load(cli_args, force=True)
        assert beer_garden.config.get("log.level") == "DEBUG"


class TestGenerateConfig(object):
    def test_correctness(self, tmpdir):
        config_file = os.path.join(str(tmpdir), "config.yaml")
        logging_config_file = os.path.join(str(tmpdir), "logging.json")

        beer_garden.config.generate(["-c", config_file, "-l", logging_config_file])

        spec = YapconfSpec(beer_garden.config._SPECIFICATION)
        spec.add_source(label="config_file", source_type="yaml", filename=config_file)
        config = spec.load_config("config_file")

        # Defaults from spec
        assert config.log.file is None
        assert config.log.level == "INFO"

        # Value passed in
        assert config.log.config_file == logging_config_file

        # Ensure that bootstrap items were not written to file
        assert config.configuration.file is None
        assert config.configuration.type is None

        with open(config_file) as f:
            yaml_config = yaml.safe_load(f)
        assert "configuration" not in yaml_config

    @pytest.mark.parametrize("file_type", ["json", "yaml"])
    def test_create_file(self, tmpdir, file_type):
        filename = os.path.join(str(tmpdir), "temp." + file_type)
        beer_garden.config.generate(["-c", filename, "-t", file_type])

        assert os.path.getsize(filename) > 0

    @pytest.mark.parametrize("file_type", ["json", "yaml"])
    def test_file_infer_type(self, tmpdir, file_type):
        filename = os.path.join(str(tmpdir), "temp." + file_type)
        beer_garden.config.generate(["-c", filename])

        assert os.path.getsize(filename) > 0

    @pytest.mark.parametrize("file_type", ["json", "yaml"])
    def test_stdout(self, capsys, file_type):
        beer_garden.config.generate(["-t", file_type])

        # Just make sure we printed something
        assert capsys.readouterr().out

    def test_omit_bootstrap(self, tmpdir):
        filename = os.path.join(str(tmpdir), "temp.yaml")
        beer_garden.config.generate(["-c", filename])

        with open(filename) as f:
            config = yaml.safe_load(f)

        assert "log" in config
        assert "configuration" not in config


class TestUpdateConfig(object):
    @pytest.mark.parametrize(
        "extension,file_type", [("json", "json"), ("yaml", "yaml"), ("yml", "yaml")]
    )
    def test_success(self, tmpdir, extension, file_type):
        config_file = os.path.join(str(tmpdir), "config." + extension)

        beer_garden.config.generate(["-c", config_file])
        beer_garden.config.migrate(["-c", config_file])

        assert os.path.exists(config_file)

    @pytest.mark.parametrize(
        "current_type,new_type", [("json", "yaml"), ("yaml", "json")]
    )
    def test_change_type(self, tmpdir, current_type, new_type):
        current_config = os.path.join(str(tmpdir), "config." + current_type)
        new_config = os.path.join(str(tmpdir), "config." + new_type)

        beer_garden.config.generate(["-c", current_config])
        beer_garden.config.migrate(["-c", current_config, "-t", new_type])

        assert os.path.exists(new_config)
        assert not os.path.exists(current_config)

    def test_change_type_error(self, monkeypatch, tmpdir):
        config_file = os.path.join(str(tmpdir), "config.json")
        beer_garden.config.generate(["-c", config_file])

        error_mock = Mock(side_effect=ValueError)
        monkeypatch.setattr(yapconf.spec.YapconfSpec, "migrate_config_file", error_mock)

        with pytest.raises(ValueError):
            beer_garden.config.migrate(["-c", config_file, "-t", "yaml"])

        assert os.path.exists(config_file)

    def test_no_file_specified(self):
        with pytest.raises(SystemExit):
            beer_garden.config.migrate([])


class TestGenerateLogging(object):
    def test_no_file(self):
        logging_config = beer_garden.config.generate_logging([])

        assert logging_config == default_app_config("INFO", None)

    def test_with_file(self, tmpdir):
        logging_config_file = Path(tmpdir, "logging.json")

        logging_config = beer_garden.config.generate_logging(
            ["--log-config-file", str(logging_config_file)]
        )

        assert Path.exists(logging_config_file)
        assert logging_config == default_app_config("INFO", None)


class TestConfigGet(object):
    def test_gets(self):
        beer_garden.config.load([], force=True)
        amq = beer_garden.config.get("amq")
        assert amq.host == "localhost"
        assert amq["host"] == "localhost"
        assert beer_garden.config.get("publish_hostname") == "localhost"
        assert beer_garden.config.get("amq.host") == "localhost"
        assert beer_garden.config.get("INVALID_KEY") is None
        assert beer_garden.config.get("") is None


class TestSafeMigrate(object):
    @pytest.fixture
    def spec(self, monkeypatch):
        # return YapconfSpec(
        raw_spec = {
            "log": {
                "type": "dict",
                "items": {
                    "config_file": {
                        "type": "str",
                        "description": "Path to a logging config file.",
                        "required": False,
                        "cli_short_name": "l",
                        "previous_names": ["log_config"],
                        "alt_env_names": ["LOG_CONFIG"],
                    },
                    "file": {
                        "type": "str",
                        "description": "File you would like the application to log to",
                        "required": False,
                        "previous_names": ["log_file"],
                    },
                    "level": {
                        "type": "str",
                        "description": "Log level for the application",
                        "default": "INFO",
                        "choices": [
                            "DEBUG",
                            "INFO",
                            "WARN",
                            "WARNING",
                            "ERROR",
                            "CRITICAL",
                        ],
                        "previous_names": ["log_level"],
                    },
                },
            },
            "configuration": {
                "type": "dict",
                "bootstrap": True,
                "items": {
                    "file": {
                        "required": False,
                        "bootstrap": True,
                        "cli_short_name": "c",
                    },
                    "type": {
                        "required": False,
                        "bootstrap": True,
                        "cli_short_name": "t",
                    },
                },
            },
        }
        # )

        monkeypatch.setattr(beer_garden.config, "_SPECIFICATION", raw_spec)

        return raw_spec

    @pytest.fixture
    def old_config(self):
        """Represent an un-migrated config with previous default values."""
        return {
            "log_config": None,
            "log_file": None,
            "log_level": "INFO",
            "configuration": {"type": "json"},
        }

    @pytest.fixture
    def new_config(self):
        """Represents a up-to-date config with all new values."""
        return {"log": {"config_file": None, "file": None, "level": "INFO"}}

    @pytest.fixture
    def config_file(self, tmpdir):
        return Path(tmpdir, "config.yaml")

    def test_success(self, tmpdir, spec, config_file, old_config, new_config):
        old_config["configuration"]["file"] = str(config_file)

        with open(config_file, "w") as f:
            yaml.safe_dump(
                old_config, stream=f, default_flow_style=False, encoding="utf-8"
            )

        beer_garden.config.load(["-c", str(config_file)], force=True)
        assert beer_garden.config.get("log.level") == "INFO"

        with open(config_file) as f:
            new_config_value = yaml.safe_load(f)

        assert new_config_value == new_config
        assert len(os.listdir(tmpdir)) == 2

    def test_no_change(self, tmpdir, spec, config_file, new_config):
        with open(config_file, "w") as f:
            yaml.safe_dump(
                new_config, stream=f, default_flow_style=False, encoding="utf-8"
            )

        beer_garden.config.load(["-c", str(config_file)], force=True)
        assert beer_garden.config.get("log.level") == "INFO"

        with open(config_file) as f:
            new_config_value = yaml.safe_load(f)

        assert new_config_value == new_config
        assert len(os.listdir(tmpdir)) == 1

    def test_migration_failure(
        self, monkeypatch, capsys, tmpdir, spec, config_file, old_config
    ):
        monkeypatch.setattr(
            beer_garden.config.YapconfSpec,
            "migrate_config_file",
            Mock(side_effect=ValueError),
        )

        with open(config_file, "w") as f:
            yaml.safe_dump(
                old_config, stream=f, default_flow_style=False, encoding="utf-8"
            )

        beer_garden.config.load(["-c", str(config_file)], force=True)

        # Make sure we printed something
        assert capsys.readouterr().err

        # If the migration fails, we should still have a single unchanged JSON file.
        assert len(os.listdir(tmpdir)) == 1
        with open(config_file) as f:
            new_config_value = yaml.safe_load(f)
        assert new_config_value == old_config

        # And the values should be unchanged
        assert beer_garden.config.get("log.level") == "INFO"

    def test_rename_failure(self, capsys, tmpdir, spec, config_file, old_config):
        with open(config_file, "w") as f:
            yaml.safe_dump(
                old_config, stream=f, default_flow_style=False, encoding="utf-8"
            )

        with patch("os.rename", Mock(side_effect=ValueError)):
            beer_garden.config.load(["-c", str(config_file)], force=True)

        # Make sure we printed something
        assert capsys.readouterr().err

        assert beer_garden.config.get("log.level") == "INFO"

        # Both the tmp file and the old JSON should still be there.
        assert len(os.listdir(tmpdir)) == 2
        assert os.path.exists(str(config_file) + ".tmp")

        # The loaded config should be the original file.
        with open(config_file) as f:
            new_config_value = yaml.safe_load(f)

        assert new_config_value == old_config

    def test_catastrophe(self, capsys, tmpdir, spec, config_file, old_config):
        with open(config_file, "w") as f:
            yaml.safe_dump(
                old_config, stream=f, default_flow_style=False, encoding="utf-8"
            )

        with patch("os.rename", Mock(side_effect=[Mock(), ValueError])):
            with pytest.raises(ValueError):
                beer_garden.config.load(["-c", str(config_file)], force=True)

        # Make sure we printed something
        assert capsys.readouterr().err

        # Both the tmp file and the old JSON should still be there.
        assert len(os.listdir(tmpdir)) == 2


@pytest.mark.parametrize(
    "file_name,file_type,expected_type",
    [
        (None, None, "yaml"),
        (None, "yaml", "yaml"),
        (None, "json", "json"),
        ("file", None, "yaml"),
        ("file", "yaml", "yaml"),
        ("file", "json", "json"),
        ("file.yaml", None, "yaml"),
        ("file.blah", None, "yaml"),
        ("file.json", None, "json"),
        ("file.yaml", "yaml", "yaml"),
        ("file.yaml", "json", "json"),
        ("file.json", "yaml", "yaml"),
        ("file.json", "json", "json"),
    ],
)
def test_get_config_type(file_name, file_type, expected_type):
    config = Box({"configuration": {"file": file_name, "type": file_type}})
    assert beer_garden.config._get_config_type(config) == expected_type


def test_parse_args():
    input_args = [
        "--log-config-file",
        "/path/to/log/config",
        "--log-file",
        "/path/to/log/file",
        "--log-level",
        "INFO",
    ]

    _, cli_vars = beer_garden.config._parse_args(input_args)

    assert cli_vars["log"] == {
        "config_file": "/path/to/log/config",
        "file": "/path/to/log/file",
        "level": "INFO",
    }
