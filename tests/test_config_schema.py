import json
import tomllib
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from jsonschema.protocols import Validator

from core import config

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def schema_validator() -> Validator:
    schema = json.loads(
        (PROJECT_ROOT / "config.schema.json").read_text(encoding="utf-8")
    )
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def test_example_config_matches_schema(schema_validator: Validator) -> None:
    path = PROJECT_ROOT / "config.example.toml"
    example = path.read_text(encoding="utf-8")

    assert isinstance(schema_validator.schema, dict)
    assert example.splitlines()[0] == f"#:schema {schema_validator.schema['$id']}"
    schema_validator.validate(tomllib.loads(example))
    assert config.load_youtube_configs(path)


@pytest.mark.parametrize(
    ("field", "value", "valid"),
    [
        ("name", " primary ", True),
        ("name", "\tprimary\r\n", True),
        ("name", " PRIMARY ", False),
        ("name", " pri mary ", False),
        ("name", " \t\n", False),
        ("reference", " @ExampleHandle ", True),
        ("reference", "\t@ExampleHandle\r\n", True),
        ("reference", " UCaaaaaaaaaaaaaaaaaaaaaa ", True),
        ("reference", " https://youtube.com/@ExampleHandle ", True),
        ("reference", "https://YouTube.com/@ExampleHandle", True),
        ("reference", " HTTPS://WWW.YOUTUBE.COM/@ExampleHandle ", True),
        (
            "reference",
            " HtTpS://WwW.YouTube.CoM/channel/UCaaaaaaaaaaaaaaaaaaaaaa ",
            True,
        ),
        ("reference", " https://YouTube.com/c/Example ", True),
        ("reference", " https://YouTube.com/user/Example ", True),
        ("reference", " ucaaaaaaaaaaaaaaaaaaaaaa ", False),
        ("reference", " @ab ", False),
        ("reference", " \t\n", False),
        ("reference", " http://YouTube.com/@ExampleHandle ", False),
        ("reference", " https://YouTube.com/Channel/UCaaaaaaaaaaaaaaaaaaaaaa ", False),
        ("reference", " https://YouTube.com.evil.test/@ExampleHandle ", False),
        ("reference", " https://user@YouTube.com/@ExampleHandle ", False),
        ("discord_webhook_urls", " https://discord.com/api/webhooks/123/token ", True),
        (
            "discord_webhook_urls",
            "\thttps://discord.com/api/webhooks/123/token\r\n",
            True,
        ),
        ("discord_webhook_urls", "https://Discord.com/api/webhooks/123/token", True),
        (
            "discord_webhook_urls",
            " HTTPS://WWW.DISCORD.COM/api/webhooks/123/token ",
            True,
        ),
        (
            "discord_webhook_urls",
            " HtTpS://WwW.Discord.CoM/api/webhooks/123/token ",
            True,
        ),
        ("discord_webhook_urls", " http://Discord.com/api/webhooks/123/token ", False),
        ("discord_webhook_urls", " https://Discord.com/API/webhooks/123/token ", False),
        (
            "discord_webhook_urls",
            " https://Discord.com.evil.test/api/webhooks/123/token ",
            False,
        ),
        (
            "discord_webhook_urls",
            " https://user@Discord.com/api/webhooks/123/token ",
            False,
        ),
        (
            "discord_webhook_urls",
            " https://Discord.com:443/api/webhooks/123/token ",
            False,
        ),
        ("discord_webhook_urls", " https://Discord.com/api/webhooks/abc/token ", False),
        (
            "discord_webhook_urls",
            " https://Discord.com/api/webhooks/123/token?x=1 ",
            False,
        ),
        (
            "discord_webhook_urls",
            " https://Discord.com/api/webhooks/123/token#x ",
            False,
        ),
        ("discord_webhook_urls", "https://discord.com/api/webhooks/123/ ", False),
        ("discord_webhook_urls", " \t\n", False),
    ],
)
def test_schema_string_validation_matches_loader(
    schema_validator: Validator, field: str, value: str, valid: bool
) -> None:
    channel = {
        "reference": "@valid",
        "discord_webhook_urls": ["https://discord.com/api/webhooks/123/token"],
    }
    instance = {"name": "primary", "channels": [channel]}
    if field == "name":
        instance[field] = value
    else:
        channel[field] = [value] if field == "discord_webhook_urls" else value

    if valid:
        parsed = config._parse_youtube_config(instance, 0)
        normalized = {
            "name": parsed.name,
            "reference": parsed.channels[0].reference,
            "discord_webhook_urls": parsed.channels[0].discord_webhook_urls[0],
        }
        assert normalized[field] == value.strip()
    else:
        with pytest.raises(ValueError):
            config._parse_youtube_config(instance, 0)

    assert schema_validator.is_valid({"instances": {"youtube": [instance]}}) is valid
