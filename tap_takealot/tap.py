"""Takealot tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_takealot import streams


class TapTakealot(Tap):
    """Takealot tap class."""

    name = "tap-takealot"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The API key to authenticate against the API service",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property(
            "end_date",
            th.DateTimeType,
            description="The latest record date to sync",
        ),
        th.Property(
            "api_url",
            th.StringType,
            default="https://seller-api.takealot.com",
            description="The url for the API service",
        ),
        th.Property(
            "page_size",
            th.IntegerType,
            default=100,
            description="Page size of each response",
        )
    ).to_dict()

    def discover_streams(self) -> list[streams.TakealotStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.SalesStream(self)
        ]


if __name__ == "__main__":
    TapTakealot.cli()
