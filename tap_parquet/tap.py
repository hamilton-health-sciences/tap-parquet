"""Parquet tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk.typing import Property, PropertiesList, ArrayType, StringType

from tap_parquet.streams import ParquetStream
import os
import re


class TapParquet(Tap):
    """Parquet tap class."""

    name = "tap-parquet"

    config_jsonschema = PropertiesList(
        Property(
            "paths",
            ArrayType(StringType),
            required=True,
            description="Paths to Parquet Datasets",
        ),
    ).to_dict()

    def _sanitize_filename(self, path: str) -> str:
        """Return a sanitized version of the filename suitable for a db table name."""
        filename = os.path.splitext(os.path.basename(path))[0]
        sanitized_filename = re.sub(r'[^a-zA-Z0-9_]', '_', filename)
        return sanitized_filename


    def discover_streams(self) -> list[ParquetStream]:
        """Return a list of discovered streams."""
        return [
            ParquetStream(self, name=self._sanitize_filename(path), path=path)
            for path in self.config["paths"]
        ]


if __name__ == "__main__":
    TapParquet.cli()
