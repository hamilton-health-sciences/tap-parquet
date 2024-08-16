"""Stream class for tap-parquet."""

from __future__ import annotations

from os import PathLike
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable

from singer_sdk import Tap
from singer_sdk._singerlib.schema import Schema
from singer_sdk.streams import Stream
from singer_sdk.typing import (
    BooleanType,
    DateTimeType,
    IntegerType,
    NumberType,
    PropertiesList,
    Property,
    StringType,
    JSONTypeHelper,
)

import pyarrow.parquet as pq
import json

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


def get_jsonschema_type(ansi_type: str) -> JSONTypeHelper:
    """Return a JSONTypeHelper object for the given type name."""
    if "int" in ansi_type:
        return IntegerType()
    if "double" in ansi_type:
        return NumberType()
    if "string" in ansi_type:
        return StringType()
    if "bool" in ansi_type:
        return BooleanType()
    if "timestamp[ns]" in ansi_type:
        return DateTimeType()
    if "timestamp[us]" in ansi_type:
        return DateTimeType()
    raise ValueError(f"Unmappable data type '{ansi_type}'.")


class ParquetStream(Stream):
    """Stream class for Parquet streams."""

    def __init__(self, tap: Tap, path: str | PathLike, schema: str | PathLike | dict[str, Any] | Schema | None = None, name: str | None = None) -> None:
        """Initialize the stream.

        Args:
            tap: The tap for this stream.
            schema: The schema for this stream.
            name: The name of the stream.
            path: The path to the parquet dataset.
        """
        self.path = path

        try:
            self.dataset = pq.ParquetDataset(self.path)
        except Exception as e:
            raise IOError(f"Error reading Parquet dataset at '{self.path}': {e}")

        if not schema:
            schema = self.discover_schema()

        super().__init__(tap, schema, name)
        
        
    def discover_schema(self) -> dict:
        """Dynamically detect the json schema for the stream.

        This is evaluated prior to any records being retrieved.
        """
        properties: list[Property] = []
        
        parquet_schema = self.dataset.schema
        for i in range(len(parquet_schema.names)):
            name, dtype = parquet_schema.names[i], parquet_schema.types[i]
            properties.append(Property(name, get_jsonschema_type(str(dtype))))
                
        return PropertiesList(*properties).to_dict()


    def get_records(
        self,
        context: Context | None,  # noqa: ARG002
    ) -> Iterable[dict]:
        """Return a generator of record-type dictionary objects."""
        
        table = self.dataset.read()

        for batch in table.to_batches():
            for row in zip(*batch.columns):
                yield {
                    table.column_names[i]: val.as_py()
                    for i, val in enumerate(row, start=0)
                }
