import polars as pl


def fixture_column(name: str, values: list, dtype: pl.DataType) -> pl.Series:
    series = pl.Series(name, values, strict=False)
    if series.dtype == pl.String and dtype != pl.String:
        if dtype == pl.Boolean:
            series = series.str.to_lowercase().replace_strict(
                {"true": True, "false": False}, return_dtype=pl.Boolean
            )
        elif dtype == pl.Date:
            series = series.str.to_date()
        elif isinstance(dtype, pl.Datetime):
            series = series.str.to_datetime(
                time_unit=dtype.time_unit, time_zone=dtype.time_zone
            )
        elif dtype == pl.Time:
            series = series.str.to_time()
    return series.cast(dtype)


def fixture_frame(rows: list[dict], schema: dict[str, pl.DataType]) -> pl.DataFrame:
    """Unit test fixture rows cast to `schema`. Row keys match columns
    case-insensitively and columns missing from a row are null."""
    column_by_lower_name = {name.lower(): name for name in schema}
    values: dict[str, list] = {name: [None] * len(rows) for name in schema}
    for index, row in enumerate(rows):
        for key, value in row.items():
            values[column_by_lower_name[key.lower()]][index] = value
    return pl.DataFrame(
        [fixture_column(name, values[name], dtype) for name, dtype in schema.items()]
    )
