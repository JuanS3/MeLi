import pandas as pd
from typing import Any


def pdjson_normalize(*, df: pd.DataFrame, orient='records', sep='_') -> pd.DataFrame:
    """
    Normalize a dataframe with JSON-like columns into a dataframe with columns

    Parameters
    ----------
    df
        The dataframe to normalize.
    orient
        The orientation of the dataframe.
    sep
        The separator to use between columns in the dataframe.

    Returns
    -------
    pd.DataFrame
        A dataframe with the normalized columns.
    """
    df_norm: pd.DataFrame = pd.json_normalize(df.to_dict(orient=orient), sep=sep)
    return df_norm


def filter_by_day(*, df: pd.DataFrame, date_col: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Filter a dataframe by date.

    Parameters
    ----------
    df
        The dataframe to filter.
    date_col
        The column in the dataframe to filter by.
    start_date
        The start date to filter by.
    end_date
        The end date to filter by.

    Returns
    -------
    pd.DataFrame
        A dataframe with the filtered data.
    """
    df_filtered: pd.DataFrame = df[
        (df[date_col] >= start_date) &
        (df[date_col] <= end_date)
    ]
    return df_filtered


def get_max_data_column(*, df: pd.DataFrame, column: str) -> Any:
    """
    Get the maximum value from a column in a dataframe.

    Parameters
    ----------
    df
        The dataframe to get the column from.
    column
        The column to get.

    Returns
    -------
    pd.DataFrame
        A dataframe with the column.
    """
    max_value: Any = df[column].max()
    return max_value

