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


def filter_by_values(*, df: pd.DataFrame, column: str, values: Any) -> pd.DataFrame:
    """
    Filter a dataframe by a column and value.

    Parameters
    ----------
    df
        The dataframe to filter.
    column
        The column in the dataframe to filter by.
    values
        The values to filter by.

    Returns
    -------
    pd.DataFrame
        A dataframe with the filtered data.
    """
    df_filtered: pd.DataFrame = df[df[column].isin(values)]
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


def get_column(*, df: pd.DataFrame, column: str) -> pd.Series:
    """
    Get a column from a dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to get the column from.
    column : str
        The column to get.

    Returns
    -------
    pd.DataFrame
        A dataframe with the column.
    """
    column_data: pd.Series = df[column]
    return column_data


def group_by(*, df: pd.DataFrame, by: list[str], operation: str) -> pd.DataFrame:
    """
    Group a dataframe by columns.

    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to group.
    by : list[str]
        The columns to group by.
    operation : str
        The operation to perform on the grouped data, one of
            - sum
            - count
            - mean

    Returns
    -------
    pd.DataFrame
        A dataframe with the grouped data.
    """
    df_grouped: Any = df.groupby(by=by)
    if operation == 'sum':
        df_grouped: Any = df_grouped.sum()
    elif operation == 'count':
        df_grouped: Any = df_grouped.count()
    elif operation == 'mean':
        df_grouped: Any = df_grouped.mean()

    return df_grouped
