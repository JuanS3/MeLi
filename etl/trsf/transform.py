import pandas as pd


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

