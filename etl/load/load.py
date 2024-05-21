import pandas as pd
from etl.utils import decorators as dec


@dec.time_it
def dataframe_to_parquet(
        *,
        df: pd.DataFrame,
        path: str = 'data/staging/dataframe.parquet.gzip',
        compression: str = 'gzip',
    ) -> None:
    """
    Save a pandas DataFrame to Parquet format.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to save.
    path : str | Optional
        The path where the DataFrame should be saved, by default 'data/staging/dataframe.parquet.gzip'
    compression : str | Optional
        The compression mode to use for the Parquet file, by default 'gzip'
    """
    df.to_parquet(path, compression=compression, engine='pyarrow')


@dec.time_it
def dataframe_to_csv(
        *,
        df: pd.DataFrame,
        path: str = 'data/processed/dataframe.csv',
    ) -> None:
    """
    Save a pandas DataFrame to CSV format.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to save.
    path : str | Optional
        The path where the DataFrame should be saved, by default 'data/processed/dataframe.csv'
    """
    df.to_csv(path)

