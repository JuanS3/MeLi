import pandas as pd
from etl.load import load
from typing import TypeAlias
from etl.utils import pprint


FileName: TypeAlias = str
ParquetArray: TypeAlias = tuple[tuple[pd.DataFrame, FileName], ...]


def to_parquet(*, array: ParquetArray, file_path: str, print_info: bool = False) -> None:
    """
    Save a dataframe in a parquet file

    Parameters
    ----------
    array: ParquetArray
        An iterable object with the dataframes to be saved
    file_path: str
        Path of the parquet file to be saved
    """
    pprint.info(msg=f'Saving parquet into {{ {file_path} }}')
    for df, name in array:
        load.dataframe_to_parquet(df=df, path=f'{file_path}/{name}.parquet.gzip')
        pprint.success(f'parquet {{ {name} }} saved')
        if print_info:
            print_basic_df_info(df=df)


def print_basic_df_info(*, df: pd.DataFrame) -> None:
    """
    Print basic information about a dataframe

    Parameters
    ----------
    df: pd.DataFrame
        The dataframe to be printed
    """
    print()
    pprint.info(f'shape: {df.shape}')
    pprint.info(f'columns: {df.columns}')
    pprint.info(f'data:\n{df.head()}')
    pprint.info(f'null values:\n{df.isnull().sum()}')
    pprint.info(f'dtypes:\n{df.dtypes}')
    print()

