import pandas as pd
from etl.extr import extraction as extr
from etl.trsf import transform as trsf
from etl.load import load
from etl.utils import (
    pprint,
    decorators as dec,
)
from etl import transversal as tr


@dec.time_it
def load_parquet(*, path_file: str) -> pd.DataFrame:
    """
    Reads the parquet file and returns a pandas dataframe

    Parameters
    ----------
    path_file: str
        Path to the parquet file

    Returns
    -------
    pd.DataFrame
        A pandas dataframe with the data from the parquet file
    """
    parquet: pd.DataFrame = extr.load_parquet(file_path=path_file)
    pprint.success(f'Parquet {{ {path_file} }} loaded!')
    tr.print_basic_df_info(df=parquet)
    return parquet


@dec.time_it
def normalize_json_columns(*, df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize json columns and returns a pandas dataframe

    Parameters
    ----------
    df: pd.DataFrame
        A pandas dataframe with the data from the parquet file

    Returns
    -------
    pd.DataFrame
        A pandas dataframe with normalized json columns
    """
    df_norm: pd.DataFrame = trsf.pdjson_normalize(df=df)
    pprint.success('JSON normalized!')
    tr.print_basic_df_info(df=df_norm)
    return df_norm


@dec.time_it
def step_normalize(*, to_norm: tuple[str, ...], folder_files: str = 'data/raw', step_code: str = '001_') -> None:
    """
    Run the normalization step

    Parameters
    ----------
    to_norm: tuple[str, ...]
        A tuple with the names of the files to normalize
    folder_files: str, Optional
        Path to the folder where the files are stored, by default 'data/raw'
    step_code: str, Optional
        The step code, by default '001_'
    """
    ext: str = '.parquet.gzip'
    for parquet_file in to_norm:
        parquet: pd.DataFrame = load_parquet(path_file=f'{folder_files}/{parquet_file}{ext}')
        parquet_norm: pd.DataFrame = normalize_json_columns(df=parquet)
        array: tr.ParquetArray = ((parquet_norm, f'{step_code}{parquet_file}'),)
        tr.to_parquet(array=array, file_path='data/staging')


@dec.time_it
def run(steps: tuple[str] = ('normalize',)) -> None:
    """
    Pipeline to transform the data and save it in a parquet files with gzip compression,
    the data fules are stored in the 'data/staging' folder by default

    Parameters
    ----------
    steps: tuple[str]
        Steps to execute in the pipeline
    """
    pprint.title('Pipeline Transform')

    if 'normalize' in steps:
        step_normalize(to_norm=('prints', 'taps'))


if __name__ == '__main__':
    run()

