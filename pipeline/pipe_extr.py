import pandas as pd
from typing import TypeAlias
from etl.load import load
from etl.utils import (
    pprint,
    decorators as dec,
)
from etl.extr import extraction as extr


ParquetArray: TypeAlias = tuple[tuple[pd.DataFrame, str]]


@dec.time_it
def load_csv(*, path_file: str) -> pd.DataFrame:
    """
    Read CSV file and display its info in the console

    Parameters
    ----------
    path_file: str
        Path of the CSV file to be loaded

    Returns
    -------
    pd.DataFrame
        A Pandas dataframe with the CSV data
    """
    csv: pd.DataFrame = extr.load_csv(file_path=path_file)
    pprint.success(f'CSV {{ {path_file} }} loaded')
    print()
    pprint.info(f'shape: {csv.shape}')
    pprint.info(f'columns: {csv.columns}')
    pprint.info(f'data:\n{csv.head()}')
    pprint.info(f'null values:\n{csv.isnull().sum()}')
    print()
    return csv


@dec.time_it
def load_json(*, path_file: str, multi_json: bool = False) -> pd.DataFrame:
    """
    Read JSON file and display its info in the console

    Parameters
    ----------
    path_file: str
        Path of the JSON file to be loaded
    multi_json: bool
        If the JSON file contains multiple JSON objects

    Returns
    -------
    pd.DataFrame
        A Pandas dataframe with the JSON data
    """
    json: pd.DataFrame = extr.load_json(file_path=path_file, multi_json=multi_json)
    pprint.success(f'JSON {{ {path_file} }} loaded')
    print()
    pprint.info(f'shape: {json.shape}')
    pprint.info(f'columns: {json.columns}')
    pprint.info(f'data:\n{json.head()}')
    pprint.info(f'null values:\n{json.isnull().sum()}')
    print()
    return json


def to_parquet(*, array: ParquetArray, file_path: str) -> None:
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


@dec.time_it
def run() -> None:
    """
    Pipeline to extract data from different sources and save it in a parquet file with gzip,
    the files are stored in the 'data/raw' folder by default
    """
    pprint.title('Pipeline Extract')
    folder: str = 'data/external'
    pays: pd.DataFrame = load_csv(path_file=f'{folder}/pays.csv')
    tabs: pd.DataFrame = load_json(path_file=f'{folder}/taps.json', multi_json=True)
    prints: pd.DataFrame = load_json(path_file=f'{folder}/prints.json', multi_json=True)

    array: ParquetArray = (
        (pays, 'pays'),
        (tabs, 'tabs'),
        (prints, 'prints')
    )
    to_parquet(array=array, file_path='data/raw')

if __name__ == '__main__':
    run()

