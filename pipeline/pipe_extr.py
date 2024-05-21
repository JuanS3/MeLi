import pandas as pd
from etl.utils import (
    pprint,
    decorators as dec,
)
from etl.extr import extraction as extr
from etl import transversal as tr


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
    tr.print_basic_df_info(df=csv)
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
    tr.print_basic_df_info(df=json)
    return json


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

    array: tr.ParquetArray = (
        (pays, 'pays'),
        (tabs, 'taps'),
        (prints, 'prints')
    )
    tr.to_parquet(array=array, file_path='data/raw')


if __name__ == '__main__':
    run()

