import pandas as pd
from etl.utils import pprint
from etl.extr import extraction as extr


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


def run():
    pprint.title('Pipeline Extract')
    pays: pd.DataFrame = load_csv(path_file='data/raw/pays.csv')
    tabs: pd.DataFrame = load_json(path_file='data/raw/taps.json', multi_json=True)
    prints: pd.DataFrame = load_json(path_file='data/raw/prints.json', multi_json=True)


if __name__ == '__main__':
    run()
