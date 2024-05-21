import pandas as pd


def load_csv(*, file_path: str, delimeter: str = ',') -> pd.DataFrame:
    """
    Reads a csv file and returns a pandas dataframe

    Parameters
    ----------
    file_path: str
        The path of the file
    delimeter: str
        The delimeter of the file

    Returns
    -------
    pd.DataFrame
        A pandas dataframe with the data from the csv file
    """
    df: pd.DataFrame = pd.read_csv(file_path, delimiter=delimeter)
    return df


def load_json(*, file_path: str, multi_json: bool = False) -> pd.DataFrame:
    """
    Reads a json file and returns a pandas dataframe

    Parameters
    ----------
    file_path: str
        The path of the file
    multi_json: bool
        If the file has multiple json objects

    Returns
    -------
    pd.DataFrame
        A pandas dataframe with the data from the json file
    """
    df: pd.DataFrame = pd.read_json(file_path) if not multi_json else pd.read_json(file_path, lines=True)
    return df


def load_parquet(*, file_path: str) -> pd.DataFrame:
    """
    Reads a parquet file and returns a pandas dataframe

    Parameters
    ----------
    file_path: str
        The path of the file

    Returns
    -------
    pd.DataFrame
        A pandas dataframe with the data from the parquet file
    """
    df: pd.DataFrame = pd.read_parquet(file_path, engine='pyarrow')
    return df

