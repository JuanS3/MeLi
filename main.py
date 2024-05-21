import pandas as pd


def read_json(*, path: str) -> pd.DataFrame:
    """
    Reads the JSON file and store it in a dataframe

    Parameters
    ----------
    path : str
        The path to the JSON file

    Returns
    -------
    pd.DataFrame
        The dataframe with the data from the JSON file
    """
    df: pd.DataFrame = pd.read_json(path)
    return df


def read_csv(*, path: str) -> pd.DataFrame:
    """
    Reads the CSV file and store it in a dataframe

    Parameters
    ----------
    path : str
        The path to the CSV file

    Returns
    -------
    pd.DataFrame
        The dataframe with the data from the CSV file
    """
    df: pd.DataFrame = pd.read_csv(path)
    return df


if __name__ == '__main__':
    csv_path: str = 'data/pays.csv'
    df = read_csv(path=csv_path)
    print('--------------------------------------------------')
    print('-                    Pays CSV                    -')
    print('--------------------------------------------------')
    print(df.head())

    for p in ('data/taps.json', 'data/prints.json'):
        # df = read_json(path=p)
        print('--------------------------------------------------')
        print(f'-           {p}              -')
        print('--------------------------------------------------')
        # print(df)
        print(pd.read_json(p))
    print('--------------------------------------------------')

