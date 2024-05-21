import pandas as pd
from typing import Any
from etl.extr import extraction as extr
from etl.trsf import transform as trsf
from etl.load import load
from etl.utils import (
    pprint,
    decorators as dec,
)
from etl import transversal as tr
import datetime as dt


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
def step_normalize(*, to_norm: tuple[str, ...], folder_files: str = 'data/raw', step_code: str = '010_') -> None:
    """
    Run the normalization step

    Parameters
    ----------
    to_norm: tuple[str, ...]
        A tuple with the names of the files to normalize
    folder_files: str, Optional
        Path to the folder where the files are stored, by default 'data/raw'
    step_code: str, Optional
        The step code, by default '010_'
    """
    pprint.title(f'STEP : Normalize -> {step_code}')
    ext: str = '.parquet.gzip'
    for parquet_file in to_norm:
        parquet: pd.DataFrame = load_parquet(path_file=f'{folder_files}/{parquet_file}{ext}')
        parquet_norm: pd.DataFrame = normalize_json_columns(df=parquet)
        array: tr.ParquetArray = ((parquet_norm, f'{step_code}{parquet_file}'),)
        tr.to_parquet(array=array, file_path='data/staging')


@dec.time_it
def filter_last_weeks(
        *,
        to_norm: tuple[str, ...],
        folder_dest: str = 'data/staging',
        folder_orig: str = 'data/staging',
        step_code: str = '021_',
        weeks: int = 1
    ) -> None:
    """
    Run the filtering step

    Parameters
    ----------
    to_norm: tuple[str, ...]
        A tuple with the names of the files to normalize
    folder_dest: str, Optional
        Path to the folder where the files are stored, by default 'data/staging'
    folder_orig: str, Optional
        Path to the folder where the files are stored, by default 'data/staging'
    step_code: str, Optional
        The step code, by default '021_'
    weeks: int, Optional
        The number of weeks to filter, by default 1
    """
    pprint.title(f'STEP : Filtering | last {weeks} weeks | -> {step_code}')
    for parquet_file in to_norm:
        parquet: pd.DataFrame = load_parquet(path_file=f'{folder_orig}/{parquet_file}.parquet.gzip')
        column: str = 'day' if 'day' in parquet.columns.values else 'pay_date'
        max_col_value: Any = trsf.get_max_data_column(df=parquet, column=column)
        max_date: dt.datetime = dt.datetime.strptime(max_col_value, '%Y-%m-%d')
        last_7_days: dt.date = ( max_date - dt.timedelta( days=( 7 * weeks ) ) ).date()
        start_date: str = last_7_days.strftime('%Y-%m-%d')
        parquet_filter: pd.DataFrame = trsf.filter_by_day(
            df=parquet,
            date_col=column,
            start_date=start_date,
            end_date=max_col_value
        )
        array: tr.ParquetArray = ((parquet_filter, f'{step_code}{parquet_file}'),)
        tr.to_parquet(array=array, file_path=folder_dest, print_info=True)


@dec.time_it
def filter_by_values(
        *,
        to_filter: tuple[str, ...],
        folder_dest: str = 'data/staging',
        folder_orig: str = 'data/staging',
        step_code: str = '022_',
        column: str = 'user_id',
        filter_from: str = 'prints',
    ):
    pprint.title(f'STEP : Filtering | Users ID | -> {step_code}')

    prints: pd.DataFrame = load_parquet(path_file=f'{folder_orig}/{filter_from}.parquet.gzip')
    column_value: pd.Series = trsf.get_column(df=prints, column=column)
    for tfilter in to_filter:
        parquet: pd.DataFrame = load_parquet(path_file=f'{folder_dest}/{tfilter}.parquet.gzip')
        parquet_filter: pd.DataFrame = trsf.filter_by_values(df=parquet, column=column, values=column_value)
        tr.to_parquet(
            array=(
                (parquet_filter, f'{step_code}{tfilter}'),
            ),
            file_path=folder_dest,
            print_info=True
        )

@dec.time_it
def step_filtering(
        *,
        to_filter: dict[int, tuple[str, ...]],
        folder_dest: str = 'data/staging',
        folder_orig: str = 'data/staging',
        step_code: str = '020_'
    ) -> None:
    """
    Run the filtering step

    Parameters
    ----------
    to_norm: tuple[str, ...]
        A tuple with the names of the files to normalize
    folder_dest: str, Optional
        Path to the folder where the files are stored, by default 'data/staging'
    folder_orig: str, Optional
        Path to the folder where the files are stored, by default 'data/staging'
    step_code: str, Optional
        The step code, by default '020_'
    """
    pprint.title(f'STEP : Filtering -> {step_code}')
    for ws, tfilter in to_filter.items():
        filter_last_weeks(to_norm=tfilter, folder_dest=folder_dest, folder_orig=folder_orig, weeks=ws)

    filter_by_values(
        to_filter=('021_010_taps', '021_010_pays'),
        filter_from='021_010_prints',
    )


@dec.time_it
def run(steps: tuple[str, ...] = ('normalize','filter_las_week')) -> None:
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
        step_normalize(to_norm=('prints', 'taps', 'pays'))

    if 'filter_las_week' in steps:
        step_filtering(to_filter={
            1: ('010_prints',),
            3: ('010_taps', '010_pays'),
        })


if __name__ == '__main__':
    run()

