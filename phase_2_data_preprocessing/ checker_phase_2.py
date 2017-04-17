import numpy as np
import re


def check_dataframe_columns(df, columns, table_name):
    """
    Checks whether the given DataFrame contains all the columns scecified.
    :param df: DataFrame.
    :param columns: list of numpy array with column names.
    :param table_name: name of the DataFrames. It is used when an exception is raised.
    :raises Exception: if there are some columns missing in the DataFrame.
    """
    df_columns = df.columns
    missing_columns = np.setdiff1d(columns, df_columns)
    if len(missing_columns) > 0:
        raise Exception("DataFrame " + table_name + " does not contain the following columns: " + str(
            missing_columns) + ".")
        

def check_dataframe_columns_using_pattern(df, columns_prefix, required_columns_count, table_name):
    """
    Checks whether the given DataFrame contains specified number of columns with a given prefix name.
    :param df: DataFrame.
    :param columns_prefix: string prefix of the columns.
    :param required_columns_count: number of columns that should be in the DataFrame with the given prefix.
    :param table_name: name of the DataFrames. It is used when an exception is raised.
    :raises Exception: if the number of required columns does not match.
    """
    col_names = df.columns
    r = re.compile(columns_prefix)
    vmatch = np.vectorize(lambda x: bool(r.match(x)))
    sel = vmatch(col_names)
    matching_col_names = col_names[sel]
    if len(matching_col_names) != required_columns_count:
        raise Exception("There are only " + str(len(matching_col_names)) + " column with prefix '" + columns_prefix
                        + "' in the DataFrame '" + table_name + "', but there should be " + str(required_columns_count)
                        + " of them.")

def check_dataframe_nonemptiness(df, table_name):
    """
    Checks emptiness of a Pandas DataFrame.
    :param df: Pandas DataFrame.
    :param table_name: name of the Pandas DataFrames. It is used when an exception is raised.
    :raises Exception: if the DataFrame does not contain any row.
    """
    if df.count() == 0:
        raise Exception("DataFrame " + table_name + " has no records.")
