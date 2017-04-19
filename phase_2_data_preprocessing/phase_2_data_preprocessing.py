from pyspark.sql.types import BooleanType
from time import gmtime, strftime
import numpy as np


def log(message = ""):
    print(strftime("%Y-%m-%d %H:%M:%S", gmtime()) + " " + message)

    
def drop_unused_columns(dataframe, columns_to_be_dropped):
    """
    Drops columns in a DataFrame.
    :param dataframe: DataFrame to be modified.
    :param columns_to_be_dropped: string list of column names.
    :return: new reduced DataFrame.
    """
    return dataframe.select([column for column in dataframe.columns if column not in columns_to_be_dropped])


def fillna(column_name, method, df, df_test=None, value=None):
    """
    Fill NA in a given column by using a specified method, original DataFrame is not modified.
    More specifically, it computes a new value from df and then replaces NA values with this value.
    If df_test is specified, the value is also used here.
    Method = 'constant' just fills the cells with a value given by the 'value' parameter.
    :param column_name: column name string.
    :param method: 'median', '-median', 'constant'.
    :param df: DataFrame on which the method should be applied (computed and filled).
    :param df_test: optional DataFrame where the same value should be filled.
    :param value: value to be used when method = 'constant'
    :raises: KeyError: if one of the DataFrames does not contain specified column.
    :return: modified df and optionally also modified df_test.
    """
    if column_name not in df.columns:
        raise KeyError("Column '" + column_name + "' not found in df DataFrame.")
    if method == 'median':
        new_val = df.approxQuantile(column_name, [0.5], 0.01)[0]
    elif method == '-median':
        new_val = - df.approxQuantile(column_name, [0.5], 0.01)[0]
    elif method == 'constant':
        if value is None:
            raise ValueError("'value' attribute must be provided for method=constant")
        new_val = value
    else:
        raise ValueError("'method' attribute can take only these values: median, -median, constant")
    df = df.na.fill({column_name: new_val})
    if df_test is not None:
        if column_name not in df_test.columns:
            raise KeyError("Column '" + column_name + "' not found in df_test DataFrame.")
        df_test = df_test.na.fill({column_name: new_val})
        return df, df_test
    else:
        return df


def fill_na_of_community_attributes(dataframe):
    """
    Fill NA values in columns related to communities.
    :param dataframe: Dataframe to be updated.
    :raises: KeyError: if the DataFrame does not contain any of these columns:
    com_degree, com_degree_total, com_count_in_group, com_degree_in_group, com_score,
    com_group_leader, com_group_follower, com_churned_cnt, com_leader_churned_cnt
    :return: modified Dataframe.
    """
    dataframe = fillna('com_degree', 'constant', dataframe, value=0)
    dataframe = fillna('com_degree_total', 'constant', dataframe, value=0)
    dataframe = fillna('com_count_in_group', 'constant', dataframe, value=1)
    dataframe = fillna('com_degree_in_group', 'constant', dataframe, value=0)
    dataframe = fillna('com_score', 'constant', dataframe, value=1.0)
    dataframe = fillna('com_group_leader', 'constant', dataframe, value=True)
    dataframe = fillna('com_group_follower', 'constant', dataframe, value=True)
    dataframe = fillna('com_churned_cnt', 'constant', dataframe, value=0)
    dataframe = fillna('com_leader_churned_cnt', 'constant', dataframe, value=0)
    return dataframe



def fill_na_cells(training_data, predict_data, callcenters_columns_prefix, calls_attributes_prefixes):
    """
    Fill NA in various columns.
    :param training_data: DataFrame with train dataset.
    :param predict_data: DataFrame with predict dataset.
    :param callcenters_columns_prefix: string prefix of the callcenters columns.
    :param calls_attributes_prefixes: string list of the call-attributes columns.
    :raises: KeyError: if the DataFrame does not contain any of these columns:
    com_degree, com_degree_total, com_count_in_group, com_degree_in_group, com_score,
    com_group_leader, com_group_follower, com_churned_cnt, com_leader_churned_cnt
    """
    # columns of callcenters calls - fill with 0.0:
    selected_columns = [column for column in training_data.columns if column.startswith(callcenters_columns_prefix)]
    training_data = training_data.na.fill(0.0, subset=selected_columns)
    predict_data = predict_data.na.fill(0.0, subset=selected_columns)
    # columns of calls attributes:
    selected_columns = [column for column in training_data.columns if column.startswith(tuple(calls_attributes_prefixes))]
    training_data = training_data.na.fill(0.0, subset=selected_columns)
    predict_data = predict_data.na.fill(0.0, subset=selected_columns)
    # fill.na of community attributes:
    training_data = fill_na_of_community_attributes(training_data)
    predict_data = fill_na_of_community_attributes(predict_data)
    return training_data, predict_data


def convert_columns_to_boolean(dataframe, column_names):
    """
    Converts columns in the given dataframe to boolean type.
    :param dataframe: DataFrame.
    :param column_names: string list of column names.
    :return: modified Dataframe.
    """
    for column_name in column_names:
        dataframe = dataframe.withColumn(column_name,dataframe[column_name].cast(BooleanType()))
    return dataframe


def create_ratio_attribute(dataframe, new_column, numerator, denominator):
    """
    Creates a new ratio attribute by dividing two existing attributes.
    :param dataframe: DataFrame to be modified
    :param new_column: string name of the new attribute
    :param numerator: string name of the attribute used as numerator.
    :param denominator: string name of the attribute used as denominator.
    :raises: KeyError: if the DataFrame does not contain numerator or denominator attributes
    :return: modified Dataframe.
    """
    if numerator not in dataframe.columns:
        raise KeyError("Column '" + numerator + "' not found in df DataFrame.")
    if denominator not in dataframe.columns:
        raise KeyError("Column '" + denominator + "' not found in df DataFrame.")
    return dataframe.withColumn(new_column, dataframe[numerator] / dataframe[denominator]).na.fill({new_column: 0.0})


def create_ratio_call_attributes(dataframe):
    """
    Creates new ratio attributes based on the call attributes.
    :param dataframe: DataFrame to be modified.
    :raises: KeyError: if the DataFrame does not contain all numerator or denominator attributes
    """
    dataframe = create_ratio_attribute(dataframe, 'cnt_incoming_calls_non_t_vs_all_ratio',
                           'cnt_incoming_calls_non_t', 'cnt_incoming_calls_all')
    dataframe = create_ratio_attribute(dataframe, 'cnt_outgoing_calls_non_t_vs_all_ratio',
                           'cnt_outgoing_calls_non_t', 'cnt_outgoing_calls_all')
    dataframe = create_ratio_attribute(dataframe, 'cnt_calls_non_t_vs_all_ratio',
                           'cnt_non_t_calls', 'cnt_calls_all')
    dataframe = create_ratio_attribute(dataframe, 'dur_incoming_calls_non_t_vs_all_ratio',
                           'dur_incoming_calls_non_t', 'dur_incoming_calls_all')
    dataframe = create_ratio_attribute(dataframe, 'dur_outgoing_calls_non_t_vs_all_ratio',
                           'dur_outgoing_calls_non_t', 'dur_outgoing_calls_all')
    dataframe = create_ratio_attribute(dataframe, 'dur_calls_non_t_vs_all_ratio',
                           'dur_non_t_calls', 'dur_calls_all')
    return dataframe


def run(cfg, cfg_tables, sqlContext):
    """
    Main computation block of the 2nd phase.
    :param cfg: dictionary of configuration constants.
    :param cfg_tables: dictionary of tmp table names.
    :param sqlContext: current pyspark sqlContext.
    """
    log('Running phase 2 - Data preprocessing.')
    # configuration:
    CATEGORICAL_ATTRIBUTES = ["rateplan_group", "rateplan_name", "committed",
                              "com_group_leader", "com_group_follower"]
    CALLCENTERS_ATTRIBUTES_PREFIX = "cc_"
    CALLS_ATTRIBUTES_PREFIXES = ["cnt", "dur", "avg", "std"]
    LABEL_ATTRIBUTE = "churned"
#    COLUMNS_TO_BE_DROPPED = ['msisdn', 'customer_type', 'calls_non_t_dur', 'calls_non_t_cnt',
#                             'calls_all_dur', 'calls_all_cnt']
    COLUMNS_TO_BE_DROPPED = ['customer_type', 'calls_non_t_dur', 'calls_non_t_cnt',
                             'calls_all_dur', 'calls_all_cnt']
    ALL_TRAIN_COLUMNS_WITHOUT_CC = np.array(
        ['msisdn', 'customer_type', 'rateplan_group', 'rateplan_name',
         'churned', 'committed', 'committed_days',
         'commitment_remaining',
         'callcenter_calls_count', 'callcenter_calls_duration', 'calls_non_t_dur',
         'calls_non_t_cnt',
         'calls_all_dur', 'calls_all_cnt', 'com_degree', 'com_degree_total',
         'com_count_in_group', 'com_degree_in_group', 'com_score', 'com_group_leader',
         'com_group_follower', 'com_churned_cnt', 'com_leader_churned_cnt',
         'cnt_incoming_calls_all',
         'dur_incoming_calls_all', 'avg_dur_incoming_calls_all', 'std_dur_incoming_calls_all',
         'cnt_outgoing_calls_all', 'dur_outgoing_calls_all', 'avg_dur_outgoing_calls_all',
         'std_dur_outgoing_calls_all', 'cnt_t_calls', 'dur_t_calls', 'avg_dur_t_calls',
         'std_dur_t_calls', 'cnt_non_t_calls', 'dur_non_t_calls', 'avg_dur_non_t_calls',
         'std_dur_non_t_calls', 'cnt_incoming_calls_t', 'dur_incoming_calls_t',
         'avg_dur_incoming_calls_t', 'std_dur_incoming_calls_t', 'cnt_incoming_calls_non_t',
         'dur_incoming_calls_non_t', 'avg_dur_incoming_calls_non_t',
         'std_dur_incoming_calls_non_t',
         'cnt_outgoing_calls_t', 'dur_outgoing_calls_t', 'avg_dur_outgoing_calls_t',
         'std_dur_outgoing_calls_t', 'cnt_outgoing_calls_non_t', 'dur_outgoing_calls_non_t',
         'avg_dur_outgoing_calls_non_t', 'std_dur_outgoing_calls_non_t', 'cnt_calls_all',
         'dur_calls_all', 'avg_dur_calls_all', 'std_dur_calls_all', 'cnt_calls_under_5s'])
    
    # load data:
    log("Loading data")
    training_data = sqlContext.read.parquet(cfg_tables['TMP_TABLE_TRAIN'])
    predict_data = sqlContext.read.parquet(cfg_tables['TMP_TABLE_PREDICT'])
    
    # check the columns of the dataframes
    check_dataframe_columns(training_data, ALL_TRAIN_COLUMNS_WITHOUT_CC, "training_data")
    # the same for predict data set (but without the 'churned' column)
    check_dataframe_columns(predict_data, ALL_TRAIN_COLUMNS_WITHOUT_CC[ALL_TRAIN_COLUMNS_WITHOUT_CC != 'churned'],
                            "predict_data")
    # check that the dataframes contain required number of columns with the given prefix (of callcenters calls)
    check_dataframe_columns_using_pattern(training_data, "cc_dur", cfg['COMMON_TOP_CALLCENTERS_COUNT'], "training_data")
    check_dataframe_columns_using_pattern(training_data, "cc_cnt", cfg['COMMON_TOP_CALLCENTERS_COUNT'], "training_data")
    check_dataframe_columns_using_pattern(predict_data, "cc_dur", cfg['COMMON_TOP_CALLCENTERS_COUNT'], "predict_data")
    check_dataframe_columns_using_pattern(predict_data, "cc_cnt", cfg['COMMON_TOP_CALLCENTERS_COUNT'], "predict_data")
    # check that the dataframes are not empty
    check_dataframe_nonemptiness(training_data, "training_data")
    check_dataframe_nonemptiness(predict_data, "predict_data")
    
    # remove rows where committed IS NULL (this is a temporary fix because of noise in the base data)
    # training_data = training_data[~training_data['committed'].isnull()].reset_index(drop=True)
    # predict_data = predict_data[~predict_data['committed'].isnull()].reset_index(drop=True)
    # TODO: overit ze to neni potreba
    
    # log("Getting msisdn values")
    # msisdn_from_predict_data = predict_data['msisdn'].values
    
    log("Removing unused columns")
    training_data = drop_unused_columns(training_data, COLUMNS_TO_BE_DROPPED)
    predict_data = drop_unused_columns(predict_data, COLUMNS_TO_BE_DROPPED)
    
    log("Imputing NA values")
    training_data, predict_data = fill_na_cells(training_data, predict_data, CALLCENTERS_ATTRIBUTES_PREFIX, CALLS_ATTRIBUTES_PREFIXES)
    
    log("Converting columns to boolean")
    training_data = convert_columns_to_boolean(training_data, ['com_group_leader', 'com_group_follower', 'committed'])
    predict_data = convert_columns_to_boolean(predict_data, ['com_group_leader', 'com_group_follower', 'committed'])
    
    log("Creating ratio call attributes")
    training_data = create_ratio_call_attributes(training_data)
    predict_data = create_ratio_call_attributes(predict_data)
    
    log("Imputing NA commitment_remaining with median")
    training_data, predict_data = fillna("commitment_remaining", "-median", training_data, predict_data)
    
    # check_dataframe_has_no_missing_values(training_data, "training_data")
    # check_dataframe_has_no_missing_values(predict_data, "predict_data")
    
    training_data.write.parquet(cfg_tables['TABLE_TRAIN'], mode='overwrite')
    predict_data.write.parquet(cfg_tables['TABLE_PREDICT'], mode='overwrite')
    
    # TODO: tady to bude nove
#    log("Encoding categorical attributes")
#    training_data, predict_data = encode_categorical_attributes(CATEGORICAL_ATTRIBUTES, training_data, predict_data)
#    
#    log("Encoding label attribute")
#    # training_data, predict_data = encode_label_attribute(LABEL_ATTRIBUTE, training_data, predict_data)
#    training_data = encode_label_attribute(LABEL_ATTRIBUTE, training_data, None)
#    
#    # check that the dataframes contain only numeric values
#    check_dataframe_contains_only_numeric_values(training_data, "training_data")
#    check_dataframe_contains_only_numeric_values(predict_data, "predict_data")
#    # check that the dataframes has no infinite values:
#    check_dataframe_has_no_infinite_values(training_data, "training_data")
#    check_dataframe_has_no_infinite_values(predict_data, "predict_data")
#    y_train = training_data.churned.values
#    training_data.drop([LABEL_ATTRIBUTE], axis=1, inplace=True)
#    X_train = training_data.values
#    X_predict = predict_data.values
#    
#    log("Saving data in numpy arrays for scikit learn")
#    # Save data:
#    # if the directory for the data does not exist, try to create it
#    make_dirs_for_file_if_not_exist(cfg_tables['TRAIN_DATA_FILE'])
#    X_train.dump(cfg_tables['TRAIN_DATA_FILE'])
#    X_predict.dump(cfg_tables['PREDICT_DATA_FILE'])
#    y_train.dump(cfg_tables['TRAIN_LABELS_FILE'])
#    pd.to_pickle(training_data.columns, cfg_tables['COLUMN_NAMES_FILE'])
#    pd.to_pickle(msisdn_from_predict_data, cfg_tables['PREDICT_LIST_MSISDN'])
#    log("Phase 2 DONE")