from sql_query_preparator import *
from sql_utils import *
from checker_phase_1 import check_table_created_and_nonempty, check_table_exist_and_nonempty,\
    TableNotCreatedError, TableEmptyError, TableDoesNotExistError
import sys
import traceback
def run_with_exceptions_handled(cfg, cfg_tables, logger, sqlContext):
    """
    Main computation block of the 1st phase.
    :param cfg: dictionary of configuration constants.
    :param cfg_tables: dictionary of tmp table names.
    :param logger: current logger.
    :param sqlContext: current pyspark sqlContext.
    """
    print("Preparing data to DB")
    df1 = sqlContext.read.parquet("s3a://mlp2017/test/mlp_cdr_test.parquet")
    df2 = sqlContext.read.parquet("s3a://mlp2017/test/mlp_ebr_base_20160301.parquet")
    df3 = sqlContext.read.parquet("s3a://mlp2017/test/mlp_ebr_base_20160401.parquet")
    df4 = sqlContext.read.parquet("s3a://mlp2017/test/mlp_ebr_churners_20151201_20160630.parquet")
    
    df1.createOrReplaceTempView(cfg['CDR_TABLE'])
    df2.createOrReplaceTempView(cfg['BASE_TAB_DATE_B'])
    df3.createOrReplaceTempView(cfg['BASE_TAB_DATE_C'])
#    df4.createOrReplaceTempView(cfg['CHURNS_TABLE'])
    
    print("Preparing train data")
    check_table_exist_and_nonempty(sqlContext, cfg['BASE_TAB_DATE_B'])
    check_table_exist_and_nonempty(sqlContext, cfg['BASE_TAB_DATE_C'])
    check_table_exist_and_nonempty(sqlContext, cfg['CDR_TABLE'])
    check_table_exist_and_nonempty(sqlContext, cfg['CHURNS_TABLE'])
    check_table_exist_and_nonempty(sqlContext, cfg['TRAIN_COMMUNITY_TABLE'])
    check_table_exist_and_nonempty(sqlContext, cfg['PREDICT_COMMUNITY_TABLE'])
    print("Preparing churn-base table")
    sql_train_1a = prepare_sql_drop_table_if_exists(cfg_tables['TMP_TRAIN_CHURN_BASE_TABLE'])
    sql_train_1b = prepare_sql_churn_base_data(churn_table_name=cfg['CHURNS_TABLE'],
                                               base_table_name=cfg['BASE_TAB_DATE_B'],
                                               churn_from_date=cfg['DATE_B'],
                                               churn_to_date=cfg['DATE_C'],
                                               restrict_commitment_to=cfg['RESTRICT_COMMITMENT_TO'],
                                               target_table_name=cfg_tables['TMP_TRAIN_CHURN_BASE_TABLE'])
    execute_sql_query(sqlContext, sql_train_1a)
    execute_sql_query(sqlContext, sql_train_1b)
    check_table_created_and_nonempty(sqlContext, cfg_tables['TMP_TRAIN_CHURN_BASE_TABLE'])
    print("Preparing top callcenters table")
    sql_train_2a = prepare_sql_drop_table_if_exists(cfg_tables['TMP_COMMON_CALLCENTERS_TABLE'])
    sql_train_2b = prepare_sql_top_call_centers(cdr_from_date=cfg['DATE_A'], cdr_to_date=cfg['DATE_B'],
                                                cdr_table_name=cfg['CDR_TABLE'], base_table_name=cfg['BASE_TAB_DATE_B'],
                                                number_of_callcenters=cfg['COMMON_TOP_CALLCENTERS_COUNT'],
                                                target_table_name=cfg_tables['TMP_COMMON_CALLCENTERS_TABLE'])
    execute_sql_query(sqlContext, sql_train_2a)
    execute_sql_query(sqlContext, sql_train_2b)
    check_table_created_and_nonempty(sqlContext, cfg_tables['TMP_COMMON_CALLCENTERS_TABLE'])
    
    
    print("Preparing table with first features")
    sql_train_3a = prepare_sql_drop_table_if_exists(cfg_tables['TMP_TRAIN_TARGET_TABLE_1'])
    sql_train_3b = prepare_sql_first_features(cdr_table_name=cfg['CDR_TABLE'],
                                              churn_base_table_name=cfg_tables['TMP_TRAIN_CHURN_BASE_TABLE'],
                                              callcenters_table_name=cfg_tables['TMP_COMMON_CALLCENTERS_TABLE'],
                                              cdr_from_date=cfg['DATE_A'], cdr_to_date=cfg['DATE_B'],
                                              target_table_name=cfg_tables['TMP_TRAIN_TARGET_TABLE_1'],
                                              contains_churn_info=True)
    execute_sql_query(sqlContext, sql_train_3a)
    execute_sql_query(sqlContext, sql_train_3b)
    check_table_created_and_nonempty(sqlContext, cfg_tables['TMP_TRAIN_TARGET_TABLE_1'])
    # this must be run after the table with callcenters is created!
    topcallcenters_df = sqlContext.sql("SELECT * FROM " + cfg_tables['TMP_COMMON_CALLCENTERS_TABLE']).toPandas()
    print("Preparing callcenters calls duration")
    sql_train_3Xa = prepare_sql_drop_table_if_exists(cfg_tables['TMP_TRAIN_CALLCENTERS_CALLS_DURATION_TABLE'])
    sql_train_3Xb = prepare_sql_callcenters_calls_duration(cdr_table_name=cfg['CDR_TABLE'], cdr_from_date=cfg['DATE_A'],
                                                           cdr_to_date=cfg['DATE_B'],
                                                           callcenters_table_name=cfg_tables['TMP_COMMON_CALLCENTERS_TABLE'],
                                                           callcenters_df=topcallcenters_df,
                                                           target_table_name=cfg_tables['TMP_TRAIN_CALLCENTERS_CALLS_DURATION_TABLE'])
    execute_sql_query(sqlContext, sql_train_3Xa)
    execute_sql_query(sqlContext, sql_train_3Xb)
    check_table_created_and_nonempty(sqlContext, cfg_tables['TMP_TRAIN_CALLCENTERS_CALLS_DURATION_TABLE'])
    print("Preparing callcenters calls counts")
    sql_train_3Ya = prepare_sql_drop_table_if_exists(cfg_tables['TMP_TRAIN_CALLCENTERS_CALLS_CNT_TABLE'])
    sql_train_3Yb = prepare_sql_callcenters_calls_cnt(cdr_table_name=cfg['CDR_TABLE'], cdr_from_date=cfg['DATE_A'],
                                                      cdr_to_date=cfg['DATE_B'],
                                                      callcenters_table_name=cfg_tables['TMP_COMMON_CALLCENTERS_TABLE'],
                                                      callcenters_df=topcallcenters_df,
                                                      target_table_name=cfg_tables['TMP_TRAIN_CALLCENTERS_CALLS_CNT_TABLE'])
    execute_sql_query(sqlContext, sql_train_3Ya)
    execute_sql_query(sqlContext, sql_train_3Yb)
    check_table_created_and_nonempty(sqlContext, cfg_tables['TMP_TRAIN_CALLCENTERS_CALLS_CNT_TABLE'])
    print("Preparing table with more features")
    sql_train_4a = prepare_sql_drop_table_if_exists(cfg_tables['TMP_TRAIN_TARGET_TABLE_2'])
    sql_train_4b = prepare_sql_second_features(base_table_name=cfg['BASE_TAB_DATE_B'], cdr_table_name=cfg['CDR_TABLE'],
                                               cdr_from_date=cfg['DATE_A'], cdr_to_date=cfg['DATE_B'],
                                               previous_table_name=cfg_tables['TMP_TRAIN_TARGET_TABLE_1'],
                                               target_table_name=cfg_tables['TMP_TRAIN_TARGET_TABLE_2'])
    execute_sql_query(sqlContext, sql_train_4a)
    execute_sql_query(sqlContext, sql_train_4b)
    check_table_created_and_nonempty(sqlContext, cfg_tables['TMP_TRAIN_TARGET_TABLE_2'])
    
    
    print("Preparing community features")
    sql_train_5a = prepare_sql_drop_table_if_exists(cfg_tables['TMP_TRAIN_COMMUNITY_ATTRIBUTES'])
    sql_train_5b = prepare_sql_community_attributes(base_table_name=cfg['BASE_TAB_DATE_B'],
                                                    community_table_name=cfg['TRAIN_COMMUNITY_TABLE'],
                                                    target_table_name=cfg_tables['TMP_TRAIN_COMMUNITY_ATTRIBUTES'],
                                                    churn_table_name=cfg['CHURNS_TABLE'],
                                                    cdr_to_date=cfg['DATE_B'])
    execute_sql_query(sqlContext, sql_train_5a)
    execute_sql_query(sqlContext, sql_train_5b)
    check_table_created_and_nonempty(sqlContext, cfg_tables['TMP_TRAIN_COMMUNITY_ATTRIBUTES'])
    print("Preparing call features")
    sql_train_6a = prepare_sql_drop_table_if_exists(cfg_tables['TMP_TRAIN_CALL_ATTRIBUTES'])
    sql_train_6b = create_call_atr_table(lb=cfg['DATE_A'], ub=cfg['DATE_B'],
                                         target_tab=cfg_tables['TMP_TRAIN_CALL_ATTRIBUTES'],
                                         cdr_tab=cfg['CDR_TABLE'], msisdn_table=cfg_tables['TMP_TRAIN_CHURN_BASE_TABLE'])
    execute_sql_query(sqlContext, sql_train_6a)
    execute_sql_query(sqlContext, sql_train_6b)
    check_table_created_and_nonempty(sqlContext, cfg_tables['TMP_TRAIN_CALL_ATTRIBUTES'])
    print("Preparing predict data")
    print("Preparing churn-base table")
    sql_test_1a = prepare_sql_drop_table_if_exists(cfg_tables['TMP_TEST_CHURN_BASE_TABLE'])
    sql_test_1b = prepare_sql_churn_base_data(churn_table_name=None, base_table_name=cfg['BASE_TAB_DATE_C'],
                                              churn_from_date=cfg['DATE_C'],
                                              churn_to_date=None,
                                              restrict_commitment_to=cfg['RESTRICT_COMMITMENT_TO'],
                                              target_table_name=cfg_tables['TMP_TEST_CHURN_BASE_TABLE'])
    execute_sql_query(sqlContext, sql_test_1a)
    execute_sql_query(sqlContext, sql_test_1b)
    check_table_created_and_nonempty(sqlContext, cfg_tables['TMP_TEST_CHURN_BASE_TABLE'])
    print("Preparing table with first features")
    sql_test_3a = prepare_sql_drop_table_if_exists(cfg_tables['TMP_TEST_TARGET_TABLE_1'])
    sql_test_3b = prepare_sql_first_features(cdr_table_name=cfg['CDR_TABLE'],
                                             churn_base_table_name=cfg_tables['TMP_TEST_CHURN_BASE_TABLE'],
                                             callcenters_table_name=cfg_tables['TMP_COMMON_CALLCENTERS_TABLE'],
                                             cdr_from_date=cfg['DATE_B'],
                                             cdr_to_date=cfg['DATE_C'],
                                             target_table_name=cfg_tables['TMP_TEST_TARGET_TABLE_1'],
                                             contains_churn_info=False)
    execute_sql_query(sqlContext, sql_test_3a)
    execute_sql_query(sqlContext, sql_test_3b)
    check_table_created_and_nonempty(sqlContext, cfg_tables['TMP_TEST_TARGET_TABLE_1'])
    
    
    print("Preparing callcenters calls duration")
    sql_test_3Xa = prepare_sql_drop_table_if_exists(cfg_tables['TMP_TEST_CALLCENTERS_CALLS_DURATION_TABLE'])
    sql_test_3Xb = prepare_sql_callcenters_calls_duration(cdr_table_name=cfg['CDR_TABLE'], cdr_from_date=cfg['DATE_B'],
                                                          cdr_to_date=cfg['DATE_C'],
                                                          callcenters_table_name=cfg_tables['TMP_COMMON_CALLCENTERS_TABLE'],
                                                          callcenters_df=topcallcenters_df,
                                                          target_table_name=cfg_tables['TMP_TEST_CALLCENTERS_CALLS_DURATION_TABLE'])
    execute_sql_query(sqlContext, sql_test_3Xa)
    execute_sql_query(sqlContext, sql_test_3Xb)
    check_table_created_and_nonempty(sqlContext, cfg_tables['TMP_TEST_CALLCENTERS_CALLS_DURATION_TABLE'])
    print("Preparing callcenters calls counts")
    sql_test_3Ya = prepare_sql_drop_table_if_exists(cfg_tables['TMP_TEST_CALLCENTERS_CALLS_CNT_TABLE'])
    sql_test_3Yb = prepare_sql_callcenters_calls_cnt(cdr_table_name=cfg['CDR_TABLE'], cdr_from_date=cfg['DATE_B'],
                                                     cdr_to_date=cfg['DATE_C'],
                                                     callcenters_table_name=cfg_tables['TMP_COMMON_CALLCENTERS_TABLE'],
                                                     callcenters_df=topcallcenters_df,
                                                     target_table_name=cfg_tables['TMP_TEST_CALLCENTERS_CALLS_CNT_TABLE'])
    execute_sql_query(sqlContext, sql_test_3Ya)
    execute_sql_query(sqlContext, sql_test_3Yb)
    check_table_created_and_nonempty(sqlContext, cfg_tables['TMP_TEST_CALLCENTERS_CALLS_CNT_TABLE'])
    print("Preparing table with more features")
    sql_test_4a = prepare_sql_drop_table_if_exists(cfg_tables['TMP_TEST_TARGET_TABLE_2'])
    sql_test_4b = prepare_sql_second_features(base_table_name=cfg['BASE_TAB_DATE_C'], cdr_table_name=cfg['CDR_TABLE'],
                                              cdr_from_date=cfg['DATE_B'], cdr_to_date=cfg['DATE_C'],
                                              previous_table_name=cfg_tables['TMP_TEST_TARGET_TABLE_1'],
                                              target_table_name=cfg_tables['TMP_TEST_TARGET_TABLE_2'])
    execute_sql_query(sqlContext, sql_test_4a)
    execute_sql_query(sqlContext, sql_test_4b)
    check_table_created_and_nonempty(sqlContext, cfg_tables['TMP_TEST_TARGET_TABLE_2'])
    print("Preparing community features")
    sql_test_5a = prepare_sql_drop_table_if_exists(cfg_tables['TMP_TEST_COMMUNITY_ATTRIBUTES'])
    sql_test_5b = prepare_sql_community_attributes(base_table_name=cfg['BASE_TAB_DATE_C'],
                                                   community_table_name=cfg['PREDICT_COMMUNITY_TABLE'],
                                                   target_table_name=cfg_tables['TMP_TEST_COMMUNITY_ATTRIBUTES'],
                                                   churn_table_name=cfg['CHURNS_TABLE'], cdr_to_date=cfg['DATE_C'])
    execute_sql_query(sqlContext, sql_test_5a)
    execute_sql_query(sqlContext, sql_test_5b)
    check_table_created_and_nonempty(sqlContext, cfg_tables['TMP_TEST_COMMUNITY_ATTRIBUTES'])
    
    
    print("Preparing call features")
    sql_test_6a = prepare_sql_drop_table_if_exists(cfg_tables['TMP_TEST_CALL_ATTRIBUTES'])
    sql_test_6b = create_call_atr_table(lb=cfg['DATE_B'], ub=cfg['DATE_C'],
                                        target_tab=cfg_tables['TMP_TEST_CALL_ATTRIBUTES'],
                                        cdr_tab=cfg['CDR_TABLE'], msisdn_table=cfg_tables['TMP_TEST_CHURN_BASE_TABLE'])
    execute_sql_query(sqlContext, sql_test_6a)
    execute_sql_query(sqlContext, sql_test_6b)
    check_table_created_and_nonempty(sqlContext, cfg_tables['TMP_TEST_CALL_ATTRIBUTES'])
    print("Joining all data together")
    # left join of attribute tables:
    # training data:
    sql_train_final = prepare_left_joined_table_sql(sqlContext, "msisdn", left_table_name=cfg_tables['TMP_TRAIN_TARGET_TABLE_2'],
                                                    right_table_name=cfg_tables[
                                                        'TMP_TRAIN_CALLCENTERS_CALLS_DURATION_TABLE'])
    sql_train_final = prepare_left_joined_table_sql(sqlContext, "msisdn", left_table_sql=sql_train_final,
                                                    right_table_name=cfg_tables['TMP_TRAIN_CALLCENTERS_CALLS_CNT_TABLE'])
    sql_train_final = prepare_left_joined_table_sql(sqlContext, "msisdn", left_table_sql=sql_train_final,
                                                    right_table_name=cfg_tables['TMP_TRAIN_COMMUNITY_ATTRIBUTES'])
    sql_train_final = prepare_left_joined_table_sql(sqlContext, "msisdn", left_table_sql=sql_train_final,
                                                    right_table_name=cfg_tables['TMP_TRAIN_CALL_ATTRIBUTES'])
    # test data:
    sql_test_final = prepare_left_joined_table_sql(sqlContext, "msisdn", left_table_name=cfg_tables['TMP_TEST_TARGET_TABLE_2'],
                                                   right_table_name=cfg_tables[
                                                       'TMP_TEST_CALLCENTERS_CALLS_DURATION_TABLE'])
    sql_test_final = prepare_left_joined_table_sql(sqlContext, "msisdn", left_table_sql=sql_test_final,
                                                   right_table_name=cfg_tables['TMP_TEST_CALLCENTERS_CALLS_CNT_TABLE'])
    sql_test_final = prepare_left_joined_table_sql(sqlContext, "msisdn", left_table_sql=sql_test_final,
                                                   right_table_name=cfg_tables['TMP_TEST_COMMUNITY_ATTRIBUTES'])
    sql_test_final = prepare_left_joined_table_sql(sqlContext, "msisdn", left_table_sql=sql_test_final,
                                                   right_table_name=cfg_tables['TMP_TEST_CALL_ATTRIBUTES'])
    sql_train_final = "CREATE TABLE " + cfg_tables['TMP_TABLE_TRAIN'] + " AS " + sql_train_final
    sql_test_final = "CREATE TABLE " + cfg_tables['TMP_TABLE_PREDICT'] + " AS " + sql_test_final
    # detele train and test tables if they exist:
    execute_sql_query(sqlContext, prepare_sql_drop_table_if_exists(cfg_tables['TMP_TABLE_TRAIN']))
    execute_sql_query(sqlContext, prepare_sql_drop_table_if_exists(cfg_tables['TMP_TABLE_PREDICT']))
    # create final train and test tables
    execute_sql_query(sqlContext, sql_train_final)
    execute_sql_query(sqlContext, sql_test_final)
    check_table_created_and_nonempty(sqlContext, cfg_tables['TMP_TABLE_TRAIN'])
    check_table_created_and_nonempty(sqlContext, cfg_tables['TMP_TABLE_PREDICT'])
    

def run(cfg, cfg_tables, logger, sqlContext):
    """
    Entry point of the 1st phase.
    :param cfg: dictionary of configuration constants.
    :param cfg_tables: dictionary of tmp table names.
    :param logger: current logger.
    :param sqlContext: current pyspark sqlContext.
    """
    try:
        run_with_exceptions_handled(cfg, cfg_tables, logger, sqlContext)
    except (TableNotCreatedError, TableEmptyError, TableDoesNotExistError) as e:
        tr = traceback.format_exc()
        logger.error(tr)
        sys.exit(-1)
        