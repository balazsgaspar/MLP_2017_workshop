from time import gmtime, strftime
from sql_query_preparator import *
from sql_utils import *
import sys
import traceback


def log(message = ""):
    print(strftime("%Y-%m-%d %H:%M:%S", gmtime()) + " " + message)


def run(cfg, cfg_tables, sqlContext):
    """
    Main computation block of the 1st phase.
    :param cfg: dictionary of configuration constants.
    :param cfg_tables: dictionary of tmp table names.
    :param sqlContext: current pyspark sqlContext.
    """
    log("Preparing data to DB")    
    
    df1 = sqlContext.read.parquet("mlp_sampled_cdr_records.parquet")
    df2 = sqlContext.read.parquet("mlp_sampled_ebr_base_20160401.parquet")
    df3 = sqlContext.read.parquet("mlp_sampled_ebr_base_20160501.parquet")
    df4 = sqlContext.read.parquet("mlp_sampled_ebr_churners_20151201_20160630.parquet")
    df5 = sqlContext.read.parquet("lpa_20160301_20160401.parquet")
    df6 = sqlContext.read.parquet("lpa_20160401_20160501.parquet")
    
    df1.createOrReplaceTempView(cfg['CDR_TABLE'])
    df2.createOrReplaceTempView(cfg['BASE_TAB_DATE_B'])
    df3.createOrReplaceTempView(cfg['BASE_TAB_DATE_C'])
    df4.createOrReplaceTempView(cfg['CHURNS_TABLE'])
    df5.createOrReplaceTempView(cfg['TRAIN_COMMUNITY_TABLE'])
    df6.createOrReplaceTempView(cfg['PREDICT_COMMUNITY_TABLE'])
    
    log("Preparing train data")
    log("Preparing churn-base table")
    sql_train_1 = prepare_sql_churn_base_data(churn_table_name=cfg['CHURNS_TABLE'],
                                               base_table_name=cfg['BASE_TAB_DATE_B'],
                                               churn_from_date=cfg['DATE_B'],
                                               churn_to_date=cfg['DATE_C'],
                                               restrict_commitment_to=cfg['RESTRICT_COMMITMENT_TO'],
                                               target_table_name=cfg_tables['TMP_TRAIN_CHURN_BASE_TABLE'])
    sql_train_1_df = execute_sql_query(sqlContext, sql_train_1)
    sql_train_1_df.createOrReplaceTempView(cfg_tables['TMP_TRAIN_CHURN_BASE_TABLE'])
    log("Preparing top callcenters table")
    sql_train_2 = prepare_sql_top_call_centers(cdr_from_date=cfg['DATE_A'], cdr_to_date=cfg['DATE_B'],
                                                cdr_table_name=cfg['CDR_TABLE'], base_table_name=cfg['BASE_TAB_DATE_B'],
                                                number_of_callcenters=cfg['COMMON_TOP_CALLCENTERS_COUNT'],
                                                target_table_name=cfg_tables['TMP_COMMON_CALLCENTERS_TABLE'])
    sql_train_2_df = execute_sql_query(sqlContext, sql_train_2)
    sql_train_2_df.createOrReplaceTempView(cfg_tables['TMP_COMMON_CALLCENTERS_TABLE'])
    
    
    log("Preparing table with first features")
    sql_train_3 = prepare_sql_first_features(cdr_table_name=cfg['CDR_TABLE'],
                                              churn_base_table_name=cfg_tables['TMP_TRAIN_CHURN_BASE_TABLE'],
                                              callcenters_table_name=cfg_tables['TMP_COMMON_CALLCENTERS_TABLE'],
                                              cdr_from_date=cfg['DATE_A'], cdr_to_date=cfg['DATE_B'],
                                              target_table_name=cfg_tables['TMP_TRAIN_TARGET_TABLE_1'],
                                              contains_churn_info=True)
    sql_train_3_df = execute_sql_query(sqlContext, sql_train_3)
    sql_train_3_df.createOrReplaceTempView(cfg_tables['TMP_TRAIN_TARGET_TABLE_1'])
    
    # this must be run after the table with callcenters is created!
    topcallcenters_df = sqlContext.sql("SELECT * FROM " + cfg_tables['TMP_COMMON_CALLCENTERS_TABLE']).toPandas()
    log("Preparing callcenters calls duration")
    sql_train_3X = prepare_sql_callcenters_calls_duration(cdr_table_name=cfg['CDR_TABLE'], cdr_from_date=cfg['DATE_A'],
                                                           cdr_to_date=cfg['DATE_B'],
                                                           callcenters_table_name=cfg_tables['TMP_COMMON_CALLCENTERS_TABLE'],
                                                           callcenters_df=topcallcenters_df,
                                                           target_table_name=cfg_tables['TMP_TRAIN_CALLCENTERS_CALLS_DURATION_TABLE'])
    sql_train_3X_df = execute_sql_query(sqlContext, sql_train_3X)
    sql_train_3X_df.createOrReplaceTempView(cfg_tables['TMP_TRAIN_CALLCENTERS_CALLS_DURATION_TABLE'])
    
    log("Preparing callcenters calls counts")
    sql_train_3Y = prepare_sql_callcenters_calls_cnt(cdr_table_name=cfg['CDR_TABLE'], cdr_from_date=cfg['DATE_A'],
                                                      cdr_to_date=cfg['DATE_B'],
                                                      callcenters_table_name=cfg_tables['TMP_COMMON_CALLCENTERS_TABLE'],
                                                      callcenters_df=topcallcenters_df,
                                                      target_table_name=cfg_tables['TMP_TRAIN_CALLCENTERS_CALLS_CNT_TABLE'])
    sql_train_3Y_df = execute_sql_query(sqlContext, sql_train_3Y)
    sql_train_3Y_df.createOrReplaceTempView(cfg_tables['TMP_TRAIN_CALLCENTERS_CALLS_CNT_TABLE'])
    
    log("Preparing table with more features")
    sql_train_4 = prepare_sql_second_features(base_table_name=cfg['BASE_TAB_DATE_B'], cdr_table_name=cfg['CDR_TABLE'],
                                               cdr_from_date=cfg['DATE_A'], cdr_to_date=cfg['DATE_B'],
                                               previous_table_name=cfg_tables['TMP_TRAIN_TARGET_TABLE_1'],
                                               target_table_name=cfg_tables['TMP_TRAIN_TARGET_TABLE_2'])
    sql_train_4_df = execute_sql_query(sqlContext, sql_train_4)
    sql_train_4_df.createOrReplaceTempView(cfg_tables['TMP_TRAIN_TARGET_TABLE_2'])
    
    log("Preparing community features")
    sql_train_5 = prepare_sql_community_attributes(base_table_name=cfg['BASE_TAB_DATE_B'],
                                                    community_table_name=cfg['TRAIN_COMMUNITY_TABLE'],
                                                    target_table_name=cfg_tables['TMP_TRAIN_COMMUNITY_ATTRIBUTES'],
                                                    churn_table_name=cfg['CHURNS_TABLE'],
                                                    cdr_to_date=cfg['DATE_B'])
    sql_train_5_df = execute_sql_query(sqlContext, sql_train_5)
    sql_train_5_df.createOrReplaceTempView(cfg_tables['TMP_TRAIN_COMMUNITY_ATTRIBUTES'])
    
    log("Preparing call features")
    sql_train_6 = create_call_atr_table(lb=cfg['DATE_A'], ub=cfg['DATE_B'],
                                         target_tab=cfg_tables['TMP_TRAIN_CALL_ATTRIBUTES'],
                                         cdr_tab=cfg['CDR_TABLE'], msisdn_table=cfg_tables['TMP_TRAIN_CHURN_BASE_TABLE'])
    sql_train_6_df = execute_sql_query(sqlContext, sql_train_6)
    sql_train_6_df.createOrReplaceTempView(cfg_tables['TMP_TRAIN_CALL_ATTRIBUTES'])
    
    log("Preparing predict data")
    log("Preparing churn-base table")
    sql_test_1 = prepare_sql_churn_base_data(churn_table_name=cfg['CHURNS_TABLE'], base_table_name=cfg['BASE_TAB_DATE_C'],
                                              churn_from_date=cfg['DATE_C'],
                                              churn_to_date=cfg['DATE_D'],
                                              restrict_commitment_to=cfg['RESTRICT_COMMITMENT_TO'],
                                              target_table_name=cfg_tables['TMP_TEST_CHURN_BASE_TABLE'])
    sql_test_1_df = execute_sql_query(sqlContext, sql_test_1)
    sql_test_1_df.createOrReplaceTempView(cfg_tables['TMP_TEST_CHURN_BASE_TABLE'])
    
    
    log("Preparing table with first features")
    sql_test_3 = prepare_sql_first_features(cdr_table_name=cfg['CDR_TABLE'],
                                             churn_base_table_name=cfg_tables['TMP_TEST_CHURN_BASE_TABLE'],
                                             callcenters_table_name=cfg_tables['TMP_COMMON_CALLCENTERS_TABLE'],
                                             cdr_from_date=cfg['DATE_B'],
                                             cdr_to_date=cfg['DATE_C'],
                                             target_table_name=cfg_tables['TMP_TEST_TARGET_TABLE_1'],
                                             contains_churn_info=True)
    sql_test_3_df = execute_sql_query(sqlContext, sql_test_3)
    sql_test_3_df.createOrReplaceTempView(cfg_tables['TMP_TEST_TARGET_TABLE_1'])
    
    
    log("Preparing callcenters calls duration")
    sql_test_3X = prepare_sql_callcenters_calls_duration(cdr_table_name=cfg['CDR_TABLE'], cdr_from_date=cfg['DATE_B'],
                                                          cdr_to_date=cfg['DATE_C'],
                                                          callcenters_table_name=cfg_tables['TMP_COMMON_CALLCENTERS_TABLE'],
                                                          callcenters_df=topcallcenters_df,
                                                          target_table_name=cfg_tables['TMP_TEST_CALLCENTERS_CALLS_DURATION_TABLE'])
    sql_test_3X_df = execute_sql_query(sqlContext, sql_test_3X)
    sql_test_3X_df.createOrReplaceTempView(cfg_tables['TMP_TEST_CALLCENTERS_CALLS_DURATION_TABLE'])
    
    log("Preparing callcenters calls counts")
    sql_test_3Y = prepare_sql_callcenters_calls_cnt(cdr_table_name=cfg['CDR_TABLE'], cdr_from_date=cfg['DATE_B'],
                                                     cdr_to_date=cfg['DATE_C'],
                                                     callcenters_table_name=cfg_tables['TMP_COMMON_CALLCENTERS_TABLE'],
                                                     callcenters_df=topcallcenters_df,
                                                     target_table_name=cfg_tables['TMP_TEST_CALLCENTERS_CALLS_CNT_TABLE'])
    sql_test_3Y_df = execute_sql_query(sqlContext, sql_test_3Y)
    sql_test_3Y_df.createOrReplaceTempView(cfg_tables['TMP_TEST_CALLCENTERS_CALLS_CNT_TABLE'])
    
    log("Preparing table with more features")
    sql_test_4 = prepare_sql_second_features(base_table_name=cfg['BASE_TAB_DATE_C'], cdr_table_name=cfg['CDR_TABLE'],
                                              cdr_from_date=cfg['DATE_B'], cdr_to_date=cfg['DATE_C'],
                                              previous_table_name=cfg_tables['TMP_TEST_TARGET_TABLE_1'],
                                              target_table_name=cfg_tables['TMP_TEST_TARGET_TABLE_2'])
    sql_test_4_df = execute_sql_query(sqlContext, sql_test_4)
    sql_test_4_df.createOrReplaceTempView(cfg_tables['TMP_TEST_TARGET_TABLE_2'])
    
    log("Preparing community features")
    sql_test_5 = prepare_sql_community_attributes(base_table_name=cfg['BASE_TAB_DATE_C'],
                                                   community_table_name=cfg['PREDICT_COMMUNITY_TABLE'],
                                                   target_table_name=cfg_tables['TMP_TEST_COMMUNITY_ATTRIBUTES'],
                                                   churn_table_name=cfg['CHURNS_TABLE'], cdr_to_date=cfg['DATE_C'])
    sql_test_5_df = execute_sql_query(sqlContext, sql_test_5)
    sql_test_5_df.createOrReplaceTempView(cfg_tables['TMP_TEST_COMMUNITY_ATTRIBUTES'])
    
    log("Preparing call features")
    sql_test_6 = create_call_atr_table(lb=cfg['DATE_B'], ub=cfg['DATE_C'],
                                        target_tab=cfg_tables['TMP_TEST_CALL_ATTRIBUTES'],
                                        cdr_tab=cfg['CDR_TABLE'], msisdn_table=cfg_tables['TMP_TEST_CHURN_BASE_TABLE'])
    sql_test_6_df = execute_sql_query(sqlContext, sql_test_6)
    sql_test_6_df.createOrReplaceTempView(cfg_tables['TMP_TEST_CALL_ATTRIBUTES'])
    
    log("Joining all data together and saving to parquet")
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
    # create final train and test tables
    sql_train_final_df = execute_sql_query(sqlContext, sql_train_final)
    sql_test_final_df = execute_sql_query(sqlContext, sql_test_final)
    
    sql_train_final_df.write.parquet(cfg_tables['TMP_TABLE_TRAIN'], mode='overwrite')
    sql_test_final_df.write.parquet(cfg_tables['TMP_TABLE_PREDICT'], mode='overwrite')
    
    log("Phase 1 DONE")

    
        
