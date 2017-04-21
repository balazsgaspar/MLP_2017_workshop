from sql_utils import generate_pivot_table_sql


# PART 1 (TRAIN AND TEST: churn/base tables)
def prepare_sql_churn_base_data(churn_table_name, base_table_name, churn_from_date, churn_to_date,
                                restrict_commitment_to, target_table_name):
    # select relevant attributes from base table + possibly from churners table (if it is used for training)
    sql_select_basic_attributes = """
        SELECT
                tb.msisdn,
                """ + ("MAX(tc.date_key) as churn_date," if churn_table_name is not None else "") + """
                MAX(tb.commitment_to_key) as commitment_to_date,
                MAX(tb.commitment_from_key) as commitment_from_date,
                MAX(tb.customer_type) as customer_type,
                MAX(tb.rateplan_group) as rateplan_group,
                MAX(tb.rateplan_name) as rateplan_name
        FROM
        """ + base_table_name + """ tb
    """
    if churn_table_name is not None:
        sql_select_basic_attributes += """
            LEFT JOIN
            """ + churn_table_name + """ tc
            ON tb.msisdn = tc.msisdn
            WHERE tb.customer_type = 'PRIVATE' AND (tc.date_key IS NULL OR tc.date_key >= """ + churn_from_date + """)
            GROUP BY tb.msisdn
        """
    else:
        sql_select_basic_attributes += """
            WHERE tb.customer_type = 'PRIVATE'
            GROUP BY tb.msisdn
        """

    # recompute some new attributes from the previous one:
    if churn_table_name is not None:
        churned_str = """
            churn_date,
            COALESCE(t.churn_date >= """ + churn_from_date + """ AND t.churn_date < """ + churn_to_date + """, false) as churned,
        """
    else:
        churned_str = ""
    sql_all = """
    SELECT SUBSTR(t.msisdn, 2, 12) as msisdn,
        """ + churned_str + """
        commitment_to_date != 0 AS committed,
        COALESCE(datediff(from_unixtime(unix_timestamp(commitment_to_date, "yyyyMMdd")),
                          from_unixtime(unix_timestamp(commitment_from_date, "yyyyMMdd"))), 0) AS committed_days,
        datediff(from_unixtime(unix_timestamp(commitment_to_date, "yyyyMMdd")),
                          from_unixtime(unix_timestamp('""" + churn_from_date + """', "yyyyMMdd"))) AS commitment_remaining,
        customer_type,
        rateplan_group,
        rateplan_name
    FROM
    (
        """ + sql_select_basic_attributes + """
    ) t
    """

    if restrict_commitment_to:
        sql_all = """
        SELECT *
        FROM
        (
        """ + sql_all + """
        ) t2
        WHERE commitment_remaining IS NULL OR commitment_remaining < 90
        """
    return sql_all



# PART 2 (TRAIN stage only: prepare top callcenters)
# i.e. the top N numbers with respect to the numbers called
def prepare_sql_top_call_centers(cdr_from_date, cdr_to_date, cdr_table_name, base_table_name,
                                 number_of_callcenters, target_table_name):
    # MSISDN number of non-stk customers:
    sql_nonstk_msisdn = """
                SELECT DISTINCT calls_nums.tomsisdn AS msisdn
                FROM
                (
                    SELECT DISTINCT tomsisdn
                    FROM """ + cdr_table_name + """
                    WHERE date_key >= """ + cdr_from_date + """ AND date_key < """ + cdr_to_date + """
                ) calls_nums
                LEFT JOIN
                (
                    SELECT DISTINCT SUBSTR(msisdn, 2, 12) AS msisdn
                    FROM """ + base_table_name + """
                ) telecom_nums
                ON calls_nums.tomsisdn=telecom_nums.msisdn WHERE telecom_nums.msisdn IS NULL
    """
    # Calls from those MSISDN numbers
    sql_nonstk_calls = """
            SELECT calls_tab.tomsisdn, calls_tab.frommsisdn
            FROM
            (
                SELECT tomsisdn, frommsisdn, duration
                FROM """ + cdr_table_name + """
                WHERE record_type = 'mSTerminating'
                      AND date_key >= """ + cdr_from_date + """ AND date_key < """ + cdr_to_date + """
            ) calls_tab
            JOIN
            (
                """ + sql_nonstk_msisdn + """
            ) non_tel_nums
            ON calls_tab.tomsisdn = non_tel_nums.msisdn GROUP BY calls_tab.tomsisdn, calls_tab.frommsisdn
    
    
    """
    # Create a table countig those calls for each non-stk msisdn
    sql_create_nonstk_call_counts = """
        SELECT sel.tomsisdn AS msisdn, COUNT(sel.tomsisdn) as count_calls
        FROM
        (
            """ + sql_nonstk_calls + """
        )
        sel WHERE tomsisdn>0
        GROUP BY tomsisdn
        ORDER BY count_calls DESC
        LIMIT """ + str(number_of_callcenters) + """
        
    """
    return sql_create_nonstk_call_counts


# PART 3 (TRAIN and TEST)
def prepare_sql_first_features(cdr_table_name, churn_base_table_name, callcenters_table_name,
                               cdr_from_date, cdr_to_date, target_table_name, contains_churn_info):
    sql_select_terminating_calls = """
            SELECT tomsisdn, frommsisdn, duration, date_key
            FROM """ + cdr_table_name + """
            WHERE record_type='mSTerminating'
                  AND date_key >= """ + cdr_from_date + """ AND date_key < """ + cdr_to_date + """
    """
    sql_select_calls_from_datacenters = """
        SELECT calls.frommsisdn,
               SUM(calls.duration) AS calls_duration, COUNT(calls.duration) AS calls_count
        FROM
        (
        """ + sql_select_terminating_calls + """
        ) calls
        JOIN
        """ + callcenters_table_name + """ callcenters
        ON callcenters.msisdn=calls.tomsisdn
        GROUP BY calls.frommsisdn
    """
    sql_base_data_with_calls = """
    SELECT 
        chtab.msisdn, 
        chtab.customer_type,
        chtab.rateplan_group,
        chtab.rateplan_name,
        """ + ("chtab.churned," if contains_churn_info else "") + """
        chtab.committed,
        chtab.committed_days,
        chtab.commitment_remaining,
        COALESCE(calls_data.calls_count, 0) as callcenter_calls_count,
        COALESCE(calls_data.calls_duration, 0) as callcenter_calls_duration
    FROM """ + churn_base_table_name + """ chtab
    LEFT JOIN
    (
    """ + sql_select_calls_from_datacenters + """
    ) calls_data
    ON chtab.msisdn=calls_data.frommsisdn                
    """
    return sql_base_data_with_calls



# PART 3 - B (TRAIN and TEST)
def prepare_sql_callcenters_calls_duration(cdr_table_name, cdr_from_date, cdr_to_date,
                                           callcenters_table_name, callcenters_df, target_table_name):
    SQL_callcenters_calls_duration = """
            SELECT calls.frommsisdn AS msisdn, calls.tomsisdn,
            SUM(calls.duration) AS calls_duration
            FROM
            (
                SELECT tomsisdn, frommsisdn, duration, date_key
                FROM """ + cdr_table_name + """
                WHERE record_type='mSTerminating'
                      AND date_key >= """ + cdr_from_date + """ AND date_key < """ + cdr_to_date + """
            ) calls
            JOIN
            """ + callcenters_table_name + """  callcenters
            ON callcenters.msisdn=calls.tomsisdn
            GROUP BY calls.frommsisdn, calls.tomsisdn
    """
    sql_calls_duration = generate_pivot_table_sql("msisdn", "tomsisdn", "calls_duration",
                                                  callcenters_df['msisdn'].values, "cc_dur_",
                                                  table_sql=SQL_callcenters_calls_duration)
    return sql_calls_duration


def prepare_sql_callcenters_calls_cnt(cdr_table_name, cdr_from_date, cdr_to_date,
                                      callcenters_table_name, callcenters_df, target_table_name):
    sql_callcenters_calls_cnt = """
            SELECT calls.frommsisdn AS msisdn, calls.tomsisdn,
            COUNT(calls.duration) AS calls_count
            FROM
            (
                SELECT tomsisdn, frommsisdn, duration, date_key
                FROM """ + cdr_table_name + """
                WHERE record_type='mSTerminating'
                      AND date_key >= """ + cdr_from_date + """ AND date_key < """ + cdr_to_date + """
            ) calls
            JOIN
            """ + callcenters_table_name + """  callcenters
            ON callcenters.msisdn=calls.tomsisdn
            GROUP BY calls.frommsisdn, calls.tomsisdn
    """
    sql_callcenters_calls_cnt = generate_pivot_table_sql("msisdn", "tomsisdn", "calls_count",
                                                         callcenters_df['msisdn'].values, "cc_cnt_",
                                                         table_sql=sql_callcenters_calls_cnt)
    return sql_callcenters_calls_cnt




# PART 4 (TRAIN AND TEST)
def prepare_sql_second_features(base_table_name, cdr_table_name, cdr_from_date, cdr_to_date,
                                previous_table_name, target_table_name):
    # These should be distinct already:
    sql_select_telecom_msisdn = """
                        SELECT SUBSTR(msisdn, 2, 12) as msisdn
                        FROM """ + base_table_name + """
    """
    sql_select_non_telecom_msisdn = """
                    SELECT DISTINCT calls_nums.tomsisdn as msisdn
                    FROM
                    (

                        SELECT DISTINCT tomsisdn
                        FROM """ + cdr_table_name + """
                        WHERE date_key >= """ + cdr_from_date + """ AND date_key < """ + cdr_to_date + """

                    ) calls_nums
                    LEFT JOIN
                    (
                    """ + sql_select_telecom_msisdn + """
                    ) telecom_nums
                    ON calls_nums.tomsisdn=telecom_nums.msisdn
                    WHERE telecom_nums.msisdn IS NULL
    """
    sql_select_all_calls = """
                    SELECT tomsisdn, frommsisdn, duration
                    FROM """ + cdr_table_name + """
                    WHERE date_key >= """ + cdr_from_date + """ AND date_key < """ + cdr_to_date + """ 
    """
    sql_select_all_calls_with_non_telecom = """
                SELECT all_calls.frommsisdn, all_calls.tomsisdn, all_calls.duration 
                FROM
                (
                """ + sql_select_all_calls + """
                ) all_calls
                JOIN
                (
                """ + sql_select_non_telecom_msisdn + """
                ) nonstk_list
                ON nonstk_list.msisdn = all_calls.tomsisdn
    """
    sql_group_calls_non_tel = """
            SELECT sl.frommsisdn as msisdn, SUM(sl.duration) as calls_non_t_dur, COUNT(sl.frommsisdn) as calls_non_t_cnt
            FROM
            (
            """ + sql_select_all_calls_with_non_telecom + """
            ) sl
            GROUP BY sl.frommsisdn
    """
    sql_group_all_calls = """
            SELECT acl.frommsisdn as msisdn, SUM(acl.duration) as calls_all_dur, COUNT(acl.frommsisdn) as calls_all_cnt
            FROM
            (
            """ + sql_select_all_calls + """
            ) acl
            GROUP BY acl.frommsisdn
    """
    sql_select_calls_data = """
        SELECT  ntd.msisdn, ntd.calls_non_t_dur, ntd.calls_non_t_cnt, ad.calls_all_dur, ad.calls_all_cnt
        FROM
        (
        """ + sql_group_all_calls + """
        ) ad
        LEFT JOIN
        (
        """ + sql_group_calls_non_tel + """
        ) ntd 
        ON ad.msisdn=ntd.msisdn
    """
    select_training_data = """
    SELECT tr.*,
        COALESCE(cld.calls_non_t_dur,0) as calls_non_t_dur,
        COALESCE(cld.calls_non_t_cnt,0) as calls_non_t_cnt,
        COALESCE(cld.calls_all_dur,0) as calls_all_dur,
        COALESCE(cld.calls_all_cnt,0) as calls_all_cnt
    FROM """ + previous_table_name + """ tr 
    LEFT JOIN 
    (
    """ + sql_select_calls_data + """
    ) cld
    ON cld.msisdn=tr.msisdn
    """
    return select_training_data

  


def prepare_sql_community_attributes(base_table_name, community_table_name, target_table_name,
                                     churn_table_name, cdr_to_date):
    # These should be distinct already:
    sql_select_telecom_msisdn = """
            SELECT SUBSTR(msisdn, 2, 12) as msisdn
            FROM """ + base_table_name + """
    """
    sql_select_churners_in_communities = """
                SELECT tx.label, SUM(tx.churned) AS churned_cnt, SUM(tx.leader_churned) AS leader_churned_cnt FROM
                (
                    SELECT t1.*,
                        (CASE WHEN t2.msisdn IS NOT NULL THEN 1 ELSE 0 END) AS churned,
                        (CASE WHEN t1.group_leader = true AND t2.msisdn IS NOT NULL THEN 1 ELSE 0 END) AS leader_churned
                    FROM """ + community_table_name + """ t1
                    LEFT JOIN 
                    (
                    SELECT SUBSTR(msisdn, 2,14) AS msisdn FROM """ + churn_table_name + """
                    WHERE date_key < """ + cdr_to_date + """
                    ) t2
                    ON t1.id = t2.msisdn
                ) tx
                GROUP BY tx.label
    """
    sql_all = """
        SELECT
            t_left.msisdn,
            t_right.degree AS com_degree,
            t_right.degree_total AS com_degree_total,
            t_right.count_in_group AS com_count_in_group,
            t_right.degree_in_group AS com_degree_in_group,
            t_right.score AS com_score,
            t_right.group_leader AS com_group_leader,
            t_right.group_follower AS com_group_follower,
            t_right.churned_cnt AS com_churned_cnt,
            t_right.leader_churned_cnt AS com_leader_churned_cnt
        FROM
        (
        """ + sql_select_telecom_msisdn + """
        ) t_left 
        LEFT JOIN
        (
            SELECT t1.*, t2.churned_cnt, t2.leader_churned_cnt FROM
            """ + community_table_name + """ t1
            LEFT JOIN
            (
            """ + sql_select_churners_in_communities + """
            ) t2
            ON t1.label = t2.label
            
        
        )
        t_right
        ON t_left.msisdn = t_right.id
    """
    return sql_all



def create_call_atr_table(lb, ub, target_tab, cdr_tab, msisdn_table):
    select_all_calls = """
        SELECT date_key,record_type, frommsisdn, tomsisdn, duration
        FROM """ + cdr_tab + """
        WHERE frommsisdn!='' AND tomsisdn!='' AND date_key >='""" + lb + """' AND date_key<'""" + ub + """'"""

    select_all_calls_under_5s = select_all_calls + """ AND duration<6"""
    select_all_telecom_msisdn = """
    SELECT DISTINCT frommsisdn as msisdn
    FROM """ + cdr_tab + """
    WHERE frommsisdn!='' AND date_key >='""" + lb + """' AND date_key<'""" + ub + """'"""
    select_all_non_telecom_msisdn = """SELECT DISTINCT ac3.tomsisdn as msisdn FROM
                                    (""" + select_all_calls + """) ac3
                                    LEFT JOIN 
                                    (""" + select_all_telecom_msisdn + """) snt3
                                    ON ac3.tomsisdn=snt3.msisdn WHERE snt3.msisdn IS NULL"""
    select_t_calls = """SELECT
                        ac1.record_type,
                        ac1.date_key,
                        ac1.frommsisdn,
                        ac1.tomsisdn,
                        ac1.duration
                        FROM (""" + select_all_calls + """) ac1 JOIN (""" + select_all_telecom_msisdn + """) snt1
                        ON ac1.tomsisdn=snt1.msisdn 
                        """
    select_non_t_calls = """SELECT
                        ac2.record_type,
                        ac2.date_key,
                        ac2.frommsisdn,
                        ac2.tomsisdn,
                        ac2.duration
                        FROM (""" + select_all_calls + """) ac2 JOIN (""" + select_all_non_telecom_msisdn + """) nsnt2
                        ON ac2.tomsisdn=nsnt2.msisdn
                        """

    select_all_incoming_calls = select_all_calls + " AND record_type='mSTerminating'"
    select_all_outgoing_calls = select_all_calls + " AND record_type='mSOriginating'"

    select_incoming_calls_t = """SELECT inc_cls5.record_type,
                                      inc_cls5.date_key,
                                      inc_cls5.frommsisdn,
                                      inc_cls5.tomsisdn,
                                      inc_cls5.duration 
                                      FROM 
                                (""" + select_all_incoming_calls + """)
                                inc_cls5 JOIN 
                                (""" + select_all_telecom_msisdn + """)
                                tems5
                                ON inc_cls5.tomsisdn=tems5.msisdn
                                """

    select_incoming_calls_non_t = """SELECT inc_cls6.record_type,
                                      inc_cls6.date_key,
                                      inc_cls6.frommsisdn,
                                      inc_cls6.tomsisdn,
                                      inc_cls6.duration 
                                      FROM 
                                (""" + select_all_incoming_calls + """)
                                inc_cls6 JOIN 
                                (""" + select_all_non_telecom_msisdn + """)
                                ntems6
                                ON inc_cls6.tomsisdn=ntems6.msisdn
                                """
    
    
    select_outgoing_calls_t = """SELECT ot_cls7.record_type,
                                      ot_cls7.date_key,
                                      ot_cls7.frommsisdn,
                                      ot_cls7.tomsisdn,
                                      ot_cls7.duration
                                      FROM 
                                (""" + select_all_outgoing_calls + """)
                                ot_cls7 JOIN 
                                (""" + select_all_telecom_msisdn + """)
                                tems7
                                ON ot_cls7.tomsisdn=tems7.msisdn
                                """

    select_outgoing_calls_non_t = """SELECT ot_cls8.record_type,
                                      ot_cls8.date_key,
                                      ot_cls8.frommsisdn,
                                      ot_cls8.tomsisdn,
                                      ot_cls8.duration
                                      FROM 
                                (""" + select_all_outgoing_calls + """)
                                ot_cls8 JOIN 
                                (""" + select_all_non_telecom_msisdn + """)
                                tems8
                                ON ot_cls8.tomsisdn=tems8.msisdn
                                """

    attrs_select_all_incoming_calls = """SELECT
                              sel9.frommsisdn as msisdn,
                              COUNT(sel9.frommsisdn) as cnt_incoming_calls_all,
                              SUM(sel9.duration) as dur_incoming_calls_all,
                              AVG(sel9.duration) as avg_dur_incoming_calls_all,
                              STDDEV(sel9.duration) as std_dur_incoming_calls_all
                              FROM (""" + select_all_incoming_calls + """) sel9
                              GROUP BY sel9.frommsisdn"""

    attrs_select_all_outgoing_calls = """SELECT
                          sel10.frommsisdn as msisdn,
                          COUNT(sel10.frommsisdn) as cnt_outgoing_calls_all,
                          SUM(sel10.duration) as dur_outgoing_calls_all,
                          AVG(sel10.duration) as avg_dur_outgoing_calls_all,
                          STDDEV(sel10.duration) as std_dur_outgoing_calls_all
                          FROM (""" + select_all_outgoing_calls + """) sel10
                          GROUP BY sel10.frommsisdn"""

    attrs_select_t_calls = """SELECT
                          sel11.frommsisdn as msisdn,
                          COUNT(sel11.frommsisdn) as cnt_t_calls,
                          SUM(sel11.duration) as dur_t_calls,
                          AVG(sel11.duration) as avg_dur_t_calls,
                          STDDEV(sel11.duration) as std_dur_t_calls
                          FROM (""" + select_t_calls + """) sel11
                          GROUP BY sel11.frommsisdn"""

    attrs_select_non_t_calls = """SELECT
                          sel12.frommsisdn as msisdn,
                          COUNT(sel12.frommsisdn) as cnt_non_t_calls,
                          SUM(sel12.duration) as dur_non_t_calls,
                          AVG(sel12.duration) as avg_dur_non_t_calls,
                          STDDEV(sel12.duration) as std_dur_non_t_calls
                          FROM (""" + select_non_t_calls + """) sel12
                          GROUP BY sel12.frommsisdn"""

    
    attrs_select_incoming_calls_t = """SELECT
                          sel13.frommsisdn as msisdn,
                          COUNT(sel13.frommsisdn) as cnt_incoming_calls_t,
                          SUM(sel13.duration) as dur_incoming_calls_t,
                          AVG(sel13.duration) as avg_dur_incoming_calls_t,
                          STDDEV(sel13.duration) as std_dur_incoming_calls_t
                          FROM (""" + select_incoming_calls_t + """) sel13
                          GROUP BY sel13.frommsisdn"""

    attrs_select_incoming_calls_non_t = """SELECT
                          sel14.frommsisdn as msisdn,
                          COUNT(sel14.frommsisdn) as cnt_incoming_calls_non_t,
                          SUM(sel14.duration) as dur_incoming_calls_non_t,
                          AVG(sel14.duration) as avg_dur_incoming_calls_non_t,
                          STDDEV(sel14.duration) as std_dur_incoming_calls_non_t
                          FROM (""" + select_incoming_calls_non_t + """) sel14
                          GROUP BY sel14.frommsisdn"""

    attrs_select_outgoing_calls_t = """SELECT
                          sel15.frommsisdn as msisdn,
                          COUNT(sel15.frommsisdn) as cnt_outgoing_calls_t,
                          SUM(sel15.duration) as dur_outgoing_calls_t,
                          AVG(sel15.duration) as avg_dur_outgoing_calls_t,
                          STDDEV(sel15.duration) as std_dur_outgoing_calls_t
                          FROM (""" + select_outgoing_calls_t + """) sel15
                          GROUP BY sel15.frommsisdn"""

    attrs_select_outgoing_calls_non_t = """SELECT
                          sel16.frommsisdn as msisdn,
                          COUNT(sel16.frommsisdn) as cnt_outgoing_calls_non_t,
                          SUM(sel16.duration) as dur_outgoing_calls_non_t,
                          AVG(sel16.duration) as avg_dur_outgoing_calls_non_t,
                          STDDEV(sel16.duration) as std_dur_outgoing_calls_non_t
                          FROM (""" + select_outgoing_calls_non_t + """) sel16
                          GROUP BY sel16.frommsisdn"""

    cnt_calls_under_5s = """SELECT sel17.frommsisdn as msisdn,
                              COUNT(sel17.frommsisdn) as cnt_calls_under_5s FROM
                              (""" + select_all_calls_under_5s + """) sel17
                              GROUP BY sel17.frommsisdn
                              """

    attrs_all_calls = """SELECT
                          sel19.frommsisdn as msisdn,
                          COUNT(sel19.frommsisdn) as cnt_calls_all,
                          SUM(sel19.duration) as dur_calls_all,
                          AVG(sel19.duration) as avg_dur_calls_all,
                          STDDEV(sel19.duration) as std_dur_calls_all
                          FROM (""" + select_all_calls + """) sel19
                          GROUP BY sel19.frommsisdn"""
    
    join_type = " LEFT JOIN "
    big_join = """
    SELECT
    msisdn_tab.msisdn,
    a.cnt_incoming_calls_all,
    a.dur_incoming_calls_all,
    a.avg_dur_incoming_calls_all,
    a.std_dur_incoming_calls_all,
    b.cnt_outgoing_calls_all,
    b.dur_outgoing_calls_all,
    b.avg_dur_outgoing_calls_all,
    b.std_dur_outgoing_calls_all,
    c.cnt_t_calls,
    c.dur_t_calls,
    c.avg_dur_t_calls,
    c.std_dur_t_calls,
    d.cnt_non_t_calls,
    d.dur_non_t_calls,
    d.avg_dur_non_t_calls,
    d.std_dur_non_t_calls,
    e.cnt_incoming_calls_t,
    e.dur_incoming_calls_t,
    e.avg_dur_incoming_calls_t,
    e.std_dur_incoming_calls_t,
    f.cnt_incoming_calls_non_t,
    f.dur_incoming_calls_non_t,
    f.avg_dur_incoming_calls_non_t,
    f.std_dur_incoming_calls_non_t,
    g.cnt_outgoing_calls_t,
    g.dur_outgoing_calls_t,
    g.avg_dur_outgoing_calls_t,
    g.std_dur_outgoing_calls_t,
    h.cnt_outgoing_calls_non_t,
    h.dur_outgoing_calls_non_t,
    h.avg_dur_outgoing_calls_non_t,
    h.std_dur_outgoing_calls_non_t,
    j.cnt_calls_all,
    j.dur_calls_all,
    j.avg_dur_calls_all,
    j.std_dur_calls_all,
    i.cnt_calls_under_5s
    FROM  
    """ + msisdn_table + """ msisdn_tab
    """ + join_type + """ 
    (""" + attrs_select_all_incoming_calls + """) a ON (msisdn_tab.msisdn=a.msisdn)
    """ + join_type + """
    (""" + attrs_select_all_outgoing_calls + """) b ON (msisdn_tab.msisdn=b.msisdn)
    """ + join_type + """
    (""" + attrs_select_t_calls + """) c ON (msisdn_tab.msisdn=c.msisdn)
    """ + join_type + """
    (""" + attrs_select_non_t_calls + """) d ON (msisdn_tab.msisdn=d.msisdn)
    """ + join_type + """
    (""" + attrs_select_incoming_calls_t + """) e ON (msisdn_tab.msisdn=e.msisdn)
    """ + join_type + """
    (""" + attrs_select_incoming_calls_non_t + """) f ON (msisdn_tab.msisdn=f.msisdn)
    """ + join_type + """
    (""" + attrs_select_outgoing_calls_t + """) g ON (msisdn_tab.msisdn=g.msisdn)
    """ + join_type + """
    (""" + attrs_select_outgoing_calls_non_t + """) h ON (msisdn_tab.msisdn=h.msisdn)
    """ + join_type + """
    (""" + cnt_calls_under_5s + """) i ON (msisdn_tab.msisdn=i.msisdn)
    """ + join_type + """
    (""" + attrs_all_calls + """) j ON (msisdn_tab.msisdn=j.msisdn)"""

    return big_join
  
