import re


def generate_pivot_table_sql(id_column, key_column, value_column, key_values, prefix_of_new_columns,
                             table_name=None, table_sql=None):
    """
    Generates an SQL query of pivot table.
    It takes a table and for each id in the id_column returns one record,
    key_values will be used as new columns in which the value_column values will be distributed
    according to the key_column.
    Either table name or table sql query string must be provided.
    :param id_column: string with the column name that contains row identifiers.
    :param key_column: string with the column name that contains column identifiers.
    :param value_column: string with the column name that contains the values.
    :param key_values: list or numpy array of key_column values that should be used as column identifiers.
    :param prefix_of_new_columns: string that should be used as a prefix for column names.
    :param table_name: string with the name of the original table.
    :param table_sql: sql query string of the original table.
    :raises ValueError: if both the table name and table sql string are not specified.
    :return: sql query string for creation of the pivot table.
    """
    sql = """
        SELECT """ + id_column
    for v in key_values:
        sql += """,
            max(CASE WHEN """ + key_column + """ = '""" + str(v) + """' THEN """ + value_column + """ END) AS """ + prefix_of_new_columns + str(v)
    if table_name is not None:
        sql += """
        FROM """ + table_name + """
        GROUP BY """ + id_column + """
        """
    elif table_sql is not None:
        sql += """
        FROM
        (
        """ + table_sql + """
        ) t
        GROUP BY """ + id_column + """
        """
    else:
        raise ValueError("table_name or table_sql parameter must be provided")
    return sql


def execute_sql_query(sqlContext, query_string):
    """
    Executes the SQL query.
    :param sqlContext: current SQL Context.
    :param query_string: string of sql query to be executed.
    :return: the result of the query.
    """
    return sqlContext.sql(query_string)


def prepare_left_joined_table_sql(sqlContext, join_on, left_table_name=None, right_table_name=None,
                                  left_table_sql=None, right_table_sql=None):
    """
    Prepares sql query string of left join. It omits the 'join_on' column of the right table.
    For both the left and right table, either the table name or an sql string must be provided.
    Important: this function queries the database in order to get the column names of the right table.
    :param sqlContext: current SQL Context.
    :param join_on: string with column name that should be used for joining.
    :param left_table_name: string with the name of the left table.
    :param right_table_name: string with the name of the right table.
    :param left_table_sql: sql query string of the left table.
    :param right_table_sql: sql query string of the right table.
    :return: sql query string of the left join.
    """
    sql_right_col_names = "describe " + right_table_name
    right_col_names = sqlContext.sql(sql_right_col_names).toPandas()
    right_col_names = right_col_names.loc[right_col_names['col_name'] != join_on, 'col_name'].values
    
    final_sql = """
        SELECT left_t.*"""
    for col_name in right_col_names:
        final_sql += """,
            right_t.""" + col_name
    if left_table_name is not None:
        final_sql += """
        FROM """ + left_table_name + """ AS left_t """
    else:
        final_sql += """
        FROM
        (
        """ + left_table_sql + """
        ) AS left_t """
    if right_table_name is not None:
        final_sql += """
        LEFT JOIN """ + right_table_name + """ AS right_t """
    else:
        final_sql += """
        LEFT JOIN
        (
        """ + right_table_sql + """
        ) AS right_t """
    final_sql += """
        ON left_t.""" + join_on + """ = right_t.""" + join_on + """
    """
    return final_sql


def does_table_exist(sqlContext, table_name):
    """
    Checks whether the given table exists in the Hive metastore.
    :param sqlContext: current SQL Context.
    :param table_name: full table name, i.e. <db_name>.<table_name>.
    :return: True if the table exists, False otherwise.
    """
    db_name = re.sub(r"\..+", "", table_name)
    table_name = re.sub(r".+\.", "", table_name)
    if len(sqlContext.sql("SHOW TABLES in " + db_name).where("tableName = '" + table_name + "'").collect()) == 1:
        return True
    else:
        return False


def get_number_of_rows_in_table(sqlContext, table_name):
    """
    Gets the number of rows in the given table.
    :param sqlContext: current SQL Context.
    :param table_name: string with the table name.
    :return: integer number.
    """
    return sqlContext.sql("SELECT COUNT(*) FROM " + table_name).collect()[0][0]
