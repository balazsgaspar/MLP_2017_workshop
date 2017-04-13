from .sql_utils import does_table_exist
from .sql_utils import get_number_of_rows_in_table


class TableNotCreatedError(Exception):
    """
    Custom exception class for handling situations in which a table was not created (i.e. it does not exist).
    """
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr("Table " + self.value + " was not created during the process.")
      

class TableDoesNotExistError(Exception):
    """
    Custom exception class for handling situations in which a table does not exist.
    Compared to TableNotCreatedError, only the output message is different.
    """
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr("Table " + self.value + " does not exist.")
      

class TableEmptyError(Exception):
    """
    Custom exception class for handling situations in which a table has no rows.
    """
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr("Table " + self.value + " has no records.")
      

def check_table_created(sqlContext, table_name):
    """
    Checks whether a table exist and if not, raises an TableNotCreatedError exception.
    :param sqlContext: current SQL Context.
    :param table_name: string with the table name.
    :raises TableNotCreatedError: if the table does not exist.
    """
    table_exist = does_table_exist(sqlContext, table_name)
    if not table_exist:
        raise TableNotCreatedError(table_name)
        

def check_table_existence(sqlContext, table_name):
    """
    Checks whether a table exist and if not, raises an TableDoesNotExistError exception.
    :param sqlContext: current SQL Context.
    :param table_name: string with the table name.
    :raises TableDoesNotExistError: if the table does not exist.
    """
    table_exist = does_table_exist(sqlContext, table_name)
    if not table_exist:
        raise TableDoesNotExistError(table_name)
        

def check_table_nonemptiness(sqlContext, table_name):
    """
    Checks whether a table contains at least one row and if not, raises an TableDoesNotExistError exception.
    :param sqlContext: current SQL Context.
    :param table_name: string with the table name.
    :raises TableEmptyError: if the table does not contain any row.
    """
    num_of_rows = get_number_of_rows_in_table(sqlContext, table_name)
    if num_of_rows == 0:
        raise TableEmptyError(table_name)
        

def check_table_created_and_nonempty(sqlContext, table_name):
    """
    Checks whether a table was created and is nonempty.
    :param sqlContext: current SQL context.
    :param table_name: string with the table name.
    :raises TableNotCreatedError: if the table does not exist.
    :raises TableEmptyError: if the table does not contain any row.
    """
    check_table_created(sqlContext, table_name)
    check_table_nonemptiness(sqlContext, table_name)
    

def check_table_exist_and_nonempty(sqlContext, table_name):
    """
    Checks whether a table exists and is nonempty.
    
    :param sqlContext: current SQL context.
    :param table_name: string with the table name.
    :raises TableDoesNotExistError: if the table does not exist.
    :raises TableEmptyError: if the table does not contain any row.
    """
    check_table_existence(sqlContext, table_name)
    check_table_nonemptiness(sqlContext, table_name)