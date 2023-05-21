# airflow modules
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    This class performs checks on the data present in the table passed as a parameter.

    Attributes
    ----------
    ui_color: str
        Task's color in the Graph View.
    sql_row_numbers: str
        sql query to count number of rows in a table.

    Methods
    -------
    __init__():
        Creates class object.
    execute():
        Checks number of rows in the table.
    """

    ui_color = '#89DA59'
    sql_row_numbers = ("""
        SELECT
            COUNT(*)
        FROM {};
    """)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):
        """
        Creates a Class constructor and stores connection name and data table name.
        
        Attributes
        ----------
        redshift_conn_id: str
            Redshift connection name in the Airflow UI.
        table:
            Name of the table where data quality will be checked. 
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self):
        """
        This method connects to Redshift through PostgresHook, performs a query to check the number of rows in the table and checks if the number of rows is greater than 0.

        Attributes
        ----------
        redshift: connection object
            Stores Redshift connection.
        records: list
            Stores the number of rows in the table.
        """

        self.log.info("Data quality check starting...")
        redshift = PostgresHook(self.redshift_conn_id)

        records = redshift.get_records(DataQualityOperator.sql_row_numbers.format(self.table))

        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError("Data quality check failed. Table {} contained 0 rows".format(self.table))
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError("Data quality check failed. Table {} contained 0 rows".format(self.table))
        self.log.info("Data quality on table {} check passed with {} records".format(self.table, records[0][0]))