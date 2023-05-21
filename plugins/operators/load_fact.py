from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 append=False,
                 sql_create="",
                 sql_insert="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.append = append
        self.sql_create = sql_create
        self.sql_insert = sql_insert

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)

        if not self.append:
            self.log.info("Creating Table {}.".format(self.table))
            redshift.run(self.sql_create, autocommit=True)

        self.log.info("Inserting data into table {}".format(self.table))
        redshift.run(self.sql_create, autocommit=True)