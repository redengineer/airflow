import logging
import psycopg2

from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class RedshiftOperator(BaseOperator):

    """
    Execute a redshift command

    :param conn_dict: conn kwargs for psycopg2
    :type conn_dict: dict

    :param sql_command: sql command str
    :type sql_command: str
    """
    template_fields = ('sql_command',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(self, conn_dict, sql_command, *args, **kwargs):
        self.redshift = psycopg2.connect(**conn_dict)
        super(RedshiftOperator, self).__init__(*args, **kwargs)
        self.sql_command = sql_command

    def execute(self, context):
        short = self.sql_command.decode(
            'utf-8'
        ).decode('utf-8')[:100].encode('utf-8')
        logging.info('Executing: ' + str(short))
        
        result = None
        with self.redshift.cursor() as c:
            c.execute(self.sql_command)
            result = c.fetchall()
        logging.info(str(result)[:100])
        return str(result)
