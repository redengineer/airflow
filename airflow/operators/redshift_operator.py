import logging

import psycopg2
import pandas as pd
import numpy as np

from airflow.models import BaseOperator
from airflow.utils import apply_defaults


def to_unicode(s):
    return s.decode('utf-8') if isinstance(s, str) else s


class RedshiftOperator(BaseOperator):

    """
    Execute a redshift command

    :param conn_dict: conn kwargs for psycopg2
    :type conn_dict: dict

    :param sql_command: sql command str
    :type sql_command: str
    """
    template_fields = ('sql_command',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(self, conn_dict, sql_command, *args, **kwargs):
        super(RedshiftOperator, self).__init__(*args, **kwargs)
        self.sql_command = sql_command

    def execute(self, context):
        short = self.sql_command.decode(
            'utf-8'
        ).decode('utf-8')[:100].encode('utf-8')
        logging.info('Executing: ' + str(short))

        redshift = psycopg2.connect(**conn_dict)
        result = None
        with redshift.cursor() as c:
            result = c.execute(self.sql_command)
        redshift.commit()
        logging.info(str(result)[:100])
        return str(result)


class RedshiftToExcelOperator(BaseOperator):

    """
    Execute a redshift command

    :param conn_dict: conn kwargs for psycopg2
    :type conn_dict: dict

    :param sql_list: sql command str
    :type sql_list: str/list
    """
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            conn_dict,
            sql_list,
            excel_file,
            sheet_names=None,
            cols_group=None,
            *args, **kwargs):
        super(RedshiftToExcelOperator, self).__init__(*args, **kwargs)
        self.sql_list = sql_list if isinstance(sql_list, list) else [sql_list]
        self.excel_file = excel_file
        self.sheet_names = sheet_names if isinstance(sheet_names, list) else [sheet_names]
        self.cols_group = cols_group if isinstance(cols_group, list) else [cols_group]

    def execute(self, context):
        self.redshift = psycopg2.connect(**conn_dict)

        writer = pd.ExcelWriter(self.excel_file)
        for i, sql_command in enumerate(self.sql_list):
            short = sql_command.decode(
                'utf-8'
            ).decode('utf-8')[:100].encode('utf-8')
            logging.info('Executing: ' + str(short))
            _df = self._execute_as_dataframe(sql_command)
            if self.cols_group[i]:
                _df.columns = map(
                    to_unicode,
                    self.cols_group[i])
            self._convert_df_col_type(_df)
            sheet_name = to_unicode(self.sheet_names[i] or '')
            _df.to_excel(
                writer,
                sheet_name=sheet_name,
                index=False)
        writer.save()

    def _execute_as_dataframe(self, sql_command):
        with self.redshift.cursor() as c:
            c.execute(sql_command)
            columns = [col.name for col in c.description]
            rows = c.fetchall()
            return pd.DataFrame(rows, columns=columns)

    def _convert_df_col_type(self, df):
        for k, v in df.dtypes.to_dict().items():
            if v.name == 'datetime64[ns]':
                df[k] = df[k].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
            elif v.name == 'object':
                df[k] = df[k].apply(to_unicode)
