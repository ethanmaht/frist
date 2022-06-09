# from sshtunnel import SSHTunnelForwarder
import yaml
import pymysql
import pandas as pd
import os
import re
import difflib
from clickhouse_driver import Client
from pyhive import hive


sql_create_table = """
CREATE TABLE IF NOT EXISTS {db_name}.`{table_name}`(
   `{key_name}` BIGINT(20) UNSIGNED AUTO_INCREMENT,
   PRIMARY KEY ( `{key_name}` )
)ENGINE=InnoDB DEFAULT CHARSET=utf8;
"""

click_create_table = """
CREATE TABLE {db_name}.{table_name} (
    {col}
) ENGINE = {engine}
PARTITION BY {}
ORDER BY join_day;
"""

sql_delete_data = """
delete from {db}.{table}
"""

click_delete_data = """
alter table {db}.{table} delete
"""


class OperateDataBase:

    def __init__(self, config_name):
        self.config_file_path = None
        self.config_file_name = '/db_config.yml'
        self.standard = ['mysql', 'mongodb', 'sqlsever', 'clickhouse', 'hive', ]
        self.type = 'mysql'
        self.config_name = config_name
        self.config = {}
        self.conn = self.db_connect()

    # def __del__(self):
    #     if self.type == 'mysql':
    #         self.conn.close()

    def read_db_config(self):
        if not self.config_file_path:
            self.config_file_path = os.path.split(os.path.realpath(__file__))[0] + self.config_file_name

        _file = open(self.config_file_path, 'r', encoding='utf-8').read()

        return yaml.load(_file, Loader=yaml.FullLoader)[self.config_name]

    def db_connect(self):
        self.config = self.read_db_config()

        if 'type' in self.config:
            self.type = self.config['type']

        if str_correcting(self.type, self.standard) == 'mysql':
            return self.mysql_connect()

        if str_correcting(self.type, self.standard) == 'clickhouse':
            return self.click_connect()

        if str_correcting(self.type, self.standard) == 'hive':
            return self.hive_connect()

    def mysql_connect(self):
        if 'port' in self.config:
            port = self.config['port']
        else:
            port = 3306

        if 'db_name' in self.config:
            return pymysql.connect(
                host=self.config['host'], port=port, user=self.config['user'], passwd=self.config['pw'],
                database=self.config['database']
            )

        return pymysql.connect(
            host=self.config['host'], port=port, user=self.config['user'], passwd=self.config['pw'],
        )

    def click_connect(self):
        if 'port' in self.config:
            return Client(
                host=self.config['host'], user=self.config['user'], password=self.config['pw'],
                port=self.config['port']
            )

        return Client(
            host=self.config['host'], user=self.config['user'], password=self.config['pw'],
        )

    def hive_connect(self):

        if 'port' in self.config:
            _port = self.config['port']
        else:
            _port = 10000

        if 'pw' in self.config:
            return hive.Connection(
                host=self.config['host'], port=_port, username=self.config['user'],
                password=self.config['pw']
            )

        return hive.Connection(
            host=self.config['host'], port=_port, username=self.config['user'],
        )

    def _create_table(self, db_name, table_name, key_name, **kwargs):
        if self.type in ['mysql', ]:
            _create_table_mysql(self.conn, db_name, table_name, key_name)
        if self.type in ['clickhouse', ]:
            _create_table_click(self.conn, db_name, table_name, **kwargs)

    def read_df_by_sql(self, sql):
        if self.type in ['mysql', 'hive', ]:
            return pd.read_sql(sql, self.conn)
        if self.type in ['clickhouse', ]:
            return read_click_sql(sql, self.conn)

    def delete_data(self, db_name, table_name, where=None):
        if self.type in ['mysql', ]:
            _delete_data_mysql(self.conn, db_name, table_name, where)
        if self.type in ['clickhouse', ]:
            _delete_data_click(self.conn, db_name, table_name, where)

    def execute_sql(self, sql, split=None):
        if self.type in ['mysql', ]:
            execute_mysql_sql(sql, self.conn)
        if self.type in ['clickhouse', ]:
            execute_click_sql(sql, self.conn, split)
        if self.type in ['hive', ]:
            execute_hql(sql, self.conn)

    def write_data(self, df, db_name, tab_name, step=None, auto_add_col=False, fill_nan=False):
        if self.type in ['mysql', ]:
            _write_mysql_date(df, self.conn, db_name, tab_name, step=step, auto_add_col=auto_add_col)
        if self.type in ['clickhouse', ]:
            _write_click_date(df, self.conn, db_name, tab_name, step=step, auto_add_col=auto_add_col, fill_nan=fill_nan)


def execute_click_sql(sql, client, split=None):
    if split:
        q_list = sql.split(";")
        for q in q_list:
            q = q.strip()
            if q:
                print(f'[Execute] hql: <{q}>')
                client.execute(q)
    else:
        client.execute(sql, columnar=True, with_column_types=True)
        print(f'Execute click sql <{sql}> success:'.replace('\n', ''))


def execute_hql(sql, conn):
    cursor = conn.cursor()
    q_list = sql.split(";")
    for q in q_list:
        q = q.strip()
        if q:
            print(f'[Execute] hql: <{q}>')
            cursor.execute(q)


def execute_mysql_sql(sql, conn):
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()


def _create_table_mysql(conn, db_name, table_name, key_name):
    cursor = conn.cursor()

    cursor.execute(
        sql_create_table.format(
            db_name=db_name, table_name=table_name, key_name=key_name
        )
    )
    conn.commit()


def _write_sql_data(write_data, conn, db_name, table):
    _data = write_data.to_dict(orient='split')
    _col = _data['columns']
    _sql, _val = make_mysql_inert_sql(db_name, table, _data)
    cursor = conn.cursor()
    cursor.executemany(_sql, _val)
    conn.commit()


def _delete_data_mysql(conn, db_name, table_name, where=None):
    cursor = conn.cursor()

    sql = sql_delete_data.format(db=db_name, table=table_name)
    if where:
        sql += f'where {where}'

    cursor.execute(sql)
    conn.commit()


def _create_table_click(conn, db_name, table_name, engine, col, partition=None, order=None, ):
    sql = click_create_table.format(
        db_name=db_name, table_name=table_name, col=col, engine=engine
    )

    if partition:
        sql += f'\nPARTITION BY {partition}'

    if order:
        sql += f'\nORDER BY {order}'

    conn.execute(sql, columnar=True, with_column_types=True)


def _write_click_date(write_data, client, db_name, table, step=0, auto_add_col=False, fill_nan=False):
    _data = write_data.to_dict(orient='split')

    if auto_add_col:
        _add_click_col(client, db_name, table, _data['columns'])

    if not _data['data']:
        return 0

    if step:
        _step_write_click_date(_data, client, db_name, table, step, fill_nan=fill_nan)
    else:
        _sql = make_click_inert_sql(db_name, table, _data, fill_nan=fill_nan)
        client.execute(_sql)
        print(f'Write date to click success: {len(_data)} row')


def _add_click_col(client, db_name, table, write_col):

    col_has = read_click_sql(f'select * from {db_name}.{table} limit 1', client).to_dict(orient='split')['columns']
    diff_col = list(set(write_col).difference(set(col_has)))

    while diff_col:
        _col = diff_col.pop(0)
        try:
            client.execute(f'alter TABLE {db_name}.{table} add COLUMN {_col} String;')
        except BaseException as e:
            print(e)


def _write_mysql_date(write_data, client, db_name, table, step=0, auto_add_col=False):
    _data = write_data.to_dict(orient='split')
    _sql = make_click_inert_sql(db_name, table, _data)
    execute_mysql_sql(_sql, client)


def _delete_data_click(conn, db_name, table_name, where=None):
    sql = click_delete_data.format(
        db=db_name, table=table_name
    )
    if where:
        sql += f'where {where}'
    else:
        print('[ERR]: clickhouse delete data must have <where>!')
    conn.execute(sql, columnar=True, with_column_types=True)


def make_click_inert_sql(db_name, table, _data, fill_nan=False):
    _col = _data['columns']
    _len = len(_col)
    _col = tuple(_col)
    _col = str(_col).replace("'", "`")
    _val = str([tuple(_) for _ in _data['data']])[1:-1]
    if fill_nan:
        _val = _val.replace("'nan'", "null").replace("None", "null")
    return f"INSERT INTO {db_name}.`{table}` {_col} VALUES {_val}"


def _step_write_click_date(write_data, client, db_name, table, step=0, fill_nan=False):
    _data, _col = write_data['data'], write_data['columns']
    _s, _e, _top = 0, step, len(_data)
    while _e < _top:
        val_sub = _data[_s: _e]
        _sql = make_click_inert_step_sql(db_name, table, val_sub, _col, fill_nan=fill_nan)
        client.execute(_sql)
        _s += step
        _e += step
    val_sub = _data[_s: _top]
    _sql = make_click_inert_step_sql(db_name, table, val_sub, _col, fill_nan=fill_nan)
    client.execute(_sql)


def make_click_inert_step_sql(db_name, table, _data, col, fill_nan=False):
    _col = col
    _len = len(_col)
    _col = tuple(_col)
    _col = str(_col).replace("'", "`")
    _val = str([tuple(_) for _ in _data])[1:-1]
    if fill_nan:
        _val = _val.replace("nan", "null").replace("None", "null")
    return f"INSERT INTO {db_name}.`{table}` {_col} VALUES {_val}"


def read_click_sql(sql, client):
    data, columns = client.execute(sql, columnar=True, with_column_types=True)
    df = pd.DataFrame({re.sub(r'\W', '_', col[0]): d for d, col in zip(data, columns)})
    return df


def make_mysql_inert_sql(db_name, table, _data, is_replace=False):
    _col = _data['columns']
    data_val = _data['data']
    _len = len(_col)
    _col = tuple(_col)
    _col = str(_col).replace("'", "`")
    _char = '({len})'.format(len='%s,' * _len)[:-2] + ')'
    _val = str([tuple(_) for _ in data_val])[1:-1]
    if is_replace:
        return f"replace INTO {db_name}.`{table}` {_col} VALUES {_char}", _val
    return f"INSERT INTO {db_name}.`{table}` {_col} VALUES {_val}"


def make_sub_mysql_inert_sql(db_name, table, _col, _data, _s, _e, is_replace=False):
    _col = tuple(_col)
    _col = str(_col).replace("'", "`")
    _val = str([tuple(_) for _ in _data])[1:-1]
    if is_replace:
        return f"replace INTO {db_name}.`{table}` {_col} VALUES {_val}"
    return f"INSERT INTO {db_name}.`{table}` {_col} VALUES {_val}"


def str_correcting(_str, standard_list: list):
    _str = _str.lower()

    if _str in standard_list:
        return _str

    _list, stand_list = [], []
    for _, stand in enumerate(standard_list):
        score = difflib.SequenceMatcher(None, _str, stand)
        if score.quick_ratio() > .75:
            _list.append(_)
            stand_list.append(stand)
    if _list:
        return standard_list[_list[stand_list.index(max(stand_list))]]
    return ''
