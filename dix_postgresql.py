# 230215

import psycopg2
from psycopg2 import Error
from psycopg2.extras import execute_batch


def get_db_info(user='', password='', host='', port=5432):
    '''
    Функция возвращает информацию о базе данных
    '''
    dv_out_text = ''
    try:
        # Подключение к существующей базе данных
        connection = psycopg2.connect(user=user,
                                      password=password,
                                      host=host,
                                      port=port)
        # Курсор для выполнения операций с базой данных
        cursor = connection.cursor()
        # сведения о PostgreSQL
        dv_out_text = f"{connection.get_dsn_parameters()}"
        # Выполнить SQL-запрос
        cursor.execute("SELECT version();")
        # Получить результат
        dv_postgresql_version = cursor.fetchone()
        dv_out_text = f"{dv_postgresql_version} | {dv_out_text}"
    except (Exception, Error) as ERROR:
        dv_out_text = f"get_db_info - ERROR: {ERROR = }"
    finally:
        try:
            if connection:
                cursor.close()
                connection.close()
        except:
            pass
    return dv_out_text


def postgresql_del_and_insert(user='', password='', host='', port=5432,
                              dv_table='', dv_id_name='', dv_id_value='', dv_df=''):
    '''
    Функция удаляет по ключу dv_id_name=dv_id_value данные из таблицы dv_table и делает insert датафрейма dv_df
    '''
    dv_result_text = ''
    dv_result_type = ''
    try:
        if len(dv_df) > 0:
            dv_sql_delete = "DELETE FROM {} WHERE {}='{}'".format(dv_table, dv_id_name, dv_id_value)
            # print(dv_sql_delete)
            #
            df_columns = list(dv_df)
            # create (col1,col2,...)
            columns = ",".join(df_columns)
            #
            # create VALUES('%s', '%s",...) one '%s' per column
            values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
            #
            # create INSERT INTO dv_table (columns) VALUES('%s',...)
            dv_sql_insert = "INSERT INTO {} ({}) {}".format(dv_table, columns, values)
            # print(dv_sql_insert)
            # print(dv_df.values)
            #
            # Подключение к существующей базе данных
            connection = psycopg2.connect(user=user,
                                          password=password,
                                          host=host,
                                          port=port)
            # Курсор для выполнения операций с базой данных
            cursor = connection.cursor()
            cursor.execute(dv_sql_delete)
            execute_batch(cursor, dv_sql_insert, dv_df.values)
            connection.commit()
        dv_result_type = 'SUCCESS'
    except Exception as error:
        dv_result_text = f'postgresql_del_and_insert - ERROR: {error = }'
        dv_result_type = 'ERROR'
    finally:
        try:
            if connection:
                cursor.close()
                connection.close()
        except:
            pass
    return dv_result_type, dv_result_text
