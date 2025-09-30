# -*- coding: utf-8 -*-
# kafka2db
# https://github.com/dneupokoev/kafka2db
dv_file_version = '250930.01'
# 250930.01 - все json, в которых есть поле tbl4c2a отправляю а эндпоинт /itzamna/kafka_tbl4c2a/ и дальнейший разбор идет там
# 250303.01 - добавил колонки 'uid_c2a_actual', 'dict_dwh' в таблицу df_in_interaction_header
# 240812.01 - изменил обращение к df, теперь изменение колонки через .loc
# 240112.01 - добавил обработку топика, который только api вызывает
# 230620.01 - добавил возможность отправлять данные по api rest
# 230313.01 - исправил проблему, когда перед номером телефона в in_interaction_header добавлялся 0
# 230310.01 - забирает, обрабатывает данные из топика и сохраняет в таблицу in_interaction_header
# 230215.02 - первая рабочая версия: забирает, обрабатывает данные из топика и сохраняет в таблицы ai_*
#
# Kafka to DB PostgreSQL
# Переливка данных из топика кафки в базу данных PostgreSQL

import settings
import os
import re
import requests
import sys
import json
import datetime
import time
import platform
import configparser
import numpy as np
import pandas as pd
#
import dix_telegram
import dix_postgresql
#
from confluent_kafka import Consumer
#
#
from pathlib import Path

try:  # from project
    dv_path_main = f"{Path(__file__).parent}/"
    dv_file_name = f"{Path(__file__).name}"
except:  # from jupiter
    dv_path_main = f"{Path.cwd()}/"
    dv_path_main = dv_path_main.replace('jupyter/', '')
    dv_file_name = 'unknown_file'

# импортируем библиотеку для логирования
from loguru import logger

# logger.add("log/" + dv_file_name + ".json", level="DEBUG", rotation="00:00", retention='30 days', compression="gz", encoding="utf-8", serialize=True)
# logger.add("log/" + dv_file_name + ".json", level="WARNING", rotation="00:00", retention='30 days', compression="gz", encoding="utf-8", serialize=True)
# logger.add("log/" + dv_file_name + ".json", level="INFO", rotation="00:00", retention='30 days', compression="gz", encoding="utf-8", serialize=True)
logger.remove()  # отключаем логирование в консоль
if settings.DEBUG is True:
    logger.add(settings.PATH_TO_LOG + dv_file_name + ".log", level="DEBUG", rotation="00:00", retention='30 days', compression="gz", encoding="utf-8")
    logger.add(sys.stderr, level="DEBUG")
else:
    logger.add(settings.PATH_TO_LOG + dv_file_name + ".log", level="INFO", rotation="00:00", retention='30 days', compression="gz", encoding="utf-8")
    logger.add(sys.stderr, level="INFO")
logger.enable(dv_file_name)  # даем имя логированию
logger.info(f'***')
logger.info(f'BEGIN')
try:
    # Получаем версию ОС
    logger.info(f'os.version = {platform.platform()}')
except Exception as error:
    # Не удалось получить версию ОС
    logger.error(f'ERROR - os.version: {error = }')
try:
    # Получаем версию питона
    logger.info(f'python.version = {sys.version}')
except Exception as error:
    # Не удалось получить версию питона
    logger.error(f'ERROR - python.version: {error = }')
logger.info(f'{dv_path_main = }')
logger.info(f'{dv_file_name = }')
logger.info(f'{dv_file_version = }')
# получаем информацию о базе данных
dv_info_postgresql = dix_postgresql.get_db_info(user=settings.PostgreSQL_dwh_user,
                                                password=settings.PostgreSQL_dwh_password,
                                                host=settings.PostgreSQL_dwh_host,
                                                port=settings.PostgreSQL_dwh_port,
                                                database=settings.PostgreSQL_dwh_database)
logger.info(f"db_info = {dv_info_postgresql}")
#
logger.info(f'{settings.KAFKA_bootstrap_servers = }')
logger.info(f'{settings.KAFKA_group_id = }')
logger.info(f'{settings.KAFKA_topic = }')
#
logger.info(f'{settings.DEBUG = }')
logger.info(f'{settings.PATH_TO_LIB = }')
logger.info(f'{settings.PATH_TO_LOG = }')
logger.info(f'{settings.max_minutes_work = }')
logger.info(f'{settings.max_number_msg = }')
logger.info(f'{settings.SEND_TELEGRAM = }')
#
dv_find_text = re.compile(r'(\r|\n|\t|\b)')

#
#
def change_phone_format_to_rus(in_txt=''):
    '''
    Функция конвертирует полученный текст в формат номер телефона
    :param in_txt: текст
    :return: телефон в формате 70001234567
    '''
    # оставляем только цифры в номере телефона
    in_txt = re.sub(r'\D', '', in_txt)
    # если телефон состоит только из 10 цифр, то впереди добавляем 7
    in_txt = re.sub(r'^\d{10}$', f'7{in_txt}', in_txt)
    # если номер состоит из 11 цифр и первая из них 8, то меняем первую цифру на 7
    in_txt = re.sub(r'(^8)(\d{10}$)', r'7\2', in_txt)
    return in_txt

#
#
def get_now():
    '''
    Функция вернет текущую дату и время в заданном формате
    '''
    logger.debug(f"get_now")
    dv_time_begin = time.time()
    dv_created = f"{datetime.datetime.fromtimestamp(dv_time_begin).strftime('%Y-%m-%d %H:%M:%S')}"
    # dv_created = f"{datetime.datetime.fromtimestamp(dv_time_begin).strftime('%Y-%m-%d %H:%M:%S.%f')}"
    return dv_created

#
#
def get_second_between_now_and_datetime(in_datetime_str='2000-01-01 00:00:00'):
    '''
    Функция вернет количество секунд между текущим временем и полученной датой-временем в формате '%Y-%m-%d %H:%M:%S'
    '''
    logger.debug(f"get_second_between_now_and_datetime")
    tmp_datetime_start = datetime.datetime.strptime(in_datetime_str, '%Y-%m-%d %H:%M:%S')
    tmp_now = datetime.datetime.strptime(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
    tmp_seconds = int((tmp_now - tmp_datetime_start).total_seconds())
    return tmp_seconds


def f_is_json_key_present(json, key):
    '''
    Функция проверяет наличие ключа в json
    '''
    try:
        buf = json[key]
    except KeyError:
        return False
    return True


def f_check_json_from_kafka(dv_in_json):
    '''
    Функция проверяет json на корректность и возвращает:
    - параметр для вызова дальнейшей функции обработки данного json
    - сконвертированный json
    '''
    dv_out_type = 'unknown'
    dv_out_json = json.loads('{}')
    try:
        dv_out_json = json.loads(dv_in_json)
        # Определим тип по наличию ключей
        if f_is_json_key_present(dv_out_json, 'is_test'):
            dv_out_type = 'is_test'
        elif f_is_json_key_present(dv_out_json, 'date_recognized') \
             and f_is_json_key_present(dv_out_json, 'linkedid'):
            dv_out_type = 'avtootvetchik-detection'
        elif f_is_json_key_present(dv_out_json, 'linkedid') \
             and f_is_json_key_present(dv_out_json, 'operatorid') \
             and f_is_json_key_present(dv_out_json, 'texts') \
             and f_is_json_key_present(dv_out_json, 'existence'):
            dv_out_type = 'ai'
        elif f_is_json_key_present(dv_out_json, 'tbl4c2a'):
            dv_out_type = 'tbl4c2a'
        else:
            dv_out_type = 'unknown'
    except:
        dv_out_json = json.loads('{}')
    return dv_out_type, dv_out_json


def f_json2db_avtootvetchik_detection(dv_in_json):
    '''
    Функция обрабатывает json и отправляет данные в api
    '''
    dv_result_text = ''
    dv_result_type = ''
    try:
        dv_etl_json = {}
        dv_etl_json['linkedid'] = dv_in_json['linkedid']
        try:
            dv_etl_json['date_recognized'] = dv_in_json['date_recognized'].replace('T', ' ')
        except:
            dv_etl_json['date_recognized'] = ''
            pass
        #
        # Вызываем api (передаем данные в c2a)
        dv_json4api = dv_etl_json
        dv_json4api['pwd'] = settings.CONST_api_c2a_pwd
        logger.info(f"{dv_json4api = }")
        response = requests.request(method='POST',
                                    url=settings.CONST_api_c2a_url_avtootvetchik,
                                    headers={'Content-Type': 'application/json'},
                                    data=json.dumps(dv_json4api))
        if response.status_code == 200:
            dv_result_text = f"f_json2db_avtootvetchik_detection - SUCCESS: {dv_etl_json['linkedid'] = }"
            dv_result_type = 'SUCCESS'
        else:
            dv_result_text = f"f_json2db_avtootvetchik_detection - ERROR: api {response.status_code = }"
            dv_result_type = 'ERROR'
    except Exception as error:
        dv_result_text = f'f_json2db_avtootvetchik_detection - ERROR: {error = }'
        dv_result_type = 'ERROR'
    return dv_result_type, dv_result_text


def f_json2db_tbl4c2a(dv_in_json):
    '''
    Функция обрабатывает json для отправителя "tbl4c2a":
    - формирует нужные SQL-запросы
    - выполняет на БД эти запросы
    '''
    dv_result_text = ''
    dv_result_type = ''
    try:
        dv_uid = dv_in_json['uid']
        dv_tbl4c2a = dv_in_json['tbl4c2a']
        #
        if settings.CONST_api_c2a_kafka_tbl4c2a != '':
            # Все отправляем в settings.CONST_api_c2a_kafka_tbl4c2a
            #
            # Вызываем api (передаем данные в c2a)
            dv_send_dict = {}
            dv_send_dict['atok'] = settings.CONST_api_c2a_pwd
            dv_send_dict['param'] = {}
            dv_send_dict['param']['tbl4c2a'] = json.dumps(dv_in_json)
            requests.request(
                method='POST', url=settings.CONST_api_c2a_kafka_tbl4c2a, headers={'Content-Type': 'application/json'}, data=json.dumps(dv_send_dict))
            pass
        #
        if dv_tbl4c2a == 'interaction':
            dv_etl_json = dv_in_json['header'][0]
            if 'uid' not in dv_etl_json:
                dv_etl_json['uid'] = f"{dv_uid}"
            try:
                dv_etl_json['date_doc'] = dv_etl_json['date_doc'].replace('T', ' ')
            except:
                dv_etl_json['date_doc'] = ''
                pass
            #
            # Вызываем api (передаем данные в c2a)
            dv_json4api = dv_etl_json
            dv_json4api['pwd'] = settings.CONST_api_c2a_pwd
            # logger.info(f"{dv_json4api = }")
            requests.request(
                method='POST', url=settings.CONST_api_c2a_url_interaction, headers={'Content-Type': 'application/json'}, data=json.dumps(dv_json4api))
            #
            # Готовим данные и сохраняем в БД
            # формируем таблицу df_in_interaction_header
            df_in_interaction_header = pd.json_normalize(dv_etl_json)
            # если "нужной" колонки нет, то создадим её
            if 'uid' not in df_in_interaction_header.columns:
                df_in_interaction_header['uid'] = f"{dv_uid}"
            if 'number' not in df_in_interaction_header.columns:
                df_in_interaction_header['number'] = ''
            if 'date_doc' not in df_in_interaction_header.columns:
                df_in_interaction_header['date_doc'] = ''
            # else:
            #     try:
            #         df_in_interaction_header['date_doc'][0] = df_in_interaction_header['date_doc'][0].replace('T', ' ')
            #     except:
            #         pass
            if 'type' not in df_in_interaction_header.columns:
                df_in_interaction_header['type'] = ''
            if 'patient_uid' not in df_in_interaction_header.columns:
                df_in_interaction_header['patient_uid'] = ''
            if 'patient_phone' not in df_in_interaction_header.columns:
                df_in_interaction_header['patient_phone'] = ''
            if 'patient_first_uid' not in df_in_interaction_header.columns:
                df_in_interaction_header['patient_first_uid'] = ''
            if 'patient_first_phone' not in df_in_interaction_header.columns:
                df_in_interaction_header['patient_first_phone'] = ''
            if 'not_recording_reason_uid' not in df_in_interaction_header.columns:
                df_in_interaction_header['not_recording_reason_uid'] = ''
            if 'autor_uid' not in df_in_interaction_header.columns:
                df_in_interaction_header['autor_uid'] = ''
            if 'phone_operator' not in df_in_interaction_header.columns:
                df_in_interaction_header['phone_operator'] = ''
            if 'phone_callcenter' not in df_in_interaction_header.columns:
                df_in_interaction_header['phone_callcenter'] = ''
            if 'is_manual_call' not in df_in_interaction_header.columns:
                df_in_interaction_header['is_manual_call'] = ''
            if 'linkedid' not in df_in_interaction_header.columns:
                df_in_interaction_header['linkedid'] = ''
            if 'uid_c2a_actual' not in df_in_interaction_header.columns:
                df_in_interaction_header['uid_c2a_actual'] = ''
            if 'dict_dwh' not in df_in_interaction_header.columns:
                df_in_interaction_header['dict_dwh'] = ''
            #
            # проверяем формат номеров телефонов и преобразуем к формату РФ
            df_in_interaction_header.loc[0, 'patient_phone'] = change_phone_format_to_rus(in_txt=df_in_interaction_header['patient_phone'][0])
            df_in_interaction_header.loc[0, 'patient_first_phone'] = change_phone_format_to_rus(in_txt=df_in_interaction_header['patient_first_phone'][0])
            #
            # оставляем только "нужные" колонки
            df_in_interaction_header = df_in_interaction_header[[
                'uid', 'number', 'date_doc', 'type', 'patient_uid', 'patient_phone', 'patient_first_uid', 'patient_first_phone', 'not_recording_reason_uid',
                'autor_uid', 'phone_operator', 'phone_callcenter', 'is_manual_call', 'linkedid', 'uid_c2a_actual', 'dict_dwh']]
            # меняем в колонках значение NaN на дефолтные значения
            df_in_interaction_header.fillna('', inplace=True)
            #
            #
            # Сохраняем данные в таблицу db_c2a.in_interaction_header
            dv_f_result_type, dv_result_text = dix_postgresql.postgresql_del_and_insert(
                user=settings.PostgreSQL_dwh_user,
                password=settings.PostgreSQL_dwh_password,
                host=settings.PostgreSQL_dwh_host,
                port=settings.PostgreSQL_dwh_port,
                database=settings.PostgreSQL_dwh_database,
                dv_table='db_c2a.in_interaction_header',
                dv_id_name='row_id',
                dv_id_value=0,
                dv_df=df_in_interaction_header)
            if dv_f_result_type != 'SUCCESS':
                raise Exception(f"{dv_result_text}")
        # elif...
        #
        #
        dv_result_text = f'f_json2db_tbl4c2a - SUCCESS: {dv_tbl4c2a} - {dv_uid = } - {dv_result_text}'
        dv_result_type = 'SUCCESS'
    except Exception as error:
        dv_result_text = f'f_json2db_tbl4c2a - ERROR: {error = }'
        dv_result_type = 'ERROR'
    return dv_result_type, dv_result_text



def f_json2db_ai(dv_in_json):
    '''
    Функция обрабатывает json для отправителя "ai":
    - формирует нужные SQL-запросы
    - выполняет на БД эти запросы
    '''
    dv_result_text = ''
    dv_result_type = ''
    try:
        dv_linkedid = dv_in_json['linkedid']
        #
        # формируем таблицу df_ai_linkedid
        df_ai_linkedid = pd.DataFrame([{'linkedid': dv_in_json['linkedid'], 'operatorid': dv_in_json['operatorid']}])
        # меняем в колонках значение NaN на дефолтные значения
        df_ai_linkedid['operatorid'].fillna('', inplace=True)
        #
        # формируем таблицу df_ai_texts
        df_ai_texts = pd.json_normalize(dv_in_json['texts'])
        df_ai_texts['linkedid'] = dv_linkedid
        # если "нужной" колонки нет, то создадим её
        if 'str' not in df_ai_texts.columns:
            df_ai_texts['str'] = np.nan
        if 'label' not in df_ai_texts.columns:
            df_ai_texts['label'] = np.nan
        if 'coefficient' not in df_ai_texts.columns:
            df_ai_texts['coefficient'] = np.nan
        # оставляем только "нужные" колонки
        df_ai_texts = df_ai_texts[['linkedid', 'str', 'label', 'coefficient']]
        # меняем в колонках значение NaN на дефолтные значения
        df_ai_texts['str'].fillna('', inplace=True)
        df_ai_texts['label'].fillna('', inplace=True)
        df_ai_texts['coefficient'].fillna(0, inplace=True)
        #
        # формируем таблицу df_ai_existence
        df_ai_existence = pd.json_normalize(dv_in_json['existence'])
        df_ai_existence['linkedid'] = dv_linkedid
        # если "нужной" колонки нет, то создадим её
        if 'existence_name' not in df_ai_existence.columns:
            df_ai_existence['existence_name'] = np.nan
        if 'is_existence' not in df_ai_existence.columns:
            df_ai_existence['is_existence'] = np.nan
        # оставляем только "нужные" колонки
        df_ai_existence = df_ai_existence[['linkedid', 'existence_name', 'is_existence']]
        # меняем в колонках значение NaN на дефолтные значения
        df_ai_existence['existence_name'].fillna('', inplace=True)
        df_ai_existence['is_existence'].fillna('0', inplace=True)
        #
        #
        # Сохраняем данные в таблицу public.ai_linkedid
        dv_f_result_type, dv_f_result_text = dix_postgresql.postgresql_del_and_insert(user=settings.PostgreSQL_user,
                                                                                      password=settings.PostgreSQL_password,
                                                                                      host=settings.PostgreSQL_host,
                                                                                      port=settings.PostgreSQL_port,
                                                                                      database=settings.PostgreSQL_database,
                                                                                      dv_table='public.ai_linkedid',
                                                                                      dv_id_name='linkedid',
                                                                                      dv_id_value=dv_linkedid,
                                                                                      dv_df=df_ai_linkedid)
        if dv_f_result_type != 'SUCCESS':
            raise Exception(f"{dv_f_result_text}")
        #
        # Сохраняем данные в таблицу public.ai_texts
        dv_f_result_type, dv_f_result_text = dix_postgresql.postgresql_del_and_insert(user=settings.PostgreSQL_user,
                                                                                      password=settings.PostgreSQL_password,
                                                                                      host=settings.PostgreSQL_host,
                                                                                      port=settings.PostgreSQL_port,
                                                                                      database=settings.PostgreSQL_database,
                                                                                      dv_table='public.ai_texts',
                                                                                      dv_id_name='linkedid',
                                                                                      dv_id_value=dv_linkedid,
                                                                                      dv_df=df_ai_texts)
        if dv_f_result_type != 'SUCCESS':
            raise Exception(f"{dv_f_result_text}")
        #
        # Сохраняем данные в таблицу public.ai_existence
        dv_f_result_type, dv_f_result_text = dix_postgresql.postgresql_del_and_insert(user=settings.PostgreSQL_user,
                                                                                      password=settings.PostgreSQL_password,
                                                                                      host=settings.PostgreSQL_host,
                                                                                      port=settings.PostgreSQL_port,
                                                                                      database=settings.PostgreSQL_database,
                                                                                      dv_table='public.ai_existence',
                                                                                      dv_id_name='linkedid',
                                                                                      dv_id_value=dv_linkedid,
                                                                                      dv_df=df_ai_existence)
        if dv_f_result_type != 'SUCCESS':
            raise Exception(f"{dv_f_result_text}")
        #
        # dv_result_text = f'f_json2db_ai - SUCCESS: {dv_linkedid = }\n{df_ai_linkedid = }\n{df_ai_texts = }\n{df_ai_existence = }'
        dv_result_text = f'f_json2db_ai - SUCCESS: {dv_linkedid = } - {dv_f_result_text}'
        dv_result_type = 'SUCCESS'
    except Exception as error:
        dv_result_text = f'f_json2db_ai - ERROR: {error = }'
        dv_result_type = 'ERROR'
    return dv_result_type, dv_result_text


def get_disk_space():
    '''
    Функция вернет информацию о свободном месте на диске в гигабайтах
    dv_statvfs_bavail = Количество свободных гагабайтов, которые разрешено использовать обычным пользователям (исключая зарезервированное пространство)
    dv_statvfs_blocks = Размер файловой системы в гигабайтах
    dv_result_bool = true - корректно отработало, false - получить данные не удалось
    '''
    logger.debug(f"get_disk_space")
    dv_statvfs_blocks = 999999
    dv_statvfs_bavail = dv_statvfs_blocks
    dv_result_bool = False
    try:
        # получаем свободное место на диске ubuntu
        statvfs = os.statvfs('/')
        # Size of filesystem in bytes (Размер файловой системы в байтах)
        dv_statvfs_blocks = round((statvfs.f_frsize * statvfs.f_blocks) / (1024 * 1024 * 1024), 2)
        # Actual number of free bytes (Фактическое количество свободных байтов)
        # dv_statvfs_bfree = round((statvfs.f_frsize * statvfs.f_bfree) / (1024 * 1024 * 1024), 2)
        # Number of free bytes that ordinary users are allowed to use (excl. reserved space)
        # Количество свободных байтов, которые разрешено использовать обычным пользователям (исключая зарезервированное пространство)
        dv_statvfs_bavail = round((statvfs.f_frsize * statvfs.f_bavail) / (1024 * 1024 * 1024), 2)
        dv_result_bool = True
    except:
        pass
    return dv_statvfs_bavail, dv_statvfs_blocks, dv_result_bool


if __name__ == '__main__':
    dv_time_begin = time.time()
    # получаем информацию о свободном месте на диске в гигабайтах
    dv_disk_space_free_begin = get_disk_space()[0]
    logger.info(f"{dv_disk_space_free_begin = } Gb")
    #
    logger.info(f"{datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')}")
    dv_for_send_txt_type = ''
    dv_for_send_text = ''
    dv_lib_path_ini = ''
    try:
        # читаем значения из конфига
        dv_lib_path_ini = f"{settings.PATH_TO_LIB}/kafka2db.cfg"
        dv_cfg = configparser.ConfigParser()
        if os.path.exists(dv_lib_path_ini):
            with open(dv_lib_path_ini, mode="r", encoding='utf-8') as fp:
                dv_cfg.read_file(fp)
        # читаем значения
        dv_cfg_last_send_tlg_success = dv_cfg.get('DEFAULT', 'last_send_tlg_success', fallback='2000-01-01 00:00:00')
    except:
        pass
    #
    try:
        dv_file_lib_path = f"{settings.PATH_TO_LIB}/kafka2db.dat"
        if os.path.exists(dv_file_lib_path):
            dv_file_lib_open = open(dv_file_lib_path, mode="r", encoding='utf-8')
            dv_file_lib_time = next(dv_file_lib_open).strip()
            dv_file_lib_open.close()
            dv_file_old_start = datetime.datetime.strptime(dv_file_lib_time, '%Y-%m-%d %H:%M:%S')
            tmp_now = datetime.datetime.strptime(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
            tmp_seconds = int((tmp_now - dv_file_old_start).total_seconds())
            if tmp_seconds < settings.max_minutes_work * 2 * 60:
                raise Exception(f"Уже выполняется c {dv_file_lib_time} - перед запуском дождитесь завершения предыдущего процесса!")
        else:
            dv_file_lib_open = open(dv_file_lib_path, mode="w", encoding='utf-8')
            dv_file_lib_time = f"{datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}"
            dv_file_lib_open.write(f"{dv_file_lib_time}")
            dv_file_lib_open.close()
        #
        #
        #
        # Подключаемся к топику кафки и получаем значения:
        logger.debug(f"Подключаемся к кафке")
        kafka_consumer = Consumer({
            'bootstrap.servers': settings.KAFKA_bootstrap_servers,
            'group.id': settings.KAFKA_group_id,
            'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest'
        })
        # conf = {'bootstrap.servers': broker, 'group.id': group, 'session.timeout.ms': 6000, 'auto.offset.reset': 'earliest', 'enable.auto.offset.store': False}
        logger.debug(f"Указываем топики, которые будем слушать")
        kafka_consumer.subscribe(settings.KAFKA_topic)
        #
        # порядковый номер сообщения с начала работы данного скрипта
        dv_number_msg = 0
        logger.debug(f"Начинаем слушать кафку")
        while True:
            # пока True будем слушать кафку
            dv_kafka_msg = kafka_consumer.poll(timeout=1.0)
            #
            if dv_kafka_msg is not None:
                # попадаем сюда когда получили сообщение
                #
                if dv_kafka_msg.error():
                    # если тип сообщения от кафки "ошибка", то пишем в лог/телеграм и завершаем работу
                    logger.error(f'ERROR - KAFKA: {dv_kafka_msg.error() = }')
                    try:
                        kafka_consumer.close()
                    except:
                        pass
                    raise Exception(f"ERROR - KAFKA: {dv_kafka_msg.error()}")
                #
                dv_number_msg += 1
                #
                dv_kafka_msg = f"{dv_kafka_msg.value().decode('utf-8')}"
                logger.debug(f"Received message: {dv_kafka_msg}")
                logger.debug(f"***")
                # получаем тип json (отправителя) и проверяем на корректность json
                dv_kafka_type, dv_kafka_json = f_check_json_from_kafka(dv_kafka_msg)
                logger.debug(f"{dv_kafka_type = }")
                logger.debug(f"{dv_kafka_json = }")
                dv_result_text = ''
                dv_result_type = ''
                if dv_kafka_type == 'ai':
                    dv_result_type, dv_result_text = f_json2db_ai(dv_kafka_json)
                elif dv_kafka_type == 'tbl4c2a':
                    dv_result_type, dv_result_text = f_json2db_tbl4c2a(dv_kafka_json)
                elif dv_kafka_type == 'avtootvetchik-detection':
                    # Пришла информации о срабатывании автоответчика
                    dv_result_type, dv_result_text = f_json2db_avtootvetchik_detection(dv_kafka_json)
                logger.debug(f"{dv_result_type = }")
                if dv_kafka_type == 'ERROR':
                    logger.error(f"{dv_kafka_type} - {dv_result_type} - {dv_result_text}")
                else:
                    logger.info(f"{dv_kafka_type} - {dv_result_type} - {dv_result_text}")
            #
            # max_number_msg - какое максимальное количество сообщений обработать за один запуск скрипта (нужно для тестирования, в бою скорее всего ограничивать не надо)
            if settings.max_number_msg != 0 and dv_number_msg >= settings.max_number_msg:
                break
            # если скрипт работает дольше отведенного времени, то прерываем работу
            if round(int('{:.0f}'.format(1000 * (time.time() - dv_time_begin))) / (1000 * 60)) >= settings.max_minutes_work:
                break
        try:
            kafka_consumer.close()
        except:
            pass
        #
        dv_for_send_txt_type = 'SUCCESS'
        dv_for_send_text = 'SUCCESS'
        os.remove(dv_file_lib_path)
    except Exception as ERROR:
        dv_for_send_txt_type = 'ERROR'
        dv_for_send_text = f"{ERROR = }"
    finally:
        # получаем информацию о свободном месте на диске в гигабайтах
        dv_disk_space_free_end, dv_statvfs_blocks, dv_result_bool = get_disk_space()
        logger.info(f"{dv_disk_space_free_end = } Gb")
        try:
            if settings.CHECK_DISK_SPACE is True:
                # формируем текст о состоянии места на диске
                if dv_result_bool is True:
                    dv_for_send_text = f"{dv_for_send_text} | disk space all/free_begin/free_end: {dv_statvfs_blocks}/{dv_disk_space_free_begin}/{dv_disk_space_free_end} Gb"
                else:
                    dv_for_send_text = f"{dv_for_send_text} | check_disk_space = ERROR"
        except:
            pass
        logger.info(f"{dv_for_send_text}")
        try:
            if settings.SEND_TELEGRAM is True:
                if dv_for_send_txt_type == 'ERROR':
                    # если отработало с ошибкой, то в телеграм оправляем ошибку ВСЕГДА! хоть каждую минуту - это не спам
                    dv_is_SEND_TELEGRAM_success = True
                else:
                    dv_is_SEND_TELEGRAM_success = False
                    # Чтобы слишком часто не спамить в телеграм сначала проверим разрешено ли именно сейчас отправлять сообщение
                    try:
                        # проверяем нужно ли отправлять успех в телеграм
                        if get_second_between_now_and_datetime(dv_cfg_last_send_tlg_success) > settings.SEND_SUCCESS_REPEATED_NOT_EARLIER_THAN_MINUTES * 60:
                            dv_is_SEND_TELEGRAM_success = True
                            # актуализируем значение конфига
                            dv_cfg_last_send_tlg_success = get_now()
                            dv_cfg.set('DEFAULT', 'last_send_tlg_success', dv_cfg_last_send_tlg_success)
                        else:
                            dv_is_SEND_TELEGRAM_success = False
                    except:
                        dv_is_SEND_TELEGRAM_success = False
                #
                # пытаться отправить будем только если предыдущие проверки подтвердили необходимость отправки
                if dv_is_SEND_TELEGRAM_success is True:
                    dix_telegram.f_telegram_send_message(tlg_bot_token=settings.TLG_BOT_TOKEN, tlg_chat_id=settings.TLG_CHAT_FOR_SEND,
                                                         txt_name=f"kafka2db {dv_file_version}",
                                                         txt_type=dv_for_send_txt_type,
                                                         txt_to_send=f"{dv_for_send_text}",
                                                         txt_mode=None)
        except:
            pass
        try:
            # сохраняем файл конфига
            with open(dv_lib_path_ini, mode='w', encoding='utf-8') as configfile:
                dv_cfg.write(configfile)
        except:
            pass
        #
        logger.info(f"{datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')}")
        work_time_ms = int('{:.0f}'.format(1000 * (time.time() - dv_time_begin)))
        logger.info(f"{work_time_ms = }")
        #
        logger.info(f'END')
