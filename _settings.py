# -*- coding: utf-8 -*-
# kafka2db
# https://github.com/dneupokoev/kafka2db
# 230215
#
# Kafka to DB PostgreSQL
# Переливка данных из топика кафки в базу данных PostgreSQL
#
# ВНИМАНИЕ!!! Перед запуском необходимо ЗАПОЛНИТЬ пароли в данном файле и ПЕРЕИМЕНОВАТЬ его в settings.py
#
# подключение к PostgreSQL
PostgreSQL_host = '192.168.5.'
PostgreSQL_port = 5432
PostgreSQL_dbname = 'dbname '
PostgreSQL_user = 'user'
PostgreSQL_password = 'password'
#
# подключение к kafka
KAFKA_bootstrap_servers = '...com:9092,...com:9092,...com:9092'
KAFKA_group_id = 'kafka2db'
# KAFKA_topic - список топиков, которые нужно слушать и обрабатывать: ['topic_01', 'topic_02', ] - можно только один, если топика не окажется в кафке, то будет ошибка
KAFKA_topic = ['topic_01', ]
# KAFKA_topic = ['topic_01', 'topic_02', ]
#
#
#
# *** Настройки ***
# для избыточного логирования True, иначе False
DEBUG = True
#
# создаем папку для логов:
# sudo mkdir /var/lib/kafka2db
# выдаем полные права на папку:
# sudo chmod 777 /var/lib/kafka2db
PATH_TO_LIB = '/var/lib/kafka2db/'
#
# создаем папку для переменных данного проекта:
# sudo mkdir /var/log/kafka2db
# выдаем полные права на папку:
# sudo chmod 777 /var/log/kafka2db
PATH_TO_LOG = '/var/log/kafka2db/'
#
#
#
# max_minutes_work - максимальное количество минут работы скрипта до остановки (0 - без остановки, int - может понадобиться, чтобы гибче управлять автозапуском)
# ВНИМАНИЕ! реально может работать на несколько минут дольше указанного
# max_minutes_work = 50
max_minutes_work = 1
#
# max_number_msg - какое максимальное количество сообщений обработать за один запуск скрипта (нужно для тестирования, в бою скорее всего ограничивать не надо)
# 0 - неограниченно
# max_number_msg = 0
max_number_msg = 1
#
#
#
# True - Проверять свободное место на диске, False - не проверять
CHECK_DISK_SPACE = True
#
#
#
# TELEGRAM
# True - отправлять результат в телеграм, False - не отправлять
SEND_TELEGRAM = False
# SEND_SUCCESS_REPEATED_NOT_EARLIER_THAN_MINUTES - минимальное количество минут между отправками УСПЕХА (чтобы не заспамить)
SEND_SUCCESS_REPEATED_NOT_EARLIER_THAN_MINUTES = 360
# создать бота - получить токен - создать группу - бота сделать администратором - получить id группы
TLG_BOT_TOKEN = 'your_bot_token'
# TLG_CHAT_FOR_SEND = идентификатор группы
# Как узнать идентификтор группы:
# 1. Добавить бота в нужную группу;
# 2. Написать хотя бы одно сообщение в неё;
# 3. Отправить GET-запрос по следующему адресу:
# curl https://api.telegram.org/bot<your_bot_token>/getUpdates
# 4. Взять значение "id" из объекта "chat". Это и есть идентификатор чата. Для групповых чатов он отрицательный, для личных переписок положительный.
TLG_CHAT_FOR_SEND = -000
