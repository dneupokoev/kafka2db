# kafka2db

Kafka to DB PostgreSQL: Переливка данных из топика кафки в базу данных PostgreSQL

Для работы потребуется Linux (тестирование проводилось на ubuntu 22.04.01). При необходимости переписать под windows, вероятно, вам не составит большого труда.

Сначала всё настроить, потом вручную запускать проект: ```kafka2db_start.sh```

Для автоматизации можно настроить (например, через cron) выполнение скрипта ```kafka2db.py```

### Кратко о том как устроена работа kafka2db:

- В Кафку кто-то пишет нужную информацию в формате json
- При запуске kafka2db начинает читать топики (настройки и их описания содержатся в settings.py), преобразует json в sql-запросы для PostgreSQL и выполняет эти запросы в PostgreSQL.
- Логирование что получено, а что нет на стороне Кафки! Скрипт только обрабатывает полученные сообщения из топиков 

### Кратко весь процесс настройки:

- Создать таблицы в PostgreSQL (для создания таблиц выполнить скрипт из проекта с учетом своих настроек)
- Настроить работу скрипта kafka2db (настроить как описано в текущей инструкции и в файле settings.py, но с учётом особенностей вашей системы)

***ВНИМАНИЕ! Пути до каталогов и файлов везде указывайте свои!***

### PostgreSQL

- Для создания структуры выполнить скрипт: script_create_sql_table_ai.sql (ВНИМАНИЕ!!! сначала изучи скрипт!)
- Если потребуются дополнительные таблицы, то создайте и впишите в скрипт

### Установка kafka2db (выполняем пошагово)

- Устанавливаем python (тестирование данной инструкции проводилось на 3.10, на остальных версиях работу не гарантирую, но должно работать на версиях 3.9+, если
  вам потребуется, то без особенного труда сможете переписать даже под 2.7)
- Устанавливаем pip:

```sudo apt install python3-pip```

- Далее устанавливаем pipenv (на linux):

```pip3 install pipenv```

- Создаем нужный каталог в нужном нам месте
- Копируем в этот каталог файлы проекта https://github.com/dneupokoev/kafka2db
- Заходим в созданный каталог и создаем в нем пустой каталог .venv
- В каталоге проекта выполняем команды (либо иным способом устанавливаем пакеты из requirements.txt):

```pipenv shell```

```pipenv sync```

- Редактируем и переименовываем файл _settings.py (описание внутри файла)
- Настраиваем регулярное выполнение (например, через cron) скрипта:

```kafka2db.py```

### Дополнительно

- Обратите внимание на описание внутри _settings.py - там все настройки
- Если работает экземпляр программы, то второй экземпляр запускаться не будет (отслеживается через создание и проверку наличия файла)
- Записывается лог ошибок (настраивается в settings, рекомендуется сюда: /var/log/kafka2db/)
- Если задать нужные настройки в settings, то результат работы будет присылать в телеграм (в личку или указанный канал)
- Можно включить (в settings) вывод информации о дисковом пространстве

### Добавления задания в cron

Смотрим какие задания уже созданы для данного пользователя:

```crontab -l```

Открываем файл для создания задания:

```crontab -e```

Каждая задача формируется следующим образом (для любого значения нужно использовать звездочку "*"):

```минута(0-59) час(0-23) день(1-31) месяц(1-12) день_недели(0-7) /полный/путь/к/команде```

Чтобы kafka2db запускался каждый час ровно в 7 минут, создаем строку и сохраняем файл:

```7 */1 * * * /opt/dix/kafka2db/kafka2db_cron.sh```

ВНИМАНИЕ!!! отредактируйте содержимое файла kafka2db_cron.sh и сделайте его исполняемым

### ВНИМАНИЕ!

- Чтобы запись из Кафки не заливалась в базу, в json нужно указать параметр "is_test" с любым значением: скрипт обработает такое сообщение как тестовое и не будет заливать в базу. 
- За один запуск kafka2db переливает не все сообщения, а только то, что вы настроите в ```settings.py``` (например, можно настроить время работы и/или
  количество сообщений). Не рекомендуется слишком долгое выполнение. Например, для тестирования достаточно 5-10 минут. В прод: запускать раз в
  час на 50 минут.