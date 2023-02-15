#!/bin/bash
cd /opt/dix/kafka2db/
PATH="/opt/dix/kafka2db/.venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
VIRTUAL_ENV="/opt/dix/kafka2db/.venv"

pipenv run python3 kafka2db.py
