#!/bin/bash
source $(dirname "$(realpath $0)")/../venv/bin/activate && \
        python3 manage.py runserver 3000 &
