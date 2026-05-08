## What is this?
Django backend to [Zasqua](https://github.com/neogranadina/zasqua). Generates the bare minimum files you need to run the frontend locally. 

All modifications are done organically by me and I'm not associated with the wider Zasqua project, AMPL, etc.

## Known issues/areas to improve
- IIIF/METS stuff doesn't work, though IIIF certainly works with the frontend, just not manifest generation.
- Add pyead -> EAD importer command
- Remove or generalize hardcoded stuff out of models
- Not internationalized 

## Getting Started
Prerequisites: Python 3.11+, MySQL 8.0
`apt install python3.12-venv pkgconfig python3-dev default-libmysqlclient-dev build-essential`

### First-time Backend Setup:
- `git clone https://github.com/reallybigmess/zasqua-backend.git && cd zasqua-backend`

### MySQL:
Quick db setup:
`CREATE USER 'zasqua'@'localhost' IDENTIFIED BY 'XXXXXXXXX';`
`CREATE DATABASE zasqua CHARACTER SET utf8mb4`
`GRANT ALL PRIVILEGES ON zasqua.* TO 'zasqua'@'localhost';`
`FLUSH PRIVILEGES;`

### Python:
`python -m venv venv && source venv/bin/activate`
`pip install -r requirements.txt`
`cp .env.example .env && vim .env` add your own db stuff
`python manage.py migrate`
`python manage.py runserver`

## Export:
- `python manage.py export_frontend_data` exports descriptions.json, repositories.json, children/*
- `python manage.py export_entity_place` exports entities.json, places.json, entity_links.json, place_links.json
- Copy everything in export/ to **export/** in your zasqua-frontend base folder
- [Run the frontend](https://github.com/reallybigmess/zasqua-frontend#getting-started)

## To-Do
- Learn model stuff
- Figure out how Django internationalization works

## Notes
- utf8mb4 character set for DB. MariaDB wants `utf8mb4_unicode_ci` collation
- added django-cors-headers and mysqlclient to requirements.txt
