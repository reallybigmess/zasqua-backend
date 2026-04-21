## What is this?
Local modifications to zasqua-backend.

## Getting Started
Prerequisites: Python 3.11+, MySQL 8.0
`apt install python3.12-venv pkgconfig python3-dev default-libmysqlclient-dev build-essential`

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



### Backend Setup:
- `git clone https://github.com/reallybigmess/zasqua-backend.git && cd zasqua-backend`

## TODO
- Learn model stuff
- Figure out how Django internationalization works

## NOTES
- utf8mb4 character set for DB. MariaDB wants `utf8mb4_unicode_ci` collation
- added django-cors-headers and mysqlclient to requirements.txt
