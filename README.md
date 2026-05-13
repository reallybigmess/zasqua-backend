## What is this?
Django backend to [Zasqua](https://github.com/neogranadina/zasqua). Generates the bare minimum files you need to run the frontend locally. 

All modifications are done organically by me and I'm not associated with the main Zasqua project, AMPL, etc.

## Known issues/areas to improve
- IIIF/METS management commands don't work, you can still show IIIF though -- see the IIIF generation section below.
- Models are permanently changed with this fork and that also changes the data zasqua-frontend operates on.

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
- Copy everything in exports/ to **export/** (note the folder names) in your zasqua-frontend base folder
- [Run the frontend](https://github.com/reallybigmess/zasqua-frontend#getting-started)

## (New!) Quick IIIF Generation:
This is just one way to externally generate manifests which worked for this project given my use case. You will need your source objects available locally as files, which will be mapped to individual description pages in Zasqua.  

Steps:
- Acquire [mkiiif](https://github.com/atomotic/iiif) and throw the binary somewhere
- Run mkiiif like so, substituting -id, -base, -title, -source, and -destination: `./mkiiif -id [YOURITEMID] -base [YOURHOST]/data/manifests -title [YOURTITLE] -source ../[YOURSOURCE] -destination manifests`
- (**Optional**): Add `-tiles` to have mkiiif generate those, but be aware it adds to the overall file size (that you will then be hosting statically.)
- (**Optional**): Add `-resolution [XX]` to change DPI/resolution of generated images, this also impacts file size.
- Copy manifests/ and its subfolders to the Zasqua **frontend** static/data/ folder.
- Now hop over to the backend. Go to the Descriptions catalog and find your item -> Make sure the digital checkbox is checked -> scroll down to Digital -> Iiif manifest url, add the URL like so:  [YOURHOST]/data/manifests/[YOURITEMID]/manifest.json
- Run `python manage.py export_frontend_data` and copy the contents of exports/, rebuild your Zasqua site and you should hopefully see things on your IIIF-represented content.

## To-Do
- EAD importer, maybe with pyead
- Overhaul/create a different IIIF management command
- Read over models.py Descriptions 
- Figure out model internationalization stuff so that the language changes in models.py are additive instead of swapping Spanish to English

## Notes
- utf8mb4 character set for DB. MariaDB wants `utf8mb4_unicode_ci` collation
- added django-cors-headers and mysqlclient to requirements.txt