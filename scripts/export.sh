#!/usr/bin/bash
# This script invokes migrate.py export_frontend_data to output directly to the default zasqua-frontend data folder. That's it.

# front and backend paths 
zfpath="$HOME/code/zasqua-frontend"
zbpath="$HOME/code/zasqua-backend"

cd $zfpath
echo "Removing previous data and making a backup..."
tar -czf "$zfpath/backup/data-$(date +%Y-%m-%d_%H%M%S).tar.gz" data
rm -r data
cd $zbpath
source venv/bin/activate && \
	python manage.py export_frontend_data --output-dir $zfpath/data/
