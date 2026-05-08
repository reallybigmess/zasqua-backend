#!/usr/bin/bash
# This script invokes 2 migration commands to output directly to the default zasqua-frontend export folder. That's it.

# front and backend paths 
zfpath="$HOME/code/zasqua-frontend"
zbpath="$HOME/code/zasqua-backend"

cd $zfpath
echo "Removing previous data and making a backup..."
tar -czf "$zfpath/backup/exports-$(date +%Y-%m-%d_%H%M%S).tar.gz" exports
rm -r exports
cd $zbpath
source venv/bin/activate && \
	python manage.py export_frontend_data --output-dir $zfpath/exports && python manage.py export_entity_place --output-dir $zfpath/exports
