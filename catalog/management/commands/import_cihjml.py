"""
Import CIHJML data from Neogranadina processed CSV.

Data structure: 5-level hierarchy
- Institution (cihjml)
- Fondo (cihjml.acc)
- Section (cihjml.acc.col)
- Series (cihjml.acc.col.c1)
- Subseries (cihjml.acc.col.c1.a)
- Items (cihjml.acc.col.c1.a.civil - individual records)

Usage:
    python manage.py import_cihjml "/path/to/cihjml-procesado - cihjml-procesado.csv"
    python manage.py import_cihjml /path/to/file.csv --dry-run
"""

import csv
import re
from datetime import date
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from catalog.models import Repository, CatalogUnit, Place, CatalogUnitPlace


class Command(BaseCommand):
    help = 'Import CIHJML catalog data from Neogranadina CSV'

    def add_arguments(self, parser):
        parser.add_argument(
            'csv_file',
            type=str,
            help='Path to the CIHJML CSV file'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Parse and validate data without saving to database'
        )
        parser.add_argument(
            '--skip-places',
            action='store_true',
            help='Skip importing place data'
        )

    def handle(self, *args, **options):
        csv_file = Path(options['csv_file'])
        dry_run = options['dry_run']
        skip_places = options['skip_places']

        if not csv_file.exists():
            raise CommandError(f'CSV file not found: {csv_file}')

        self.stdout.write(f'Importing from: {csv_file}')
        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN - no data will be saved'))

        try:
            with transaction.atomic():
                repository = self.get_or_create_repository(dry_run)

                # Import places first (if not skipping)
                places_map = {}
                if not skip_places:
                    places_map = self.import_places(csv_file, dry_run)

                # Import hierarchy and items
                count = self.import_records(csv_file, repository, places_map, dry_run)

                if dry_run:
                    raise DryRunComplete()

        except DryRunComplete:
            self.stdout.write(self.style.SUCCESS('Dry run complete - no data saved'))
        except Exception as e:
            import traceback
            traceback.print_exc()
            raise CommandError(f'Import failed: {e}')

        self.stdout.write(self.style.SUCCESS(f'Import complete! {count} records imported.'))

    def get_or_create_repository(self, dry_run):
        """Create or retrieve the CIHJML repository."""
        if dry_run:
            self.stdout.write('Would create/get repository: CIHJML')
            return None

        repository, created = Repository.objects.get_or_create(
            repository_code='CIHJML',
            defaults={
                'name': 'Centro de Investigaciones Históricas José María Arboleda Llorente',
                'abbreviation': 'CIHJML',
                'institution_type': 'university',
                'country_code': 'COL',
                'region': 'cauca',
                'city': 'Popayán',
                'default_metadata_standard': 'ISADG',
                'default_language': 'es',
                'notes': 'Historical archive of the Universidad del Cauca. Contains colonial records from the Audiencia de Quito region.',
            }
        )

        if created:
            self.stdout.write(f'Created repository: {repository}')
        else:
            self.stdout.write(f'Using existing repository: {repository}')

        return repository

    def import_places(self, csv_file, dry_run):
        """Extract and import unique places from the CSV."""
        self.stdout.write('Extracting places...')

        places_map = {}  # gz_id -> Place

        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for row in reader:
                gz_ids = row.get('gz_id', '').strip()
                labels = row.get('label', '').strip()
                nombres = row.get('nombre', '').strip()
                categorias = row.get('categoria', '').strip()
                lats = row.get('lat', '').strip()
                lons = row.get('lon', '').strip()
                partidos = row.get('partido_generico', '').strip()
                provincias = row.get('provincia_generica', '').strip()
                regiones = row.get('region', '').strip()

                if not gz_ids:
                    continue

                # Split pipe-separated values
                gz_id_list = gz_ids.split('|')
                label_list = labels.split('|') if labels else []
                nombre_list = nombres.split('|') if nombres else []
                cat_list = categorias.split('|') if categorias else []
                lat_list = lats.split('|') if lats else []
                lon_list = lons.split('|') if lons else []
                partido_list = partidos.split('|') if partidos else []
                provincia_list = provincias.split('|') if provincias else []
                region_list = regiones.split('|') if regiones else []

                for i, gz_id in enumerate(gz_id_list):
                    gz_id = gz_id.strip()
                    if not gz_id or gz_id in places_map:
                        continue

                    label = label_list[i].strip() if i < len(label_list) else ''
                    nombre = nombre_list[i].strip() if i < len(nombre_list) else ''
                    categoria = cat_list[i].strip() if i < len(cat_list) else ''
                    lat = self.parse_coordinate(lat_list[i]) if i < len(lat_list) else None
                    lon = self.parse_coordinate(lon_list[i]) if i < len(lon_list) else None
                    partido = partido_list[i].strip() if i < len(partido_list) else ''
                    provincia = provincia_list[i].strip() if i < len(provincia_list) else ''
                    region = region_list[i].strip() if i < len(region_list) else ''

                    places_map[gz_id] = {
                        'gz_id': gz_id,
                        'label': label,
                        'historical_name': nombre,
                        'place_type': categoria,
                        'latitude': lat,
                        'longitude': lon,
                        'historical_admin_1': partido,
                        'historical_admin_2': provincia,
                        'historical_region': region,
                    }

        self.stdout.write(f'Found {len(places_map)} unique places')

        if dry_run:
            return places_map

        # Create Place objects
        created_places = {}
        for gz_id, data in places_map.items():
            place, _ = Place.objects.get_or_create(
                gazetteer_id=gz_id,
                defaults={
                    'gazetteer_source': 'CIHJML',
                    'label': data['label'] or data['historical_name'] or gz_id,
                    'historical_name': data['historical_name'],
                    'place_type': data['place_type'] if data['place_type'] in ['city', 'town', 'village', 'region', 'province'] else 'other',
                    'latitude': data['latitude'],
                    'longitude': data['longitude'],
                    'country_code': 'COL',
                    'historical_admin_1': data['historical_admin_1'],
                    'historical_admin_2': data['historical_admin_2'],
                    'historical_region': data['historical_region'],
                }
            )
            created_places[gz_id] = place

        self.stdout.write(f'Created/found {len(created_places)} places')
        return created_places

    def parse_coordinate(self, coord_str):
        """Parse coordinate string to decimal."""
        if not coord_str:
            return None
        coord_str = coord_str.strip()
        # Handle nan values
        if coord_str.lower() == 'nan' or coord_str == '':
            return None
        try:
            # Handle formats like "244.185.199.987" (malformed) or "2.44185199987"
            if coord_str.count('.') > 1:
                # Probably malformed, try to fix
                parts = coord_str.split('.')
                if len(parts) >= 2:
                    coord_str = parts[0] + '.' + ''.join(parts[1:])
            val = float(coord_str)
            # Check for nan float
            import math
            if math.isnan(val):
                return None
            return val
        except ValueError:
            return None

    def import_records(self, csv_file, repository, places_map, dry_run):
        """Import the CSV records building the hierarchy."""
        self.stdout.write('Importing records...')

        units_map = {}  # idno -> CatalogUnit
        count = 0

        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        self.stdout.write(f'Found {len(rows)} rows')

        # First pass: build hierarchy structure
        hierarchy = {}  # idno -> data

        for row in rows:
            # Institution
            inst_id = row.get('institucion_id', '').strip()
            inst_name = row.get('institucion_nombre', '').strip()
            if inst_id and inst_id not in hierarchy:
                hierarchy[inst_id] = {
                    'idno': inst_id,
                    'title': inst_name,
                    'level_type': 'fonds',
                    'parent_idno': None,
                }

            # Fondo
            fondo_id = row.get('fondo_id', '').strip()
            fondo_name = row.get('fondo_nombre', '').strip()
            if fondo_id and fondo_id not in hierarchy:
                hierarchy[fondo_id] = {
                    'idno': fondo_id,
                    'title': fondo_name,
                    'level_type': 'subfonds',
                    'parent_idno': inst_id,
                }

            # Subfondo/Section
            subfondo_id = row.get('subfondo_id', '').strip()
            subfondo_name = row.get('subfondo_nombre', '').strip()
            if subfondo_id and subfondo_id not in hierarchy:
                hierarchy[subfondo_id] = {
                    'idno': subfondo_id,
                    'title': subfondo_name,
                    'level_type': 'section',
                    'parent_idno': fondo_id,
                }

            # Serie
            serie_id = row.get('serie_id', '').strip()
            serie_name = row.get('serie_nombre', '').strip()
            if serie_id and serie_id not in hierarchy:
                hierarchy[serie_id] = {
                    'idno': serie_id,
                    'title': serie_name,
                    'level_type': 'series',
                    'parent_idno': subfondo_id,
                }

            # Subserie
            subserie_id = row.get('subserie_id', '').strip()
            subserie_name = row.get('subserie_nombre', '').strip()
            if subserie_id and subserie_id not in hierarchy:
                hierarchy[subserie_id] = {
                    'idno': subserie_id,
                    'title': subserie_name,
                    'level_type': 'subseries',
                    'parent_idno': serie_id,
                }

            # Item (the actual document)
            item_id = row.get('unidad_compuesta', '').strip()
            if item_id and item_id not in hierarchy:
                hierarchy[item_id] = {
                    'idno': item_id,
                    'title': row.get('ca_objects.preferred_labels', '').strip(),
                    'level_type': 'item',
                    'parent_idno': subserie_id,
                    'original_reference': row.get('NG_signatura_original', '').strip(),
                    'description': row.get('scopeAndContent', '').strip(),
                    'extent': row.get('extentAndMedium', '').strip(),
                    'date_expression': row.get('ca_objects.unitdate.date_value', '').strip(),
                    'event_dates': row.get('eventDates', '').strip(),
                    'event_start': row.get('eventStartDates', '').strip(),
                    'event_end': row.get('eventEndDates', '').strip(),
                    'language': row.get('language', '').strip(),
                    'script': row.get('script', '').strip(),
                    'finding_aids': row.get('OtherFindingAids', '').strip(),
                    'location_originals': row.get('locationOfOriginals', '').strip(),
                    'location_copies': row.get('locationOfCopies', '').strip(),
                    'subjects': row.get('ca_objects.subject', '').strip(),
                    'notes': row.get('ObjectNotes', '').strip(),
                    'description_control': row.get('DescriptionControl', '').strip(),
                    'related_entities': row.get('related_entities', '').strip(),
                    'related_entities_kind': row.get('related_entities_kind', '').strip(),
                    'gz_ids': row.get('gz_id', '').strip(),
                    'places_label': row.get('ca_places', '').strip(),
                }

        self.stdout.write(f'Found {len(hierarchy)} unique hierarchy entries')

        # Sort by depth
        sorted_items = sorted(hierarchy.values(), key=lambda x: x['idno'].count('.'))

        # Create units
        for item in sorted_items:
            idno = item['idno']
            parent_idno = item['parent_idno']
            title = item['title'] or f'Untitled ({idno})'

            if dry_run:
                if count < 10:
                    self.stdout.write(f"  Would create: {idno} ({item['level_type']})")
                elif count == 10:
                    self.stdout.write('  ... (suppressing further output)')
                count += 1
                units_map[idno] = {'idno': idno}
                continue

            # Get parent
            parent = units_map.get(parent_idno) if parent_idno else None

            # Parse dates
            date_expr = item.get('date_expression', '') or item.get('event_dates', '')
            date_start = self.parse_date(item.get('event_start', ''))
            date_end = self.parse_date(item.get('event_end', ''))

            # Parse subjects
            subjects = [s.strip() for s in item.get('subjects', '').split('|') if s.strip()] if item.get('subjects') else None

            # Parse language
            lang_codes = None
            lang = item.get('language', '').strip().lower()
            if lang:
                lang_codes = ['es'] if 'es' in lang else [lang[:3]]

            unit = CatalogUnit.objects.create(
                repository=repository,
                parent=parent,
                local_identifier=idno,
                original_reference=item.get('original_reference', ''),
                title=title,
                description=item.get('description', ''),
                extent_expression=item.get('extent', ''),
                date_expression=date_expr,
                date_start=date_start,
                date_end=date_end,
                level_type=item['level_type'],
                metadata_standard='ISADG',
                language_codes=lang_codes,
                finding_aids=item.get('finding_aids', ''),
                location_of_originals=item.get('location_originals', ''),
                location_of_copies=item.get('location_copies', ''),
                notes=item.get('notes', ''),
                subjects_topic=subjects,
                subjects_geographic=[item['places_label']] if item.get('places_label') else None,
                statement_of_responsibility=item.get('description_control', ''),
            )

            # Link to places
            if places_map and item.get('gz_ids'):
                gz_ids = [g.strip() for g in item['gz_ids'].split('|') if g.strip()]
                for gz_id in gz_ids:
                    place = places_map.get(gz_id)
                    if place:
                        CatalogUnitPlace.objects.get_or_create(
                            catalog_unit=unit,
                            place=place,
                            defaults={'place_role': 'mentioned'}
                        )

            units_map[idno] = unit
            count += 1

            if count % 1000 == 0:
                self.stdout.write(f'  Imported {count} records...')

        return count

    def parse_date(self, date_str):
        """Parse date string to date object."""
        if not date_str:
            return None

        date_str = date_str.strip()

        # Try YYYY-MM-DD format
        try:
            parts = date_str.split('-')
            if len(parts) == 3:
                return date(int(parts[0]), int(parts[1]), int(parts[2]))
            elif len(parts) == 1 and len(date_str) == 4:
                return date(int(date_str), 1, 1)
        except (ValueError, IndexError):
            pass

        return None


class DryRunComplete(Exception):
    """Raised to rollback transaction during dry run."""
    pass
