"""
Import Rionegro (AHR) data from CollectiveAccess export.

Data structure: 4-level hierarchy in rionegro_collections.csv
- Institution (CO.AHR)
- Fondo (CO.AHR.GOB, etc.)
- Subfondo/Caja (CO.AHR.GOB.T255, etc.)
- Carpeta (CO.AHR.GOB.T255.T001, etc.)

Usage:
    python manage.py import_rionegro /path/to/catalogues/archivo-historico-rionegro/data/csv/
    python manage.py import_rionegro /path/to/data/ --dry-run
"""

import csv
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from catalog.models import Repository, CatalogUnit


class Command(BaseCommand):
    help = 'Import Rionegro (AHR) catalog data from CollectiveAccess CSV export'

    def add_arguments(self, parser):
        parser.add_argument(
            'data_dir',
            type=str,
            help='Path to the CSV data directory'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Parse and validate data without saving to database'
        )

    def handle(self, *args, **options):
        data_dir = Path(options['data_dir'])
        dry_run = options['dry_run']

        collections_file = data_dir / 'rionegro_collections.csv'

        if not collections_file.exists():
            raise CommandError(f'Collections file not found: {collections_file}')

        self.stdout.write(f'Importing from: {data_dir}')
        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN - no data will be saved'))

        try:
            with transaction.atomic():
                repository = self.get_or_create_repository(dry_run)
                count = self.import_collections(collections_file, repository, dry_run)

                if dry_run:
                    raise DryRunComplete()

        except DryRunComplete:
            self.stdout.write(self.style.SUCCESS('Dry run complete - no data saved'))
        except Exception as e:
            raise CommandError(f'Import failed: {e}')

        self.stdout.write(self.style.SUCCESS(f'Import complete! {count} records imported.'))

    def get_or_create_repository(self, dry_run):
        """Create or retrieve the AHR repository."""
        if dry_run:
            self.stdout.write('Would create/get repository: AHR')
            return None

        repository, created = Repository.objects.get_or_create(
            repository_code='AHR',
            defaults={
                'name': 'Archivo Histórico de Rionegro',
                'abbreviation': 'AHR',
                'institution_type': 'municipal_archive',
                'country_code': 'COL',
                'region': 'antioquia',
                'city': 'Rionegro',
                'default_metadata_standard': 'ISADG',
                'default_language': 'es',
                'notes': 'Municipal historical archive of Rionegro, Antioquia.',
            }
        )

        if created:
            self.stdout.write(f'Created repository: {repository}')
        else:
            self.stdout.write(f'Using existing repository: {repository}')

        return repository

    def import_collections(self, filepath, repository, dry_run):
        """Import rionegro_collections.csv building the hierarchy."""
        self.stdout.write(f'Reading collections from: {filepath}')

        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        self.stdout.write(f'Found {len(rows)} rows')

        # Build hierarchy - track created units
        units_map = {}  # idno -> CatalogUnit
        count = 0

        # First pass: collect unique hierarchy levels
        hierarchy_levels = {}  # idno -> data

        for row in rows:
            # Institution level
            inst_idno = row.get('ca_collections.idno_institucion', '').strip()
            inst_name = row.get('ca_collections.preferred_labels_institucion', '').strip()
            if inst_idno and inst_idno not in hierarchy_levels:
                hierarchy_levels[inst_idno] = {
                    'idno': inst_idno,
                    'title': inst_name,
                    'level_type': 'fonds',
                    'parent_idno': None,
                }

            # Fondo level
            fondo_idno = row.get('ca_collections.idno_fondo', '').strip()
            fondo_name = row.get('ca_collections.preferred_labels_fondo', '').strip()
            if fondo_idno and fondo_idno not in hierarchy_levels:
                hierarchy_levels[fondo_idno] = {
                    'idno': fondo_idno,
                    'title': fondo_name,
                    'level_type': 'series',
                    'parent_idno': inst_idno,
                }

            # Subfondo/Caja level
            subfondo_idno = row.get('ca_collections.idno_subfondo', '').strip()
            subfondo_name = row.get('ca_collections.preferred_labels_subfondo', '').strip()
            if subfondo_idno and subfondo_idno not in hierarchy_levels:
                hierarchy_levels[subfondo_idno] = {
                    'idno': subfondo_idno,
                    'title': subfondo_name,
                    'level_type': 'file',
                    'parent_idno': fondo_idno,
                }

            # Item/Carpeta level - this is the actual row data
            idno = row.get('ca_collections.idno', '').strip()
            if idno and idno not in hierarchy_levels:
                hierarchy_levels[idno] = {
                    'idno': idno,
                    'title': row.get('ca_collections.preferred_labels', '').strip() or f'Item {idno}',
                    'level_type': 'item',
                    'parent_idno': subfondo_idno,
                    'description': row.get('ca_collections.scopecontent', '').strip(),
                    'extent': row.get('ca_collections.extent_text', '').strip(),
                    'access_conditions': row.get('ca_collections.accessrestrict', '').strip(),
                    'reproduction': row.get('ca_collections.reproduction', '').strip(),
                    'notes': row.get('ca_collections.note', '').strip(),
                    'date_expression': row.get('ca_collections.unitdate.date_value', '').strip(),
                    'places': row.get('ca_places', '').strip(),
                    'subjects': row.get('ca_collections.description', '').strip(),
                    'related': row.get('ca_collections.relatedmaterial', '').strip(),
                }

        self.stdout.write(f'Found {len(hierarchy_levels)} unique hierarchy entries')

        # Sort by depth (number of dots in idno)
        sorted_items = sorted(hierarchy_levels.values(), key=lambda x: x['idno'].count('.'))

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
            parent = None
            if parent_idno and parent_idno in units_map:
                parent = units_map[parent_idno]

            # Parse access conditions
            access = None
            access_text = item.get('access_conditions', '')
            if 'libre' in access_text.lower():
                access = 'open'
            elif 'restringido' in access_text.lower():
                access = 'restricted'

            # Parse dates
            date_start, date_end = self.parse_date_range(item.get('date_expression', ''))

            unit = CatalogUnit.objects.create(
                repository=repository,
                parent=parent,
                local_identifier=idno,
                title=title,
                description=item.get('description', ''),
                extent_expression=item.get('extent', ''),
                date_expression=item.get('date_expression', ''),
                date_start=date_start,
                date_end=date_end,
                level_type=item['level_type'],
                metadata_standard='ISADG',
                access_conditions=access or '',
                reproduction_conditions=item.get('reproduction', ''),
                notes=item.get('notes', ''),
                subjects_geographic=[item['places']] if item.get('places') else None,
                subjects_topic=[item['subjects']] if item.get('subjects') else None,
            )

            units_map[idno] = unit
            count += 1

            if count % 500 == 0:
                self.stdout.write(f'  Imported {count} records...')

        return count

    def parse_date_range(self, date_str):
        """Parse date expression into start/end dates."""
        import re
        from datetime import date

        if not date_str:
            return None, None

        date_str = date_str.strip()

        # Handle ranges like "1757 - 1917"
        range_match = re.match(r'(\d{4})\s*-\s*(\d{4})', date_str)
        if range_match:
            try:
                start_year = int(range_match.group(1))
                end_year = int(range_match.group(2))
                return date(start_year, 1, 1), date(end_year, 12, 31)
            except ValueError:
                return None, None

        # Handle single year
        year_match = re.match(r'^(\d{4})$', date_str)
        if year_match:
            try:
                year = int(year_match.group(1))
                return date(year, 1, 1), date(year, 12, 31)
            except ValueError:
                return None, None

        return None, None


class DryRunComplete(Exception):
    """Raised to rollback transaction during dry run."""
    pass
