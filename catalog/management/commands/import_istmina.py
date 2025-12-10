"""
Import Istmina (AHJCI) data from EAP1477 project.

Data structure:
- collections.csv: Hierarchy (Repository -> Fonds -> Series/Cajas)
- objects.csv: Individual documents (Items)

Usage:
    python manage.py import_istmina /path/to/catalogues/Istmina/data/processed/
    python manage.py import_istmina /path/to/data/ --dry-run
"""

import csv
import re
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from catalog.models import Repository, CatalogUnit


class Command(BaseCommand):
    help = 'Import Istmina (AHJCI) catalog data from EAP1477 CSV files'

    def add_arguments(self, parser):
        parser.add_argument(
            'data_dir',
            type=str,
            help='Path to the processed data directory containing collections.csv and objects.csv'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Parse and validate data without saving to database'
        )

    def handle(self, *args, **options):
        data_dir = Path(options['data_dir'])
        dry_run = options['dry_run']

        collections_file = data_dir / 'collections.csv'
        objects_file = data_dir / 'objects.csv'

        if not collections_file.exists():
            raise CommandError(f'Collections file not found: {collections_file}')
        if not objects_file.exists():
            raise CommandError(f'Objects file not found: {objects_file}')

        self.stdout.write(f'Importing from: {data_dir}')
        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN - no data will be saved'))

        try:
            with transaction.atomic():
                # Create or get repository
                repository = self.get_or_create_repository(dry_run)

                # Import collections (hierarchy)
                collections_map = self.import_collections(collections_file, repository, dry_run)

                # Import objects (items)
                objects_count = self.import_objects(objects_file, collections_map, repository, dry_run)

                if dry_run:
                    raise DryRunComplete()

        except DryRunComplete:
            self.stdout.write(self.style.SUCCESS('Dry run complete - no data saved'))
        except Exception as e:
            raise CommandError(f'Import failed: {e}')

        self.stdout.write(self.style.SUCCESS('Import complete!'))

    def get_or_create_repository(self, dry_run):
        """Create or retrieve the AHJCI repository."""
        if dry_run:
            self.stdout.write('Would create/get repository: AHJCI')
            return None

        repository, created = Repository.objects.get_or_create(
            repository_code='AHJCI',
            defaults={
                'name': 'Archivo Histórico del Juzgado del Circuito de Istmina',
                'abbreviation': 'AHJCI',
                'institution_type': 'judicial_archive',
                'country_code': 'COL',
                'region': 'choco',
                'city': 'Istmina',
                'default_metadata_standard': 'EAP',
                'default_language': 'es',
                'notes': 'EAP1477 digitization project. Judicial records from Chocó region, 1860-1930.',
            }
        )

        if created:
            self.stdout.write(f'Created repository: {repository}')
        else:
            self.stdout.write(f'Using existing repository: {repository}')

        return repository

    def import_collections(self, filepath, repository, dry_run):
        """Import collections.csv to create the hierarchy."""
        self.stdout.write(f'Reading collections from: {filepath}')

        collections_map = {}  # idno -> CatalogUnit

        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        self.stdout.write(f'Found {len(rows)} collection records')

        # Sort by idno length to ensure parents are created before children
        rows.sort(key=lambda r: len(r['collection_idno']))

        for row in rows:
            idno = row['collection_idno'].strip()
            parent_idno = row['parent_collection'].strip() if row['parent_collection'] else None
            title = row['collection_title'].strip()
            description = row['collection_description'].strip() if row['collection_description'] else ''
            date_expr = row['collection_date'].strip() if row['collection_date'] else ''

            # Determine level type based on hierarchy depth
            level_type = self.determine_level_type(idno)

            # Parse dates
            date_start, date_end = self.parse_date_range(date_expr)

            if dry_run:
                self.stdout.write(f'  Would create collection: {idno} ({level_type})')
                collections_map[idno] = {'idno': idno, 'title': title}
                continue

            # Get parent if exists
            parent = None
            if parent_idno and parent_idno in collections_map:
                parent = collections_map[parent_idno]

            unit = CatalogUnit.objects.create(
                repository=repository,
                parent=parent,
                local_identifier=idno,
                title=title,
                description=description,
                date_expression=date_expr,
                date_start=date_start,
                date_end=date_end,
                level_type=level_type,
                metadata_standard='EAP',
            )

            collections_map[idno] = unit
            self.stdout.write(f'  Created: {idno} - {title[:50]}...')

        return collections_map

    def import_objects(self, filepath, collections_map, repository, dry_run):
        """Import objects.csv as items."""
        self.stdout.write(f'Reading objects from: {filepath}')

        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        self.stdout.write(f'Found {len(rows)} object records')

        count = 0
        errors = 0

        for row in rows:
            try:
                obj_id = row['id'].strip()
                parent_idno = row['collection_idno'].strip()
                title = row['document_title'].strip() if row['document_title'] else f'Document {obj_id}'
                description = row['document_summary'].strip() if row['document_summary'] else ''
                date_expr = row['date'].strip() if row['date'] else ''
                extent = row['volume_support'].strip() if row['volume_support'] else ''
                physical_ref = row['physical_reference'].strip() if row['physical_reference'] else ''
                digital_folder = row['digital_folder'].strip() if row['digital_folder'] else ''
                cataloger = row['cataloger'].strip() if row['cataloger'] else ''
                review_date = row['review_date'].strip() if row['review_date'] else ''

                # Parse subjects/topics
                topics = self.parse_pipe_list(row.get('related_topics', ''))
                locations = self.parse_pipe_list(row.get('locations_mentioned', ''))
                people = self.parse_pipe_list(row.get('people_mentioned', ''))

                # Notes
                physical_issues = row.get('physical_issues', '').strip()
                digitization_issues = row.get('digitization_issues', '').strip()
                cataloging_notes = row.get('cataloging_notes', '').strip()

                # Combine notes
                notes_parts = []
                if physical_issues:
                    notes_parts.append(f'Physical issues: {physical_issues}')
                if digitization_issues:
                    notes_parts.append(f'Digitization issues: {digitization_issues}')
                if cataloging_notes:
                    notes_parts.append(f'Cataloging notes: {cataloging_notes}')
                notes = '\n'.join(notes_parts)

                # Parse date
                date_start, date_end = self.parse_date_range(date_expr)

                if dry_run:
                    count += 1
                    if count <= 5:
                        self.stdout.write(f'  Would create item: {obj_id}')
                    elif count == 6:
                        self.stdout.write('  ... (suppressing further output)')
                    continue

                # Get parent
                parent = collections_map.get(parent_idno)
                if not parent:
                    self.stderr.write(f'  Warning: Parent not found for {obj_id}: {parent_idno}')
                    errors += 1
                    continue

                unit = CatalogUnit.objects.create(
                    repository=repository,
                    parent=parent,
                    local_identifier=obj_id,
                    title=title,
                    description=description,
                    date_expression=date_expr,
                    date_start=date_start,
                    date_end=date_end,
                    extent_expression=extent,
                    original_reference=physical_ref,
                    digital_folder_name=digital_folder,
                    level_type='item',
                    metadata_standard='EAP',
                    cataloger_name=cataloger,
                    notes=notes,
                    subjects_topic=topics if topics else None,
                    subjects_geographic=locations if locations else None,
                    subjects_name_string=people if people else None,
                )

                count += 1
                if count % 100 == 0:
                    self.stdout.write(f'  Imported {count} items...')

            except Exception as e:
                self.stderr.write(f'  Error importing {row.get("id", "unknown")}: {e}')
                errors += 1

        self.stdout.write(f'Imported {count} items ({errors} errors)')
        return count

    def determine_level_type(self, idno):
        """Determine ISAD(G) level type from identifier pattern."""
        parts = idno.split('.')
        depth = len(parts)

        if depth == 1:
            return 'fonds'  # AHJCI
        elif depth == 2:
            return 'series'  # AHJCI.MFC
        elif depth == 3:
            return 'file'  # AHJCI.MFC.001 (Caja)
        else:
            return 'item'  # AHJCI.MFC.001.001

    def parse_date_range(self, date_str):
        """Parse date expression into start/end dates."""
        if not date_str:
            return None, None

        date_str = date_str.strip()

        # Handle ranges like "1860 - 1930"
        range_match = re.match(r'(\d{4})\s*-\s*(\d{4})', date_str)
        if range_match:
            try:
                from datetime import date
                start_year = int(range_match.group(1))
                end_year = int(range_match.group(2))
                return date(start_year, 1, 1), date(end_year, 12, 31)
            except ValueError:
                return None, None

        # Handle single year like "1877"
        year_match = re.match(r'^(\d{4})$', date_str)
        if year_match:
            try:
                from datetime import date
                year = int(year_match.group(1))
                return date(year, 1, 1), date(year, 12, 31)
            except ValueError:
                return None, None

        # Handle uncertain dates like "189?"
        uncertain_match = re.match(r'^(\d{3})\?$', date_str)
        if uncertain_match:
            try:
                from datetime import date
                decade = int(uncertain_match.group(1)) * 10
                return date(decade, 1, 1), date(decade + 9, 12, 31)
            except ValueError:
                return None, None

        return None, None

    def parse_pipe_list(self, value):
        """Parse pipe-separated values into a list."""
        if not value:
            return []
        return [item.strip() for item in value.split('|') if item.strip()]


class DryRunComplete(Exception):
    """Raised to rollback transaction during dry run."""
    pass
