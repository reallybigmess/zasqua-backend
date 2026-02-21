"""
Import place authority from M1 extraction.

Replaces ALL existing Place records and DescriptionPlace links with fresh
data built from places.csv produced by the M1 title/entity batch.

Usage:
    python manage.py import_m1_places
    python manage.py import_m1_places --dry-run
    python manage.py import_m1_places --csv-path /path/to/places.csv
"""

import csv
import re
import sys
import time
import unicodedata
from collections import defaultdict

from django.core.management.base import BaseCommand

from catalog.models import (
    Description, DescriptionPlace, Place, generate_neogranadina_code,
)


# Known equivalences: less-frequent normalized form → canonical normalized key.
# Canonical is whichever form appears more often in the data.
# Ambiguous cases (santafe vs santa fe, colonial vs modern names) are deferred
# to M2.5 LOD linking.
PLACE_ALIASES = {
    # Santafé de Bogotá — 'santafe de bogota' (70) is more frequent
    'santa fe de bogota': 'santafe de bogota',
    'santa fe de bogota (ciudad)': 'santafe de bogota',
    'santafe de bogota (ciudad)': 'santafe de bogota',
    # Cartagena — 'cartagena' (718) is far more frequent
    'cartagena de indias': 'cartagena',
}


def normalize_name(name):
    """Normalize place name for deduplication."""
    name = unicodedata.normalize('NFD', name.strip())
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
    name = re.sub(r'\s+', ' ', name).strip().lower()
    return PLACE_ALIASES.get(name, name)


# M1 role → DescriptionPlace.role
ROLE_MAP = {
    'mentioned': 'mentioned',
    'subject': 'subject',
    'origin': 'sent_from',
    'origin of shipment': 'sent_from',
    'destination': 'sent_to',
    'recipient': 'sent_to',
    'venue': 'venue',
    'created': 'created',
    'sent_from': 'sent_from',
    'sent_to': 'sent_to',
    'published': 'published',
}

DEFAULT_CSV_PATH = (
    '/Users/juancobo/Databases/zasqua/zasqua-dev-notes/'
    'releases/0.4.0/title-entity-cleanup/review/places.csv'
)

BATCH_SIZE = 500


class Command(BaseCommand):
    help = 'Import place authority from M1 extraction (replaces all existing place data)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be done without making changes',
        )
        parser.add_argument(
            '--csv-path',
            default=DEFAULT_CSV_PATH,
            help='Path to places.csv (default: M1 review output)',
        )

    # ------------------------------------------------------------------ #
    # Logging helpers                                                       #
    # ------------------------------------------------------------------ #

    def log(self, message, style=None, newline=True):
        if style:
            message = style(message)
        if newline:
            self.stdout.write(message)
        else:
            self.stdout.write(message, ending='')
        sys.stdout.flush()

    def log_phase(self, phase_name):
        self.log(f'\n{"=" * 60}')
        self.log(f'  {phase_name}')
        self.log(f'{"=" * 60}')
        self._phase_start = time.time()

    def log_elapsed(self):
        elapsed = time.time() - self._phase_start
        self.log(f'  Elapsed: {elapsed:.1f}s')

    # ------------------------------------------------------------------ #
    # Main                                                                  #
    # ------------------------------------------------------------------ #

    def handle(self, *args, **options):
        self.dry_run = options['dry_run']
        csv_path = options['csv_path']

        if self.dry_run:
            self.log(self.style.WARNING('DRY RUN — no changes will be made'))

        rows = self._load_csv(csv_path)
        unique_places = self._deduplicate(rows)
        self._delete_existing()
        name_to_id = self._create_places(unique_places)
        self._create_links(rows, name_to_id)
        self._summary(unique_places)

    # ------------------------------------------------------------------ #
    # Phase 1: Load CSV                                                     #
    # ------------------------------------------------------------------ #

    def _load_csv(self, csv_path):
        self.log_phase('Phase 1: Loading places.csv')
        self.log('  Counting rows...', newline=False)
        with open(csv_path, 'r', encoding='utf-8') as f:
            total = sum(1 for _ in f) - 1
        self.log(f' {total:,} rows')

        rows = []
        with open(csv_path, 'r', encoding='utf-8', newline='') as f:
            for row in csv.DictReader(f):
                rows.append(row)
        self.log(f'  Loaded {len(rows):,} rows')
        self.log_elapsed()
        return rows

    # ------------------------------------------------------------------ #
    # Phase 2: Deduplicate                                                  #
    # ------------------------------------------------------------------ #

    def _deduplicate(self, rows):
        self.log_phase('Phase 2: Deduplicating by normalized name')

        # Key: normalized name → canonical name (most frequent form)
        name_counts = defaultdict(lambda: defaultdict(int))
        unknown_roles = defaultdict(int)

        for row in rows:
            name = row['name'].strip()
            raw_role = row.get('role', 'mentioned').strip().lower()

            if raw_role not in ROLE_MAP:
                unknown_roles[raw_role] += 1

            norm = normalize_name(name)
            name_counts[norm][name] += 1

        if unknown_roles:
            self.log(self.style.WARNING('  Unknown roles (will map to "mentioned"):'))
            for r, n in sorted(unknown_roles.items(), key=lambda x: -x[1]):
                self.log(f'    {r!r}: {n}')

        unique_places = []
        for norm, counts in name_counts.items():
            canonical = max(counts, key=lambda n: counts[n])
            unique_places.append({
                'norm_key': norm,
                'canonical_name': canonical,
            })

        self.log(f'  Unique places: {len(unique_places):,}')
        self.log_elapsed()
        return unique_places

    # ------------------------------------------------------------------ #
    # Phase 3: Delete existing data                                         #
    # ------------------------------------------------------------------ #

    def _delete_existing(self):
        self.log_phase('Phase 3: Deleting existing place links and records')

        if self.dry_run:
            n_links = DescriptionPlace.objects.count()
            n_places = Place.objects.count()
            self.log(f'  Would delete {n_links:,} DescriptionPlace links')
            self.log(f'  Would delete {n_places:,} Place records')
            return

        # Delete links first (PROTECT FK constraint on place)
        self.log('  Deleting DescriptionPlace links...', newline=False)
        deleted = 0
        while True:
            ids = list(DescriptionPlace.objects.values_list('id', flat=True)[:10000])
            if not ids:
                break
            DescriptionPlace.objects.filter(id__in=ids).delete()
            deleted += len(ids)
            self.log(f' {deleted:,}...', newline=False)
        self.log(f' done ({deleted:,} deleted)')

        # Delete places (clear self-referential parent FKs first to avoid constraint issues)
        self.log('  Clearing place parent references...', newline=False)
        Place.objects.update(parent=None)
        self.log(' done')

        self.log('  Deleting Place records...', newline=False)
        deleted = 0
        while True:
            ids = list(Place.objects.values_list('id', flat=True)[:10000])
            if not ids:
                break
            Place.objects.filter(id__in=ids).delete()
            deleted += len(ids)
            self.log(f' {deleted:,}...', newline=False)
        self.log(f' done ({deleted:,} deleted)')

        self.log_elapsed()

    # ------------------------------------------------------------------ #
    # Phase 4: Create new Place records                                     #
    # ------------------------------------------------------------------ #

    def _create_places(self, unique_places):
        self.log_phase('Phase 4: Creating Place records with nl-xxxxx codes')

        # Pre-generate unique codes (bulk_create bypasses save())
        self.log('  Pre-generating place codes...', newline=False)
        needed = len(unique_places)
        codes = set()
        while len(codes) < needed:
            codes.add(generate_neogranadina_code(prefix='nl', length=5))
        codes = list(codes)
        self.log(f' {len(codes):,} ready')

        if self.dry_run:
            self.log(f'  Would create {needed:,} Place records')
            return {p['norm_key']: i + 1 for i, p in enumerate(unique_places)}

        batch = []
        created = 0
        for i, place in enumerate(unique_places):
            batch.append(Place(
                place_code=codes[i],
                label=place['canonical_name'],
                display_name=place['canonical_name'],
                needs_geocoding=True,
                needs_review=True,
                review_note='M2.1 import — awaiting gazetteer matching',
            ))
            if len(batch) >= BATCH_SIZE:
                Place.objects.bulk_create(batch, batch_size=BATCH_SIZE)
                created += len(batch)
                self.log(f'  Created {created:,}/{needed:,}...')
                batch = []
        if batch:
            Place.objects.bulk_create(batch, batch_size=BATCH_SIZE)
            created += len(batch)

        self.log(f'  Total created: {created:,}')
        self.log_elapsed()

        # Rebuild lookup (MySQL bulk_create doesn't return IDs)
        self.log_phase('Phase 4b: Building name → id lookup')
        self.log('  Indexing...', newline=False)
        name_to_id = {}
        for label, place_id in Place.objects.values_list('label', 'id'):
            name_to_id[normalize_name(label)] = place_id
        self.log(f' {len(name_to_id):,} entries indexed')
        self.log_elapsed()
        return name_to_id

    # ------------------------------------------------------------------ #
    # Phase 5: Create DescriptionPlace links                                #
    # ------------------------------------------------------------------ #

    def _create_links(self, rows, name_to_id):
        self.log_phase('Phase 5: Creating DescriptionPlace links')

        self.log('  Loading valid description IDs...', newline=False)
        valid_desc_ids = set(Description.objects.values_list('id', flat=True))
        self.log(f' {len(valid_desc_ids):,}')

        batch = []
        created = 0
        skipped_no_place = 0
        skipped_no_desc = 0
        unknown_roles = defaultdict(int)
        total = len(rows)

        for i, row in enumerate(rows):
            name = row['name'].strip()
            raw_role = row.get('role', 'mentioned').strip().lower()

            role = ROLE_MAP.get(raw_role)
            if role is None:
                unknown_roles[raw_role] += 1
                role = 'mentioned'

            desc_id = int(row['id'])
            if desc_id not in valid_desc_ids:
                skipped_no_desc += 1
                continue

            norm = normalize_name(name)
            place_id = name_to_id.get(norm)
            if place_id is None:
                skipped_no_place += 1
                continue

            if not self.dry_run:
                batch.append(DescriptionPlace(
                    description_id=desc_id,
                    place_id=place_id,
                    role=role,
                    needs_review=True,
                ))
                if len(batch) >= BATCH_SIZE:
                    DescriptionPlace.objects.bulk_create(
                        batch, ignore_conflicts=True
                    )
                    created += len(batch)
                    batch = []
            else:
                created += 1

            if (i + 1) % 50000 == 0:
                pct = 100 * (i + 1) / total
                self.log(f'  Processed {i + 1:,}/{total:,} ({pct:.0f}%)')

        if batch and not self.dry_run:
            DescriptionPlace.objects.bulk_create(batch, ignore_conflicts=True)
            created += len(batch)

        if unknown_roles:
            self.log(self.style.WARNING('  Unknown roles mapped to "mentioned":'))
            for r, n in sorted(unknown_roles.items(), key=lambda x: -x[1]):
                self.log(f'    {r!r}: {n}')

        self.log(f'  Links created:            {created:,}')
        self.log(f'  Skipped — no place match: {skipped_no_place}')
        self.log(f'  Skipped — no description: {skipped_no_desc}')
        self.log_elapsed()

    # ------------------------------------------------------------------ #
    # Summary                                                               #
    # ------------------------------------------------------------------ #

    def _summary(self, unique_places):
        self.log_phase('Summary')
        if not self.dry_run:
            n_places = Place.objects.count()
            n_links = DescriptionPlace.objects.count()
            self.log(f'  Place records:          {n_places:,}')
            self.log(f'  DescriptionPlace links: {n_links:,}')
            self.log(self.style.SUCCESS('  Done.'))
        else:
            self.log(f'  Would create ~{len(unique_places):,} places')
            self.log(self.style.WARNING('  Dry run — no changes made.'))
