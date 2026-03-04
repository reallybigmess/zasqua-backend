"""
Import entity authority from M1 extraction.

Replaces ALL existing Entity records and DescriptionEntity links with fresh
data built from entities.csv produced by the M1 title/entity batch.

Usage:
    python manage.py import_m1_entities
    python manage.py import_m1_entities --dry-run
    python manage.py import_m1_entities --csv-path /path/to/entities.csv
"""

import csv
import re
import sys
import time
import unicodedata
from collections import defaultdict

from django.core.management.base import BaseCommand

from catalog.models import (
    Description, DescriptionEntity, Entity, generate_neogranadina_code,
)

# --- Honorific stripping ---
# Strip address prefixes only — NOT identity titles like Marqués, Conde, Duque,
# Teniente, Gobernador, which ARE the entity name.
HONORIFIC_PREFIXES = re.compile(
    r'^(Don |Doña |Dona |Dr\. |Fray |Fr\. |Sor |Pbro\. |Pbra\. |Sr\. )',
    re.IGNORECASE,
)


def normalize_name(name):
    """Normalize entity name for deduplication."""
    name = HONORIFIC_PREFIXES.sub('', name.strip())
    name = unicodedata.normalize('NFD', name)
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
    return re.sub(r'\s+', ' ', name).strip().lower()


# M1 entity type → model entity_type
TYPE_MAP = {
    'person': 'person',
    'institution': 'corporate',
    'corporate': 'corporate',
    'family': 'family',
}

# M1 role → DescriptionEntity.role
# None means skip the row entirely (place role misapplied to entity)
ROLE_MAP = {
    'creator': 'creator',
    'author': 'author',
    'sender': 'sender',
    'recipient': 'recipient',
    'scribe': 'scribe',
    'witness': 'witness',
    'notary': 'notary',
    'mentioned': 'mentioned',
    'subject': 'subject',
    'photographer': 'photographer',
    'artist': 'artist',
    'plaintiff': 'plaintiff',
    'defendant': 'defendant',
    'petitioner': 'petitioner',
    'procurador': 'petitioner',
    'official': 'official',
    'regidor': 'official',
    'alcalde': 'official',
    'corregidor': 'official',
    'inspector': 'official',
    'heir': 'heir',
    'judge': 'judge',
    'albacea': 'albacea',
    'executor': 'albacea',
    'spouse': 'spouse',
    'victim': 'victim',
    'grantor': 'grantor',
    'donor': 'donor',
    'seller': 'seller',
    'vendor': 'seller',
    'buyer': 'buyer',
    'grantee': 'buyer',
    'mortgagor': 'mortgagor',
    'mortgagee': 'mortgagee',
    'appellant': 'appellant',
    'creditor': 'creditor',
    'debtor': 'debtor',
    'partner': 'mentioned',
    'cacique': 'mentioned',
    'beneficiary': 'mentioned',
    'principal': 'mentioned',
    # Place-vocabulary roles used for institutions — map to sender/recipient
    'origin': 'sender',
    'destination': 'recipient',
    # PE-BN vocabulary
    'signatory': 'creator',
    'signer': 'creator',
    'issuer': 'creator',
    'proposer': 'petitioner',
    'examiner': 'official',
    'approver': 'official',
    'secretary': 'official',
    'fiscal': 'official',
    'diputado': 'official',
    'diputado secretario': 'official',
    'presidente': 'official',
    'rector': 'official',
    'replier': 'recipient',
    'informant': 'mentioned',
    'contributor': 'mentioned',
    'participant': 'mentioned',
    'voter': 'mentioned',
    'tribunal member': 'official',
    'decision maker': 'official',
    'decision-maker': 'official',
    'decision_maker': 'official',
    'executing officer': 'official',
    'hearing authority': 'official',
    'absentee voter': 'mentioned',
    'crew member': 'mentioned',
    'crew officer': 'official',
    'military commander': 'official',
    'military officer': 'official',
    'ship captain': 'official',
    'port official': 'official',
    'intervening official': 'official',
    'contracting party': 'mentioned',
    'appointed judge': 'judge',
}

DEFAULT_CSV_PATH = (
    '/Users/juancobo/Databases/zasqua/zasqua-dev-notes/'
    'releases/0.4.0/title-entity-cleanup/review/entities.csv'
)

BATCH_SIZE = 500


class Command(BaseCommand):
    help = 'Import entity authority from M1 extraction (replaces all existing entity data)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be done without making changes',
        )
        parser.add_argument(
            '--csv-path',
            default=DEFAULT_CSV_PATH,
            help='Path to entities.csv (default: M1 review output)',
        )
        parser.add_argument(
            '--append',
            action='store_true',
            help='Add to existing entity data instead of replacing it (for PE-BN and other additive passes)',
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
        self.append  = options['append']
        csv_path     = options['csv_path']

        if self.dry_run:
            self.log(self.style.WARNING('DRY RUN — no changes will be made'))
        if self.append:
            self.log(self.style.WARNING('APPEND MODE — existing entity data will not be deleted'))

        rows = self._load_csv(csv_path)
        unique_entities = self._deduplicate(rows)
        if not self.append:
            self._delete_existing()
        name_to_id = self._create_entities(unique_entities)
        self._create_links(rows, name_to_id)
        self._summary(unique_entities)

    # ------------------------------------------------------------------ #
    # Phase 1: Load CSV                                                     #
    # ------------------------------------------------------------------ #

    def _load_csv(self, csv_path):
        self.log_phase('Phase 1: Loading entities.csv')
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
        self.log_phase('Phase 2: Deduplicating by (normalized name, type)')

        # Key: (normalized_name, entity_type)
        # Value: name_counts dict for picking the most common canonical form
        groups = defaultdict(lambda: defaultdict(int))
        skipped_type = 0
        unknown_roles = defaultdict(int)

        for row in rows:
            name = row['name'].strip()
            raw_type = row.get('type', 'person').strip().lower()
            raw_role = row.get('role', 'mentioned').strip().lower()

            # Strip multi-value roles: "recipient, signer" → "recipient"
            if ',' in raw_role:
                raw_role = raw_role.split(',')[0].strip()
            elif '/' in raw_role:
                raw_role = raw_role.split('/')[0].strip()

            entity_type = TYPE_MAP.get(raw_type)
            if not entity_type:
                skipped_type += 1
                continue

            if raw_role not in ROLE_MAP:
                unknown_roles[raw_role] += 1

            key = (normalize_name(name), entity_type)
            groups[key][name] += 1

        if unknown_roles:
            self.log(self.style.WARNING('  Unknown roles (will map to "mentioned"):'))
            for r, n in sorted(unknown_roles.items(), key=lambda x: -x[1]):
                self.log(f'    {r!r}: {n}')

        # Build canonical list: most frequent name form wins
        unique_entities = []
        for (norm_key, entity_type), name_counts in groups.items():
            canonical = max(name_counts, key=lambda n: name_counts[n])
            unique_entities.append({
                'norm_key': (norm_key, entity_type),
                'canonical_name': canonical,
                'entity_type': entity_type,
            })

        persons = sum(1 for e in unique_entities if e['entity_type'] == 'person')
        corps = sum(1 for e in unique_entities if e['entity_type'] == 'corporate')
        families = sum(1 for e in unique_entities if e['entity_type'] == 'family')

        self.log(f'  Unique entities: {len(unique_entities):,}')
        self.log(f'    — persons:       {persons:,}')
        self.log(f'    — corporate:     {corps:,}')
        self.log(f'    — families:      {families:,}')
        self.log(f'  Skipped — unknown type: {skipped_type}')
        self.log_elapsed()
        return unique_entities

    # ------------------------------------------------------------------ #
    # Phase 3: Delete existing data                                         #
    # ------------------------------------------------------------------ #

    def _delete_existing(self):
        self.log_phase('Phase 3: Deleting existing entity links and records')

        if self.dry_run:
            n_links = DescriptionEntity.objects.count()
            n_entities = Entity.objects.count()
            self.log(f'  Would delete {n_links:,} DescriptionEntity links')
            self.log(f'  Would delete {n_entities:,} Entity records')
            return

        # Delete links first (PROTECT FK constraint on entity)
        self.log('  Deleting DescriptionEntity links...', newline=False)
        deleted = 0
        while True:
            ids = list(DescriptionEntity.objects.values_list('id', flat=True)[:10000])
            if not ids:
                break
            DescriptionEntity.objects.filter(id__in=ids).delete()
            deleted += len(ids)
            self.log(f' {deleted:,}...', newline=False)
        self.log(f' done ({deleted:,} deleted)')

        # Delete entities
        self.log('  Deleting Entity records...', newline=False)
        deleted = 0
        while True:
            ids = list(Entity.objects.values_list('id', flat=True)[:10000])
            if not ids:
                break
            Entity.objects.filter(id__in=ids).delete()
            deleted += len(ids)
            self.log(f' {deleted:,}...', newline=False)
        self.log(f' done ({deleted:,} deleted)')

        self.log_elapsed()

    # ------------------------------------------------------------------ #
    # Phase 4: Create new Entity records                                    #
    # ------------------------------------------------------------------ #

    def _create_entities(self, unique_entities):
        self.log_phase('Phase 4: Creating Entity records with ne-xxxxx codes')

        # In append mode, build a lookup of existing entities first so we
        # can reuse their IDs instead of creating duplicates.
        existing = {}
        if self.append:
            self.log('  Loading existing entity index...', newline=False)
            for display_name, entity_type, entity_id in Entity.objects.values_list(
                'display_name', 'entity_type', 'id'
            ):
                existing[(normalize_name(display_name), entity_type)] = entity_id
            self.log(f' {len(existing):,} existing entities indexed')

        # Only create entities whose normalised key is not already in the DB
        to_create = [e for e in unique_entities if e['norm_key'] not in existing]
        reused    = len(unique_entities) - len(to_create)
        if self.append:
            self.log(f'  Reusing existing: {reused:,}  |  New to create: {len(to_create):,}')

        # Pre-generate enough unique codes (bulk_create bypasses save())
        self.log('  Pre-generating entity codes...', newline=False)
        needed = len(to_create)
        codes = set()
        while len(codes) < needed:
            codes.add(generate_neogranadina_code(prefix='ne', length=5))
        codes = list(codes)
        self.log(f' {len(codes):,} ready')

        if self.dry_run:
            self.log(f'  Would create {needed:,} Entity records')
            # Return synthetic lookup for dry-run stats (existing + new)
            lookup = dict(existing)
            for i, e in enumerate(to_create):
                lookup[e['norm_key']] = -(i + 1)  # negative = synthetic dry-run ID
            return lookup

        batch = []
        created = 0
        for i, ent in enumerate(to_create):
            batch.append(Entity(
                entity_code=codes[i],
                display_name=ent['canonical_name'],
                sort_name=ent['canonical_name'],
                entity_type=ent['entity_type'],
                needs_review=True,
                review_note='M2.1 import — awaiting LOD matching',
            ))
            if len(batch) >= BATCH_SIZE:
                Entity.objects.bulk_create(batch, batch_size=BATCH_SIZE)
                created += len(batch)
                self.log(f'  Created {created:,}/{needed:,}...')
                batch = []
        if batch:
            Entity.objects.bulk_create(batch, batch_size=BATCH_SIZE)
            created += len(batch)

        self.log(f'  Total created: {created:,}')
        self.log_elapsed()

        # Rebuild lookup (MySQL bulk_create doesn't return IDs)
        self.log_phase('Phase 4b: Building name → id lookup')
        self.log('  Indexing...', newline=False)
        name_to_id = {}
        for display_name, entity_type, entity_id in Entity.objects.values_list(
            'display_name', 'entity_type', 'id'
        ):
            key = (normalize_name(display_name), entity_type)
            name_to_id[key] = entity_id
        self.log(f' {len(name_to_id):,} entries indexed')
        self.log_elapsed()
        return name_to_id

    # ------------------------------------------------------------------ #
    # Phase 5: Create DescriptionEntity links                               #
    # ------------------------------------------------------------------ #

    def _create_links(self, rows, name_to_id):
        self.log_phase('Phase 5: Creating DescriptionEntity links')

        self.log('  Loading valid description IDs...', newline=False)
        valid_desc_ids = set(Description.objects.values_list('id', flat=True))
        self.log(f' {len(valid_desc_ids):,}')

        batch = []
        created = 0
        skipped_no_entity = 0
        skipped_no_desc = 0
        unknown_roles = defaultdict(int)
        total = len(rows)

        for i, row in enumerate(rows):
            name = row['name'].strip()
            raw_type = row.get('type', 'person').strip().lower()
            raw_role = row.get('role', 'mentioned').strip().lower()

            # Strip multi-value roles: "recipient, signer" → "recipient"
            if ',' in raw_role:
                raw_role = raw_role.split(',')[0].strip()
            elif '/' in raw_role:
                raw_role = raw_role.split('/')[0].strip()

            entity_type = TYPE_MAP.get(raw_type)
            if not entity_type:
                continue

            role = ROLE_MAP.get(raw_role)
            if role is None:
                unknown_roles[raw_role] += 1
                role = 'mentioned'

            desc_id = int(row['id'])
            if desc_id not in valid_desc_ids:
                skipped_no_desc += 1
                continue

            key = (normalize_name(name), entity_type)
            entity_id = name_to_id.get(key)
            if entity_id is None:
                skipped_no_entity += 1
                continue

            if not self.dry_run:
                batch.append(DescriptionEntity(
                    description_id=desc_id,
                    entity_id=entity_id,
                    role=role,
                    function=row.get('function', '').strip(),
                    name_as_recorded=row.get('name_as_recorded', '').strip(),
                    needs_review=True,
                ))
                if len(batch) >= BATCH_SIZE:
                    DescriptionEntity.objects.bulk_create(
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
            DescriptionEntity.objects.bulk_create(batch, ignore_conflicts=True)
            created += len(batch)

        if unknown_roles:
            self.log(self.style.WARNING('  Unknown roles mapped to "mentioned":'))
            for r, n in sorted(unknown_roles.items(), key=lambda x: -x[1]):
                self.log(f'    {r!r}: {n}')

        self.log(f'  Links created:             {created:,}')
        self.log(f'  Skipped — no entity match: {skipped_no_entity}')
        self.log(f'  Skipped — no description:  {skipped_no_desc}')
        self.log_elapsed()

    # ------------------------------------------------------------------ #
    # Summary                                                               #
    # ------------------------------------------------------------------ #

    def _summary(self, unique_entities):
        self.log_phase('Summary')
        if not self.dry_run:
            n_entities = Entity.objects.count()
            n_links = DescriptionEntity.objects.count()
            self.log(f'  Entity records:          {n_entities:,}')
            self.log(f'  DescriptionEntity links: {n_links:,}')
            self.log(self.style.SUCCESS('  Done.'))
        else:
            self.log(f'  Would create ~{len(unique_entities):,} entities')
            self.log(self.style.WARNING('  Dry run — no changes made.'))
