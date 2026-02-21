"""
Export PE-BN OCR text from CollectiveAccess for NER batch processing.

Queries CA MySQL for the full (cleaned, uncompressed) OCR text for each
PE-BN description, applies clean_ocr_text cleaning, and writes a
metadata.json batch input file for the NER agent run.

Text strategy:
- Documents <= 12,000 chars after cleaning: full text
- Documents >  12,000 chars after cleaning: first 10,000 + '[...]' + last 2,000

Usage:
    python manage.py export_pe_bn_ocr
    python manage.py export_pe_bn_ocr --dry-run
    python manage.py export_pe_bn_ocr --output /path/to/metadata.json
    python manage.py export_pe_bn_ocr --ca-db abcneogranadina
"""

import json
import sys
import time

import MySQLdb
from django.core.management.base import BaseCommand

from catalog.management.commands.import_ocr_text import clean_ocr_text
from catalog.models import Description


OCR_FULL_CAP = 12_000
OCR_HEAD     = 10_000
OCR_TAIL     =  2_000

DEFAULT_OUTPUT = (
    '/Users/juancobo/Databases/zasqua/zasqua-dev-notes/'
    'releases/0.4.0/pe-bn-ner/metadata.json'
)


def truncate_for_ner(text):
    """Apply head+tail strategy for long texts."""
    if len(text) <= OCR_FULL_CAP:
        return text
    return text[:OCR_HEAD] + '\n[...]\n' + text[-OCR_TAIL:]


class Command(BaseCommand):
    help = 'Export PE-BN OCR text from CA for NER batch processing'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show statistics without writing output',
        )
        parser.add_argument(
            '--output',
            default=DEFAULT_OUTPUT,
            help='Path to write metadata.json (default: pe-bn-ner/metadata.json)',
        )
        parser.add_argument(
            '--ca-db',
            default='abcneogranadina',
            help='CA database name (default: abcneogranadina)',
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
        dry_run    = options['dry_run']
        output     = options['output']
        ca_db      = options['ca_db']

        if dry_run:
            self.log(self.style.WARNING('DRY RUN — no file will be written'))

        ocr_by_object = self._load_ca_ocr(ca_db)
        desc_lookup   = self._load_descriptions()
        items         = self._build_items(ocr_by_object, desc_lookup)

        if not dry_run:
            self._write_output(items, output)

        self._summary(items, output, dry_run)

    # ------------------------------------------------------------------ #
    # Phase 1: Load OCR text from CA                                       #
    # ------------------------------------------------------------------ #

    def _load_ca_ocr(self, ca_db):
        self.log_phase('Phase 1: Loading OCR text from CA database')

        conn = MySQLdb.connect(
            host='localhost', user='root', password='',
            db=ca_db, charset='utf8mb4',
        )
        cur = conn.cursor()

        self.log('  Querying ca_object_representations...', newline=False)
        cur.execute("""
            SELECT oxr.object_id, r.media_content
            FROM ca_objects_x_object_representations oxr
            JOIN ca_object_representations r
                ON r.representation_id = oxr.representation_id
            WHERE r.media_content IS NOT NULL
                AND r.media_content <> ''
                AND r.deleted = 0
            ORDER BY oxr.object_id, LENGTH(r.media_content) DESC
        """)

        # Keep only the longest text per object (best quality OCR pass)
        ocr_by_object = {}
        for object_id, media_content in cur:
            if object_id not in ocr_by_object:
                ocr_by_object[object_id] = media_content

        cur.close()
        conn.close()

        self.log(f' {len(ocr_by_object):,} CA objects with OCR text')
        self.log_elapsed()
        return ocr_by_object

    # ------------------------------------------------------------------ #
    # Phase 2: Load PE-BN descriptions from Django                         #
    # ------------------------------------------------------------------ #

    def _load_descriptions(self):
        self.log_phase('Phase 2: Loading PE-BN descriptions')

        self.log('  Querying descriptions...', newline=False)
        rows = Description.objects.filter(
            repository__code='pe-bn',
            ca_object_id__isnull=False,
        ).values(
            'id', 'ca_object_id', 'reference_code',
            'title', 'date_expression', 'resource_type',
        )

        # Build lookup: ca_object_id → description metadata
        desc_lookup = {
            row['ca_object_id']: row
            for row in rows
        }
        self.log(f' {len(desc_lookup):,} PE-BN descriptions with ca_object_id')
        self.log_elapsed()
        return desc_lookup

    # ------------------------------------------------------------------ #
    # Phase 3: Clean, truncate, and build output items                     #
    # ------------------------------------------------------------------ #

    def _build_items(self, ocr_by_object, desc_lookup):
        self.log_phase('Phase 3: Cleaning and building items')

        matched     = set(ocr_by_object.keys()) & set(desc_lookup.keys())
        unmatched   = set(ocr_by_object.keys()) - set(desc_lookup.keys())
        no_ocr      = set(desc_lookup.keys())  - set(ocr_by_object.keys())

        self.log(f'  Matched (OCR + description): {len(matched):,}')
        if unmatched:
            self.log(f'  CA objects without Django match: {len(unmatched):,}')
        if no_ocr:
            self.log(f'  Descriptions without CA OCR:     {len(no_ocr):,}')

        items           = []
        skipped_empty   = 0
        full_text       = 0
        head_tail       = 0

        for ca_object_id in sorted(matched):
            raw  = ocr_by_object[ca_object_id]
            desc = desc_lookup[ca_object_id]

            cleaned = clean_ocr_text(raw)
            if not cleaned:
                skipped_empty += 1
                continue

            if len(cleaned) <= OCR_FULL_CAP:
                text = cleaned
                full_text += 1
            else:
                text = truncate_for_ner(cleaned)
                head_tail += 1

            items.append({
                'id':             desc['id'],
                'reference_code': desc['reference_code'] or '',
                'title':          desc['title'] or '',
                'date_expression': desc['date_expression'] or '',
                'resource_type':  desc['resource_type'] or '',
                'ocr_text':       text,
            })

        self.log(f'  Items built:      {len(items):,}')
        self.log(f'  Full text:        {full_text:,} ({100*full_text/len(items):.0f}%)')
        self.log(f'  Head+tail:        {head_tail:,} ({100*head_tail/len(items):.0f}%)')
        self.log(f'  Skipped (empty):  {skipped_empty:,}')
        self.log_elapsed()
        return items

    # ------------------------------------------------------------------ #
    # Phase 4: Write output                                                 #
    # ------------------------------------------------------------------ #

    def _write_output(self, items, output):
        self.log_phase('Phase 4: Writing metadata.json')
        self.log(f'  Output: {output}')
        self.log('  Writing...', newline=False)

        with open(output, 'w', encoding='utf-8') as f:
            json.dump(items, f, ensure_ascii=False, indent=2)

        self.log(' done')
        self.log_elapsed()

    # ------------------------------------------------------------------ #
    # Summary                                                               #
    # ------------------------------------------------------------------ #

    def _summary(self, items, output, dry_run):
        self.log_phase('Summary')
        self.log(f'  Items exported: {len(items):,}')
        if not dry_run:
            self.log(f'  Written to:     {output}')
            self.log(self.style.SUCCESS('  Done.'))
        else:
            self.log(self.style.WARNING('  Dry run — no file written.'))
