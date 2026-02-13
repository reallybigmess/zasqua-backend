"""
Import OCR text from CollectiveAccess ca_object_representations into
Zasqua Description.ocr_text.

Reads media_content from the ABC CA MySQL dump, cleans up common OCR
artefacts (page numbers, repeated volume headers, form feeds, soft
hyphens), removes Spanish stop words, and compresses long texts using
a hybrid strategy: the first 15K characters are kept verbatim (supporting
phrase search and readable excerpts), and beyond that cap only unique
words not already present are appended.

For objects with multiple representations, takes the longest text
(typically the higher-quality OCR pass).

Usage:
    python manage.py import_ocr_text
    python manage.py import_ocr_text --dry-run
"""

import re
import sys
import time

import MySQLdb
from django.core.management.base import BaseCommand

from catalog.models import Description


# ---------------------------------------------------------------------------
# Known CDIP volume headers — modern editorial metadata, not document content.
# These appear at the start of thousands of OCR texts because every document
# in the same CDIP volume shares the same page header.
# ---------------------------------------------------------------------------
CDIP_HEADERS = {
    # Editors / compilers
    'ELLA DUNBAR TEMPLE',
    'FELIX DENEGRI LUNA',
    'CESAR PACHECO VELEZ',
    'CÉSAR PACHECO VÉLEZ',
    'CARLOS DANIEL VALCARCEL',
    'JORGE ARIAS-SCHREIBER PEZET',
    'AUGUSTO TAMAYO VARGAS',
    'MIGUEL MATICORENA ESTRADA',
    'MANUEL JESUS APARICIO VEGA',
    'HIPOLITO UNANUE',
    'JOSE BAQUIJANO Y CARRILLO',
    'JOSE FAUSTINO SANCHEZ CARRION',
    'R UL PORRA',              # OCR artefact for RAÚL PORRAS
    'RAUL PORRAS BARRENECHEA',
    'RAÚL PORRAS BARRENECHEA',
    # Volume / section titles
    'GUERRILLAS Y MONTONERAS PATRIOTAS',
    'OBRA GUBERNATIVA Y EPISTOLARIO DE BOLIVAR',
    'DEFENSA DEL VIRREINATO',
    'PRIMER CONGRESO CONSTITUYENTE',
    'ACTAS DE LAS SESIONES PUBLICAS',
    'DECRETOS Y COMUNICACIONES',
    'JUNTAS DE GUERRA',
    'DOCUMENTOS DE LA REBELIÓN DE TÚPAC AMARU',
    'DOCUMENTOS DE LA REBELION DE TUPAC AMARU',
    'LA REVOLUCION DEL CUSCO DE 1814',
    'LA ACCION DEL CLERO',
    'LIBRO XIV DE CLAUSTROS',
    # Fragments that appear when OCR splits a name across lines
    'ELLA',
    'TEMPLE',
    'DUNBAR',
    'GRAL.',
    'EP FELIPE DE LA BARRA',
    'GRAL. EP FELIPE DE LA BARRA',
    'BARRENECHEA',
    'MARCOS',
    'UNIVERSIDAD',
}

# Normalised set for matching (strip trailing punctuation, upper)
CDIP_HEADERS_NORM = set()
for h in CDIP_HEADERS:
    CDIP_HEADERS_NORM.add(h.upper().rstrip(' -'))
    # Also add without accents for fuzzy matching
    stripped = h.upper().rstrip(' -')
    for src, dst in [('Á', 'A'), ('É', 'E'), ('Í', 'I'), ('Ó', 'O'), ('Ú', 'U')]:
        stripped = stripped.replace(src, dst)
    CDIP_HEADERS_NORM.add(stripped)


# Regex: line that is only digits (1-4), optionally with whitespace
RE_PAGE_NUMBER = re.compile(r'^\s*\d{1,4}\s*$')

# Regex: dot leaders (three or more dots/spaces in sequence)
RE_DOT_LEADERS = re.compile(r'(?:\.\s*){3,}')

# Regex: multiple blank lines
RE_MULTI_BLANK = re.compile(r'\n{3,}')

# Regex: soft hyphen at end of line followed by newline (rejoin word)
RE_SOFT_HYPHEN_EOL = re.compile(r'\xad\s*\n')


def clean_ocr_text(text):
    """
    Clean up OCR text from CDIP volumes.

    1. Remove form feed characters
    2. Rejoin soft-hyphenated words
    3. Remove CDIP volume headers from the start of the text
    4. Remove standalone page numbers
    5. Remove dot leader lines
    6. Collapse multiple blank lines
    7. Strip leading/trailing whitespace
    """
    if not text:
        return ''

    # 1. Form feeds
    text = text.replace('\x0c', '\n')

    # 2. Soft hyphens: rejoin words split across lines
    text = RE_SOFT_HYPHEN_EOL.sub('', text)
    # Remove any remaining soft hyphens (mid-line)
    text = text.replace('\xad', '')

    # 3. Remove CDIP headers from the start of the text.
    #    Headers appear in the first ~10 non-empty lines: editor names,
    #    page numbers, volume titles. We strip them until we hit a line
    #    that doesn't match a known header or page number pattern.
    lines = text.split('\n')
    header_end = 0
    consecutive_non_header = 0

    for i, line in enumerate(lines):
        stripped = line.strip()

        # Skip empty lines in the header zone
        if not stripped:
            if i < 20:
                header_end = i + 1
            continue

        normalised = stripped.upper().rstrip(' -')
        # Also try without accents
        normalised_no_accent = normalised
        for src, dst in [('Á', 'A'), ('É', 'E'), ('Í', 'I'), ('Ó', 'O'),
                         ('Ú', 'U'), ('Ê', 'E'), ('~', 'N')]:
            normalised_no_accent = normalised_no_accent.replace(src, dst)

        is_header = (
            normalised in CDIP_HEADERS_NORM
            or normalised_no_accent in CDIP_HEADERS_NORM
            or RE_PAGE_NUMBER.match(stripped)
        )

        if is_header and i < 20:
            header_end = i + 1
            consecutive_non_header = 0
        else:
            consecutive_non_header += 1
            # Once we've seen 2 consecutive non-header lines, stop looking
            if consecutive_non_header >= 2 or i >= 20:
                break

    if header_end > 0:
        lines = lines[header_end:]
        text = '\n'.join(lines)

    # 4. Remove standalone page numbers throughout the text
    text = '\n'.join(
        line for line in text.split('\n')
        if not RE_PAGE_NUMBER.match(line)
    )

    # 5. Remove dot leader sequences (tables of contents, lists)
    text = RE_DOT_LEADERS.sub(' ', text)

    # 6. Collapse multiple blank lines
    text = RE_MULTI_BLANK.sub('\n\n', text)

    # 7. Strip
    text = text.strip()

    return text


# ---------------------------------------------------------------------------
# Spanish stop words — high-frequency function words that add index bulk
# without carrying search-useful meaning.
# ---------------------------------------------------------------------------
SPANISH_STOP_WORDS = frozenset({
    # Articles
    'el', 'la', 'los', 'las', 'un', 'una', 'unos', 'unas',
    # Contractions
    'del', 'al',
    # Prepositions
    'de', 'en', 'a', 'por', 'con', 'para', 'sin', 'sobre', 'entre',
    'hasta', 'hacia', 'desde', 'contra', 'durante', 'mediante',
    # Conjunctions
    'y', 'e', 'o', 'u', 'que', 'pero', 'ni', 'sino', 'como',
    'aunque', 'porque', 'pues',
    # Pronouns
    'se', 'su', 'sus', 'le', 'les', 'lo', 'me', 'te', 'nos',
    'este', 'esta', 'estos', 'estas', 'ese', 'esa', 'esos', 'esas',
    # Common verb forms
    'es', 'fue', 'ha', 'han', 'son', 'era', 'ser', 'sido',
    # Adverbs / determiners
    'no', 'más', 'muy', 'ya', 'todo', 'toda', 'todos', 'todas',
})

# Character cap for the preserved head.  Text up to this limit keeps its
# word order (supporting phrase search and readable Pagefind excerpts).
# Beyond this, only unique words are appended.
OCR_HEAD_CAP = 15_000


def compress_for_search(text):
    """
    Compress cleaned OCR text for search indexing.

    1. Remove Spanish stop words throughout.
    2. Keep the first OCR_HEAD_CAP characters as-is (word order preserved
       for phrase search and readable Pagefind excerpts).
    3. Beyond the cap, append only unique words not already present in
       the head (captures rare terms from the tail without bloating size).
    """
    if not text:
        return ''

    # Remove stop words
    words = text.split()
    filtered = []
    for w in words:
        # Strip punctuation for the stop-word check but keep the
        # original token so we don't mangle the text.
        bare = w.lower().strip('.,;:!?()[]«»"\'-')
        if bare not in SPANISH_STOP_WORDS:
            filtered.append(w)
    text = ' '.join(filtered)

    if len(text) <= OCR_HEAD_CAP:
        return text

    # Split at a word boundary near the cap
    head = text[:OCR_HEAD_CAP]
    cut = head.rfind(' ')
    if cut > OCR_HEAD_CAP * 0.8:
        head = head[:cut]

    tail = text[len(head):]

    # Collect lowercase forms of words already in the head
    seen = set(w.lower() for w in head.split())

    # From the tail, keep only words not yet seen
    unique_tail = []
    for w in tail.split():
        key = w.lower()
        if key not in seen:
            unique_tail.append(w)
            seen.add(key)

    if unique_tail:
        return head + ' ' + ' '.join(unique_tail)
    return head


class Command(BaseCommand):
    help = 'Import OCR text from CA ca_object_representations into Description.ocr_text'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be done without making changes',
        )
        parser.add_argument(
            '--ca-db',
            default='abcneogranadina',
            help='CA database name (default: abcneogranadina)',
        )

    def log(self, message, style=None, newline=True):
        if style:
            message = style(message)
        if newline:
            self.stdout.write(message)
        else:
            self.stdout.write(message, ending='')
        sys.stdout.flush()

    def log_phase(self, phase_name):
        self.log(f'\n{"="*60}')
        self.log(f'  {phase_name}')
        self.log(f'{"="*60}')
        self.phase_start = time.time()

    def log_elapsed(self):
        elapsed = time.time() - self.phase_start
        self.log(f'  Elapsed: {elapsed:.1f}s')

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        ca_db = options['ca_db']

        if dry_run:
            self.log(self.style.WARNING('DRY RUN — no changes will be made'))

        start_time = time.time()

        # ------------------------------------------------------------------
        # Phase 1: Load OCR text from CA
        # ------------------------------------------------------------------
        self.log_phase('Phase 1: Loading OCR text from CA database')

        conn = MySQLdb.connect(
            host='localhost', user='root', password='',
            db=ca_db, charset='utf8mb4',
        )
        cur = conn.cursor()

        # For each ca_object, get the longest OCR text (best quality).
        # Join through the relation table to get the object_id.
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

        # Keep only the longest text per object
        ocr_by_object = {}
        for object_id, media_content in cur:
            if object_id not in ocr_by_object:
                ocr_by_object[object_id] = media_content

        cur.close()
        conn.close()

        self.log(f'  Loaded OCR for {len(ocr_by_object):,} CA objects')
        self.log_elapsed()

        # ------------------------------------------------------------------
        # Phase 2: Match to Zasqua descriptions
        # ------------------------------------------------------------------
        self.log_phase('Phase 2: Matching to Zasqua descriptions')

        # Build lookup: ca_object_id -> description id
        desc_lookup = dict(
            Description.objects.filter(
                ca_object_id__isnull=False,
                repository__code='pe-bn',
            ).values_list('ca_object_id', 'id')
        )
        self.log(f'  PE-BN descriptions with ca_object_id: {len(desc_lookup):,}')

        matched = set(ocr_by_object.keys()) & set(desc_lookup.keys())
        self.log(f'  Matched OCR to descriptions: {len(matched):,}')

        unmatched = set(ocr_by_object.keys()) - set(desc_lookup.keys())
        if unmatched:
            self.log(f'  OCR objects without Zasqua match: {len(unmatched):,}')

        self.log_elapsed()

        # ------------------------------------------------------------------
        # Phase 3: Clean and import
        # ------------------------------------------------------------------
        self.log_phase('Phase 3: Cleaning, compressing, and importing')

        total = len(matched)
        updated = 0
        skipped_empty = 0
        total_chars_raw = 0
        total_chars_clean = 0
        total_chars_compressed = 0
        texts_truncated = 0

        for i, ca_object_id in enumerate(sorted(matched)):
            raw_text = ocr_by_object[ca_object_id]
            total_chars_raw += len(raw_text)

            cleaned = clean_ocr_text(raw_text)
            total_chars_clean += len(cleaned)

            if not cleaned:
                skipped_empty += 1
                continue

            compressed = compress_for_search(cleaned)
            total_chars_compressed += len(compressed)
            if len(compressed) > OCR_HEAD_CAP:
                texts_truncated += 1

            if not dry_run:
                Description.objects.filter(
                    id=desc_lookup[ca_object_id]
                ).update(ocr_text=compressed)

            updated += 1

            if (i + 1) % 2000 == 0:
                pct = 100 * (i + 1) / total
                self.log(f'  Processed {i + 1:,}/{total:,} ({pct:.0f}%)')

        self.log(f'  Processed {total:,}/{total:,} (100%)')
        self.log_elapsed()

        # ------------------------------------------------------------------
        # Summary
        # ------------------------------------------------------------------
        self.log_phase('Summary')

        elapsed = time.time() - start_time
        clean_reduction = 100 * (1 - total_chars_clean / total_chars_raw) if total_chars_raw else 0
        total_reduction = 100 * (1 - total_chars_compressed / total_chars_raw) if total_chars_raw else 0

        self.log(f'  Descriptions updated: {updated:,}')
        self.log(f'  Skipped (empty after cleanup): {skipped_empty:,}')
        self.log(f'  Texts truncated at {OCR_HEAD_CAP:,} cap: {texts_truncated:,}')
        self.log(f'  Raw text total:        {total_chars_raw:,} chars ({total_chars_raw / 1024 / 1024:.1f} MB)')
        self.log(f'  After cleanup:         {total_chars_clean:,} chars ({total_chars_clean / 1024 / 1024:.1f} MB) ({clean_reduction:.1f}% reduction)')
        self.log(f'  After compression:     {total_chars_compressed:,} chars ({total_chars_compressed / 1024 / 1024:.1f} MB) ({total_reduction:.1f}% reduction)')
        self.log(f'  Total time: {elapsed:.1f}s')

        if dry_run:
            self.log(self.style.WARNING('\nDRY RUN — no changes were made'))
