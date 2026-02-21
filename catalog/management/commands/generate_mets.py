"""
Generate METS XML documents for all descriptions.

Produces one METS 1.12.1 XML file per description with Dublin Core
descriptive metadata.  Digitised items additionally reference their
IIIF manifest in <fileSec>.

Outputs are static XML files intended for upload to R2
(mets.zasqua.org/{slug}.xml).

Usage:
    python manage.py generate_mets --output-dir /tmp/mets
    python manage.py generate_mets --output-dir /tmp/mets --repository co-cihjml --limit 10
"""

import os
import sys
import time
from datetime import datetime, timezone
from xml.etree.ElementTree import Element, SubElement, ElementTree, indent

from django.core.management.base import BaseCommand

from catalog.models import Description, Repository


# ---------------------------------------------------------------------------
# Namespaces
# ---------------------------------------------------------------------------

NS_METS = 'http://www.loc.gov/METS/'
NS_XLINK = 'http://www.w3.org/1999/xlink'
NS_DC = 'http://purl.org/dc/elements/1.1/'
NS_DCTERMS = 'http://purl.org/dc/terms/'

# Register prefixes so ElementTree uses them instead of ns0, ns1, etc.
from xml.etree.ElementTree import register_namespace
register_namespace('', NS_METS)
register_namespace('xlink', NS_XLINK)
register_namespace('dc', NS_DC)
register_namespace('dcterms', NS_DCTERMS)


# ---------------------------------------------------------------------------
# Language mapping (shared with export_frontend_data)
# ---------------------------------------------------------------------------

_LANGUAGE_MAP = {
    '192': 'Español',
    '173': 'Español',
    '195': 'Español',
    'Spanish': 'Español',
}


# ---------------------------------------------------------------------------
# Description level → DC type mapping
# ---------------------------------------------------------------------------

_DC_TYPE_MAP = {
    'fonds': 'Collection',
    'subfonds': 'Collection',
    'series': 'Collection',
    'subseries': 'Collection',
    'collection': 'Collection',
    'section': 'Collection',
    'file': 'Collection',
    'item': 'Text',
    'volume': 'Text',
}


# ---------------------------------------------------------------------------
# Per-repository rights text for dc:rights
# ---------------------------------------------------------------------------

_RIGHTS_DIGITISED = {
    'pe-bn': 'CC BY-NC 4.0. Para obtener derechos de reproducción para publicaciones, por favor diríjase a la Biblioteca Nacional del Perú.',
    'co-cihjml': 'CC BY-NC 4.0. Para obtener derechos de reproducción para publicaciones, por favor diríjase al CIHJML, Universidad del Cauca.',
    'co-ahr': 'CC BY-NC 4.0. Para obtener derechos de reproducción para publicaciones, por favor diríjase al Archivo Histórico de Rionegro.',
    'co-ahrb': 'CC BY-NC 4.0. Para obtener derechos de reproducción para publicaciones, por favor diríjase al Archivo Histórico Regional de Boyacá.',
    'co-ahjci': 'CC BY-NC 4.0. Para obtener derechos de reproducción para publicaciones, por favor diríjase al Archivo Histórico del Juzgado del Circuito de Istmina.',
}

_RIGHTS_DEFAULT = 'Los catálogos y descripciones de Zasqua son de libre acceso.'


# ---------------------------------------------------------------------------
# METS generation
# ---------------------------------------------------------------------------

def _add_dc(parent, tag, text):
    """Add a Dublin Core element if text is non-empty."""
    if not text:
        return
    el = SubElement(parent, tag)
    el.text = str(text).strip()


def build_mets(desc, repo, create_date):
    """Build a METS ElementTree for a single description.

    Args:
        desc: dict with description fields (from .values() query)
        repo: dict with repository fields, or None
        create_date: ISO 8601 datetime string for metsHdr

    Returns:
        ElementTree ready to write
    """
    ref = desc['reference_code'] or ''
    title = desc['title'] or ''
    level = desc['description_level'] or ''

    # Root <mets>
    root = Element(f'{{{NS_METS}}}mets')
    root.set('OBJID', ref)
    root.set('LABEL', title)
    if level:
        root.set('TYPE', level)
    root.set('PROFILE', 'http://www.loc.gov/standards/mets/profiles/')
    # xlink namespace is registered via register_namespace above

    # <metsHdr>
    hdr = SubElement(root, f'{{{NS_METS}}}metsHdr')
    hdr.set('CREATEDATE', create_date)

    agent_creator = SubElement(hdr, f'{{{NS_METS}}}agent')
    agent_creator.set('ROLE', 'CREATOR')
    agent_creator.set('TYPE', 'ORGANIZATION')
    name_el = SubElement(agent_creator, f'{{{NS_METS}}}name')
    name_el.text = 'Fundación Histórica Neogranadina (NIT 900.861.407), Bogotá, Colombia'
    note_el = SubElement(agent_creator, f'{{{NS_METS}}}note')
    note_el.text = 'https://neogranadina.org'

    if repo:
        agent_custodian = SubElement(hdr, f'{{{NS_METS}}}agent')
        agent_custodian.set('ROLE', 'CUSTODIAN')
        agent_custodian.set('TYPE', 'ORGANIZATION')
        name_el = SubElement(agent_custodian, f'{{{NS_METS}}}name')
        name_el.text = repo['name']

    # <dmdSec>
    dmd = SubElement(root, f'{{{NS_METS}}}dmdSec')
    dmd.set('ID', 'dmd-001')
    wrap = SubElement(dmd, f'{{{NS_METS}}}mdWrap')
    wrap.set('MDTYPE', 'DC')
    xml_data = SubElement(wrap, f'{{{NS_METS}}}xmlData')

    # Dublin Core elements
    _add_dc(xml_data, f'{{{NS_DC}}}title', title)
    _add_dc(xml_data, f'{{{NS_DC}}}identifier', ref)
    _add_dc(xml_data, f'{{{NS_DC}}}date', desc.get('date_expression'))
    _add_dc(xml_data, f'{{{NS_DC}}}description', desc.get('scope_content'))
    _add_dc(xml_data, f'{{{NS_DC}}}creator', desc.get('creator_display'))

    lang = desc.get('language') or ''
    _add_dc(xml_data, f'{{{NS_DC}}}language', _LANGUAGE_MAP.get(lang, lang))

    _add_dc(xml_data, f'{{{NS_DC}}}format', desc.get('extent'))

    dc_type = _DC_TYPE_MAP.get(level, '')
    _add_dc(xml_data, f'{{{NS_DC}}}type', dc_type)

    # dc:source — repository name + city
    if repo:
        source = repo['name']
        if repo.get('city'):
            source += f", {repo['city']}"
        _add_dc(xml_data, f'{{{NS_DC}}}source', source)

    # dc:rights
    repo_code = desc.get('repository__code', '')
    has_digital = desc.get('has_digital', False)
    if has_digital and repo_code in _RIGHTS_DIGITISED:
        _add_dc(xml_data, f'{{{NS_DC}}}rights', _RIGHTS_DIGITISED[repo_code])
    else:
        _add_dc(xml_data, f'{{{NS_DC}}}rights', _RIGHTS_DEFAULT)

    _add_dc(xml_data, f'{{{NS_DC}}}subject', desc.get('place_display'))

    # dcterms:isPartOf — parent reference code
    parent_ref = desc.get('parent__reference_code')
    if parent_ref:
        _add_dc(xml_data, f'{{{NS_DCTERMS}}}isPartOf', parent_ref)

    # dc:publisher — for bibliographic items with imprint
    _add_dc(xml_data, f'{{{NS_DC}}}publisher', desc.get('imprint'))

    # <fileSec> — only for digitised items with IIIF manifest
    iiif_url = desc.get('iiif_manifest_url') or ''
    if iiif_url:
        file_sec = SubElement(root, f'{{{NS_METS}}}fileSec')
        file_grp = SubElement(file_sec, f'{{{NS_METS}}}fileGrp')
        file_grp.set('USE', 'IIIF manifest')
        file_el = SubElement(file_grp, f'{{{NS_METS}}}file')
        file_el.set('ID', 'iiif-manifest')
        file_el.set('MIMETYPE', 'application/ld+json')
        flocat = SubElement(file_el, f'{{{NS_METS}}}FLocat')
        flocat.set('LOCTYPE', 'URL')
        flocat.set(f'{{{NS_XLINK}}}href', iiif_url)

    # <structMap>
    struct_map = SubElement(root, f'{{{NS_METS}}}structMap')
    struct_map.set('TYPE', 'logical')
    div = SubElement(struct_map, f'{{{NS_METS}}}div')
    if level:
        div.set('TYPE', level)
    div.set('LABEL', title)
    div.set('DMDID', 'dmd-001')

    if iiif_url:
        fptr = SubElement(div, f'{{{NS_METS}}}fptr')
        fptr.set('FILEID', 'iiif-manifest')

    indent(root)
    return ElementTree(root)


class Command(BaseCommand):
    help = 'Generate METS XML documents for all descriptions'

    def add_arguments(self, parser):
        parser.add_argument(
            '--output-dir',
            default='mets',
            help='Output directory (default: ./mets)',
        )
        parser.add_argument(
            '--repository',
            help='Limit to a single repository code (e.g. co-cihjml)',
        )
        parser.add_argument(
            '--limit',
            type=int,
            help='Limit number of descriptions to process',
        )
        parser.add_argument(
            '--base-url',
            default='https://mets.zasqua.org',
            help='Base URL for METS documents (default: https://mets.zasqua.org)',
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
        output_dir = options['output_dir']
        repo_filter = options.get('repository')
        limit = options.get('limit')
        start_time = time.time()

        os.makedirs(output_dir, exist_ok=True)

        create_date = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

        # ------------------------------------------------------------------
        # Phase 1: Load repositories
        # ------------------------------------------------------------------
        self.log_phase('Phase 1: Loading repositories')

        repos = {}
        for repo in Repository.objects.filter(enabled=True):
            repos[repo.code] = {
                'name': repo.name,
                'short_name': repo.short_name,
                'city': repo.city,
                'country': repo.country,
            }

        self.log(f'  Loaded {len(repos)} repositories')
        self.log_elapsed()

        # ------------------------------------------------------------------
        # Phase 2: Load descriptions
        # ------------------------------------------------------------------
        self.log_phase('Phase 2: Loading descriptions')

        qs = (
            Description.objects
            .select_related('repository', 'parent')
            .order_by('id')
        )

        if repo_filter:
            qs = qs.filter(repository__code=repo_filter)

        rows = list(qs.values(
            'id', 'repository__code',
            'reference_code', 'title', 'description_level',
            'date_expression', 'scope_content', 'extent',
            'creator_display', 'place_display',
            'language', 'arrangement',
            'access_conditions', 'reproduction_conditions',
            'imprint',
            'parent__reference_code',
            'has_digital', 'iiif_manifest_url',
        ))

        if limit:
            rows = rows[:limit]

        self.log(f'  Loaded {len(rows):,} descriptions')
        self.log_elapsed()

        # ------------------------------------------------------------------
        # Phase 3: Generate METS XML
        # ------------------------------------------------------------------
        self.log_phase('Phase 3: Generating METS XML')

        total_size = 0
        files_written = 0

        for i, desc in enumerate(rows):
            ref = desc['reference_code'] or ''
            if not ref:
                continue

            # Slug matches the frontend permalink pattern
            slug = ref.replace('?', '').replace('#', '')

            repo_code = desc.get('repository__code', '')
            repo = repos.get(repo_code)

            tree = build_mets(desc, repo, create_date)

            file_path = os.path.join(output_dir, f'{slug}.xml')
            tree.write(
                file_path,
                encoding='unicode',
                xml_declaration=True,
            )

            total_size += os.path.getsize(file_path)
            files_written += 1

            if files_written % 10000 == 0:
                elapsed = time.time() - self.phase_start
                self.log(f'  {files_written:,} files ({elapsed:.1f}s)')

        size_mb = total_size / 1024 / 1024
        self.log(f'  Wrote {files_written:,} METS files ({size_mb:.1f} MB)')
        self.log_elapsed()

        # ------------------------------------------------------------------
        # Summary
        # ------------------------------------------------------------------
        self.log_phase('Summary')

        elapsed = time.time() - start_time
        self.log(f'  METS files: {files_written:,}')
        self.log(f'  Total size: {size_mb:.1f} MB')
        self.log(f'  Output directory: {os.path.abspath(output_dir)}')
        self.log(f'  Total time: {elapsed:.1f}s')
