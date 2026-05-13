"""
Exports entity/place data

Outputs:
  entities.json  — entities
  places.json  — places
  entity_links.json  — entity links
  place_links.json  — place links
You get the idea.

Usage:
    python manage.py export_entity_place
    python manage.py export_entity_place --output-dir /tmp/zasqua-export
"""

import json
import os
import re
import sys
import time

from django.core.management.base import BaseCommand

from catalog.models import Entity, Place, DescriptionEntity, DescriptionPlace #,EntityFunction


# ---------------------------------------------------------------------------
# children_level inference — ported from DescriptionListSerializer
# ---------------------------------------------------------------------------

_LEVEL_HIERARCHY = {
    'fonds': 'caja',
    'collection': 'file',
    'subfonds': 'series',
    'series': 'subseries',
    'subseries': 'file',
    'file': 'item',
}


class Command(BaseCommand):
    help = 'Export entity and place data as static JSON files'

    def add_arguments(self, parser):
        parser.add_argument(
            '--output-dir',
            default='export',
            help='Output directory (default: ./export)',
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
        start_time = time.time()

        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(os.path.join(output_dir, 'children'), exist_ok=True)

        # ------------------------------------------------------------------
        # Phase 1: Load places
        # ------------------------------------------------------------------
        self.log_phase('Phase 1: Loading places')

        rows = list(
            Place.objects
            .order_by('id')
            .values(
                'id', 'place_code', 'label', 'display_name', 'place_type', 'name_variants', 'latitude',
                'longitude', 'colonial_gobernacion', 'colonial_partido', 'colonial_region',
                'country_code', 'admin_level_1', 'admin_level_2',
                'tgn_id', 'hgis_id', 'whg_id', 'wikidata_id', 'created_at', 'updated_at',
            )
        )

        self.log(f'  Loaded {len(rows):,} places')
        self.log_elapsed()

        # ------------------------------------------------------------------
        # Phase 2: Write places.json
        # ------------------------------------------------------------------
        self.log_phase('Phase 2 : Writing places.json')

        places = list(Place.objects.order_by('id'))
        # missing fclass

        place_records = []
        for d in rows:
            place_records.append({
                'id': d['id'],
                'place_code': d['place_code'],
                'label': d['label'],
                'display_name': d['display_name'],
                'place_type': d['place_type'],
                'latitude': float(d['latitude']),
                'longitude': float(d['longitude']),
                'country_code': d['country_code'],
                'name_variants': d['name_variants'],
                'tgn_id': d['tgn_id'],
                'hgis_id': d['hgis_id'],
                'whg_id': d['whg_id'],
                'wikidata_id': d['wikidata_id'],

                'colonial_gobernacion': d['colonial_gobernacion'],
                'colonial_partido': d['colonial_partido'],
                'colonial_region': d['colonial_region'],
                'admin_level_1': d['admin_level_1'],
                'admin_level_2': d['admin_level_2'],

                'created_at': d['created_at'].isoformat(),
                'modified_at': d['updated_at'].isoformat(),
            })

            #print(place_records)

        repo_path = os.path.join(output_dir, 'places.json')
        with open(repo_path, 'w') as f:
            json.dump(place_records, f, ensure_ascii=False)

        size_kb = os.path.getsize(repo_path) / 1024
        self.log(f'  Wrote {len(place_records)} places ({size_kb:.1f} KB)')
        self.log_elapsed()


        # ------------------------------------------------------------------
        # Phase 3: Load place-links
        # ------------------------------------------------------------------
        self.log_phase('Phase 3: Loading place links')

        placelink_rows = list(
            DescriptionPlace.objects
            .select_related('description', 'place', 'description__repository')
            .order_by('id')
            .values(
                'description__reference_code', 'description__date_expression', 'description__date_start',
                'description__repository__code', 'description__title', 'place__label', 'place__place_code',
                'role',
            )
        )
        self.log(f'  Loaded {len(rows):,} place links')
        self.log_elapsed()

        # ------------------------------------------------------------------
        # Phase 4: Write place_links.json
        # ------------------------------------------------------------------
        self.log_phase('Phase 4 : Writing place_links.json')

        pl = list(DescriptionPlace.objects.order_by('id'))

        pl_records = []
        for d in placelink_rows:
            pl_records.append({
                'place_code': d['place__place_code'],
                'reference_code': d['description__reference_code'],
                'title': d['description__title'],
                'date_expression': d['description__date_expression'],
                'date_start': (
                    d['description__date_start'].isoformat() if d['description__date_start'] else None
                ),
                'repository_code':  d['description__repository__code'],
                'role': d['role'],
            })

            #print(entity_records)

        repo_path = os.path.join(output_dir, 'place_links.json')
        with open(repo_path, 'w') as f:
            json.dump(pl_records, f, ensure_ascii=False)

        size_kb = os.path.getsize(repo_path) / 1024
        self.log(f'  Wrote {len(pl_records)} entities ({size_kb:.1f} KB)')
        self.log_elapsed()


        # ------------------------------------------------------------------
        # Phase 5: Load entities
        # ------------------------------------------------------------------
        self.log_phase('Phase 5: Loading entities')
        # todo add function/role stuff to work correctly

        rows = list(
            Entity.objects
            .order_by('id')
            .select_related('description', 'description__repository')
            .values(
                'id', 'entity_code', 'display_name', 'sort_name', 'entity_type', 'given_name', 'surname',
                'honorific', 'date_start', 'date_end', 'name_variants', 'primary_function',
                'functions', 'dates_of_existence', 'history', 'viaf_id', 'wikidata_id', 'created_at', 'updated_at',
            )
        )

        self.log(f'  Loaded {len(rows):,} entities')
        self.log_elapsed()

        # ------------------------------------------------------------------
        # Phase 6: Write entities.json
        # ------------------------------------------------------------------
        self.log_phase('Phase 6 : Writing entities.json')

        entities = list(Entity.objects.order_by('id'))

        entity_records = []
        for d in rows:
            entity_records.append({
                'id': d['id'],
                'entity_code': d['entity_code'],
                'display_name': d['display_name'],
                'sort_name': d['sort_name'],
                'entity_type': d['entity_type'],
                #'roles': d['roles'],
                'given_name': d['given_name'],
                'surname': d['surname'],
                'honorific': d['honorific'],
                'date_earliest': str(d['date_start']),
                'date_latest': str(d['date_end']),
                'name_variants': d['name_variants'],
                'primary_function': d['primary_function'],
                'functions': d['functions'],
                'dates_of_existence': str(d['dates_of_existence']),
                'history': d['history'],
                'viaf_id': d['viaf_id'],
                'wikidata_id': d['wikidata_id'],
                # 'date_formatted': d['']
                'created_at': d['created_at'].isoformat(),
                'modified_at': d['updated_at'].isoformat(),
            })

            #print(entity_records)

        repo_path = os.path.join(output_dir, 'entities.json')
        with open(repo_path, 'w') as f:
            json.dump(entity_records, f, ensure_ascii=False)

        size_kb = os.path.getsize(repo_path) / 1024
        self.log(f'  Wrote {len(entity_records)} entities ({size_kb:.1f} KB)')
        self.log_elapsed()

        # ------------------------------------------------------------------
        # Phase 7: Load entity-links
        # ------------------------------------------------------------------
        self.log_phase('Phase 7: Loading entity links')

        entitylink_rows = list(
            DescriptionEntity.objects
            .select_related('description', 'entity', 'description__repository')
            .order_by('id')
            .values(
                'description__reference_code', 'description__date_expression', 'description__date_start',
                'description__repository', 'description__repository__code', 'description__title', 'entity__entity_code',
                'role',
            )
        )
        self.log(f'  Loaded {len(rows):,} entity links')
        self.log_elapsed()

        # ------------------------------------------------------------------
        # Phase 8: Write entity_links.json
        # ------------------------------------------------------------------
        self.log_phase('Phase 8 : Writing entity_links.json')

        en = list(DescriptionPlace.objects.order_by('id'))

        en_records = []
        for d in entitylink_rows:
            en_records.append({
                'entity_code': d['entity__entity_code'],
                'reference_code': d['description__reference_code'],
                'title': d['description__title'],
                'date_expression': d['description__date_expression'],
                'date_start': (
                    d['description__date_start'].isoformat() if d['description__date_start'] else None
                ),
                'repository_code':  d['description__repository__code'],
                'role': d['role'],
            })

            #print(entity_records)

        repo_path = os.path.join(output_dir, 'entity_links.json')
        with open(repo_path, 'w') as f:
            json.dump(en_records, f, ensure_ascii=False)

        size_kb = os.path.getsize(repo_path) / 1024
        self.log(f'  Wrote {len(en_records)} entity links ({size_kb:.1f} KB)')
        self.log_elapsed()

        # ------------------------------------------------------------------
        # Summary
        # ------------------------------------------------------------------
        self.log_phase('Summary')

        elapsed = time.time() - start_time
        self.log(f'  Places: {len(place_records):,}')
        self.log(f'  Place links: {len(pl_records):,}')
        self.log(f'  Entities: {len(entity_records):,}')
        self.log(f'  Entity links: {len(en_records):,}')
        self.log(f'  Output directory: {os.path.abspath(output_dir)}')
        self.log(f'  Total time: {elapsed:.1f}s')
