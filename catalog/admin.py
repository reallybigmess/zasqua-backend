"""
Zasqua Catalog Admin Configuration

Admin interfaces for the 6-table schema:
- Repository, Description, Entity, Place
- DescriptionEntity, DescriptionPlace (junction tables)
"""

from django.contrib import admin
from mptt.admin import MPTTModelAdmin

from .models import (
    Repository, Description, Entity, Place,
    DescriptionEntity, DescriptionPlace
)


class DescriptionEntityInline(admin.TabularInline):
    """Inline for linking entities to descriptions."""
    model = DescriptionEntity
    extra = 1
    autocomplete_fields = ['entity']


class DescriptionPlaceInline(admin.TabularInline):
    """Inline for linking places to descriptions."""
    model = DescriptionPlace
    extra = 1
    autocomplete_fields = ['place']


@admin.register(Repository)
class RepositoryAdmin(admin.ModelAdmin):
    list_display = ['name', 'code', 'city', 'country_code', 'enabled']
    list_filter = ['enabled', 'country_code']
    search_fields = ['name', 'code', 'city']
    ordering = ['name']

    fieldsets = (
        (None, {
            'fields': ('code', 'name', 'country_code', 'city')
        }),
        ('Details', {
            'classes': ('collapse',),
            'fields': ('address', 'website', 'notes', 'enabled')
        }),
    )


@admin.register(Description)
class DescriptionAdmin(MPTTModelAdmin):
    """
    Admin for archival descriptions.
    Essential fields visible, rest collapsed by ISAD(G) area.
    """

    list_display = ['title_short', 'reference_code', 'repository', 'level',
                    'date_expression', 'is_published']
    list_filter = ['repository', 'level', 'resource_type', 'is_published',
                   'needs_review', 'has_digital']
    search_fields = ['title', 'reference_code', 'local_identifier',
                     'scope_content', 'creator_display']
    autocomplete_fields = ['repository', 'parent']
    ordering = ['tree_id', 'lft']
    date_hierarchy = 'date_start'

    inlines = [DescriptionEntityInline, DescriptionPlaceInline]

    fieldsets = (
        # Essential fields - always visible
        ('Essential', {
            'fields': (
                'repository',
                'parent',
                ('level', 'resource_type'),
                ('reference_code', 'local_identifier'),
                'title',
                'translated_title',
                ('date_expression', 'date_start', 'date_end'),
                'extent',
                'scope_content',
                'language',
                ('is_published', 'has_digital'),
            )
        }),

        # Identity (ISAD 3.1)
        ('Identity Details', {
            'classes': ('collapse',),
            'fields': (
                'uniform_title',
                'genre',
                'date_certainty',
                ('dimensions', 'medium'),
            )
        }),

        # Bibliographic (printed materials)
        ('Bibliographic', {
            'classes': ('collapse',),
            'fields': (
                'imprint',
                'edition_statement',
                'series_statement',
                ('volume_number', 'issue_number'),
                'pages',
            )
        }),

        # Context (ISAD 3.2)
        ('Context', {
            'classes': ('collapse',),
            'fields': ('provenance',)
        }),

        # Content (ISAD 3.3)
        ('Content & Structure', {
            'classes': ('collapse',),
            'fields': ('arrangement',)
        }),

        # Access (ISAD 3.4)
        ('Access & Rights', {
            'classes': ('collapse',),
            'fields': (
                'access_conditions',
                'reproduction_conditions',
                ('rights_status', 'rights_holder'),
                'rights_statement',
            )
        }),

        # Allied Materials (ISAD 3.5)
        ('Allied Materials', {
            'classes': ('collapse',),
            'fields': (
                'location_of_originals',
                'related_materials',
            )
        }),

        # Notes (ISAD 3.6)
        ('Notes', {
            'classes': ('collapse',),
            'fields': (
                'notes',
                'internal_notes',
            )
        }),

        # Denormalized / Display
        ('Display Fields', {
            'classes': ('collapse',),
            'fields': (
                'creator_display',
                'place_display',
                'path_cache',
            )
        }),

        # Digital
        ('Digital', {
            'classes': ('collapse',),
            'fields': ('iiif_manifest_url',)
        }),

        # Workflow
        ('Workflow', {
            'classes': ('collapse',),
            'fields': (
                ('needs_review', 'review_note'),
            )
        }),

        # Provenance (CA migration)
        ('CA Migration', {
            'classes': ('collapse',),
            'fields': (
                ('ca_object_id', 'ca_collection_id'),
            )
        }),
    )

    def title_short(self, obj):
        """Truncated title for list display."""
        return obj.title[:80] + '...' if len(obj.title) > 80 else obj.title
    title_short.short_description = 'Title'


@admin.register(Entity)
class EntityAdmin(admin.ModelAdmin):
    list_display = ['display_name', 'entity_type', 'dates_of_existence',
                    'needs_review']
    list_filter = ['entity_type', 'needs_review']
    search_fields = ['display_name', 'sort_name', 'name_variants']
    ordering = ['sort_name']

    fieldsets = (
        (None, {
            'fields': (
                ('display_name', 'sort_name'),
                'entity_type',
                'name_variants',
            )
        }),
        ('Dates & History', {
            'classes': ('collapse',),
            'fields': (
                'dates_of_existence',
                ('date_start', 'date_end'),
                'history',
            )
        }),
        ('Corporate Bodies', {
            'classes': ('collapse',),
            'fields': ('legal_status', 'functions')
        }),
        ('Control', {
            'classes': ('collapse',),
            'fields': ('sources',)
        }),
        ('Workflow', {
            'classes': ('collapse',),
            'fields': (
                ('needs_review', 'review_note'),
                'merged_into',
            )
        }),
        ('CA Migration', {
            'classes': ('collapse',),
            'fields': ('ca_entity_id',)
        }),
    )


@admin.register(Place)
class PlaceAdmin(admin.ModelAdmin):
    list_display = ['label', 'display_name', 'place_type', 'latitude',
                    'longitude', 'needs_geocoding', 'needs_review']
    list_filter = ['place_type', 'needs_geocoding', 'needs_review',
                   'country_code', 'colonial_gobernacion']
    search_fields = ['label', 'display_name', 'name_variants']
    ordering = ['label']

    fieldsets = (
        (None, {
            'fields': (
                ('label', 'display_name'),
                'place_type',
                'name_variants',
                'parent',
            )
        }),
        ('Geography', {
            'fields': (
                ('latitude', 'longitude'),
                'coordinate_precision',
            )
        }),
        ('Colonial Context', {
            'classes': ('collapse',),
            'fields': (
                'colonial_gobernacion',
                'colonial_partido',
                'colonial_region',
            )
        }),
        ('Modern Context', {
            'classes': ('collapse',),
            'fields': (
                'country_code',
                'admin_level_1',
                'admin_level_2',
            )
        }),
        ('Workflow', {
            'classes': ('collapse',),
            'fields': (
                ('needs_geocoding', 'needs_review'),
                'review_note',
                'merged_into',
            )
        }),
        ('CA Migration', {
            'classes': ('collapse',),
            'fields': ('ca_place_ids',)
        }),
    )


@admin.register(DescriptionEntity)
class DescriptionEntityAdmin(admin.ModelAdmin):
    list_display = ['description', 'entity', 'role', 'sequence', 'needs_review']
    list_filter = ['role', 'needs_review']
    search_fields = ['description__title', 'entity__display_name']
    autocomplete_fields = ['description', 'entity']
    ordering = ['description', 'sequence']


@admin.register(DescriptionPlace)
class DescriptionPlaceAdmin(admin.ModelAdmin):
    list_display = ['description', 'place', 'role', 'needs_review']
    list_filter = ['role', 'needs_review']
    search_fields = ['description__title', 'place__label']
    autocomplete_fields = ['description', 'place']
    ordering = ['description']
