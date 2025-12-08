"""
Fisqua Catalog Admin Configuration

Admin interfaces for Repository, CatalogUnit, Place, and CatalogUnitPlace models.
"""

from django.contrib import admin
from mptt.admin import MPTTModelAdmin

from .models import Repository, CatalogUnit, Place, CatalogUnitPlace


class CatalogUnitPlaceInline(admin.TabularInline):
    """Inline for editing place associations on CatalogUnit."""
    model = CatalogUnitPlace
    extra = 1
    autocomplete_fields = ['place']


@admin.register(Repository)
class RepositoryAdmin(admin.ModelAdmin):
    list_display = ['name', 'abbreviation', 'repository_code', 'country_code',
                    'city', 'default_metadata_standard', 'is_active']
    list_filter = ['is_active', 'default_metadata_standard', 'institution_type',
                   'country_code']
    search_fields = ['name', 'abbreviation', 'repository_code', 'city']
    ordering = ['name']

    fieldsets = (
        ('Basic Information', {
            'fields': ('name', 'name_translations', 'abbreviation', 'repository_code')
        }),
        ('Contact & Location', {
            'fields': ('institution_type', 'country_code', 'region', 'city',
                       'address', 'website_url', 'contact_email', 'contact_phone')
        }),
        ('Standards & Settings', {
            'fields': ('default_metadata_standard', 'supports_isadg',
                       'supports_isaar', 'default_language_code')
        }),
        ('Administrative', {
            'fields': ('is_active', 'notes')
        }),
    )


@admin.register(CatalogUnit)
class CatalogUnitAdmin(MPTTModelAdmin):
    list_display = ['title_short', 'reference_code', 'repository', 'level_type',
                    'date_expression', 'is_published']
    list_filter = ['repository', 'metadata_standard', 'level_type', 'is_published',
                   'access_conditions']
    search_fields = ['title', 'reference_code', 'local_identifier', 'description',
                     'scope_content']
    autocomplete_fields = ['repository', 'parent', 'created_by', 'updated_by']
    ordering = ['tree_id', 'lft']
    date_hierarchy = 'date_start'

    inlines = [CatalogUnitPlaceInline]

    fieldsets = (
        ('Identity & Hierarchy', {
            'fields': ('repository', 'parent', 'metadata_standard',
                       'level_type', 'level_name', 'reference_code',
                       'local_identifier', 'ark', 'original_reference')
        }),
        ('Title', {
            'fields': ('title', 'title_original_language', 'title_transliterated',
                       'translated_title', 'uniform_title')
        }),
        ('Dates', {
            'fields': ('date_expression', 'date_type', 'date_start', 'date_end',
                       'date_start_approximation', 'date_end_approximation',
                       'date_bulk_start', 'date_bulk_end', 'date_note',
                       'alternative_calendar', 'alternative_calendar_dates'),
            'classes': ('collapse',)
        }),
        ('Level & Extent', {
            'fields': ('isadg_level', 'eap_level', 'extent_expression',
                       'extent_quantity', 'extent_unit', 'extent_note',
                       'dimensions', 'medium', 'duration', 'condition'),
            'classes': ('collapse',)
        }),
        ('Context', {
            'fields': ('creator_string', 'administrative_history',
                       'biographical_history', 'archival_history',
                       'custodial_history', 'acquisition_source',
                       'acquisition_date', 'acquisition_method', 'acquisition_note'),
            'classes': ('collapse',)
        }),
        ('Content & Structure', {
            'fields': ('description', 'description_translations', 'scope_content',
                       'resource_type', 'appraisal_destruction', 'accruals',
                       'system_of_arrangement')
        }),
        ('Access & Rights', {
            'fields': ('access_conditions', 'access_restrictions_note',
                       'access_restriction_type', 'access_restriction_end_date',
                       'eap_access_status', 'eap_restriction_reason',
                       'contains_sensitive_data', 'sensitive_data_nature',
                       'reproduction_conditions', 'rights_copyright_status',
                       'rights_publication_status', 'rights_holder_name',
                       'rights_statement', 'rights_license', 'rights_note'),
            'classes': ('collapse',)
        }),
        ('Language & Scripts', {
            'fields': ('language_codes', 'language_note', 'script_codes',
                       'writing_system'),
            'classes': ('collapse',)
        }),
        ('Physical & Finding Aids', {
            'fields': ('physical_characteristics', 'technical_requirements',
                       'finding_aids', 'finding_aid_url'),
            'classes': ('collapse',)
        }),
        ('Allied Materials', {
            'fields': ('location_of_originals', 'physical_location_institution',
                       'physical_location_country', 'physical_location_city',
                       'physical_collection_title', 'physical_collection_number',
                       'physical_box', 'physical_folder', 'physical_location_note',
                       'location_of_copies', 'related_units', 'publication_note'),
            'classes': ('collapse',)
        }),
        ('Notes', {
            'fields': ('notes', 'notes_translations', 'internal_notes')
        }),
        ('Description Control', {
            'fields': ('archivist_note', 'cataloger_name', 'rules_conventions',
                       'description_date', 'description_revision_date',
                       'description_status', 'statement_of_responsibility'),
            'classes': ('collapse',)
        }),
        ('Provenance Details', {
            'fields': ('author', 'scribe', 'publisher', 'publisher_location',
                       'editor', 'photographer', 'artist', 'composer', 'director',
                       'volume_number', 'issue_number', 'page_number'),
            'classes': ('collapse',)
        }),
        ('Subjects & Keywords', {
            'fields': ('subjects_topic', 'subjects_geographic', 'subjects_temporal',
                       'subjects_religion', 'subjects_name_string',
                       'related_title_of_works'),
            'classes': ('collapse',)
        }),
        ('Digital', {
            'fields': ('external_url', 'external_url_label', 'iiif_manifest_url',
                       'iiif_manifest_version', 'digital_folder_name',
                       'digital_file_count', 'digital_file_format',
                       'digitization_date', 'digitization_notes',
                       'has_digital_files', 'has_external_link'),
            'classes': ('collapse',)
        }),
        ('Display & Publication', {
            'fields': ('sequence_number', 'sort_key', 'descendant_count',
                       'is_published', 'publication_date', 'featured')
        }),
        ('Metadata', {
            'fields': ('created_by', 'updated_by'),
            'classes': ('collapse',)
        }),
    )

    def title_short(self, obj):
        """Truncated title for list display."""
        return obj.title[:80] + '...' if len(obj.title) > 80 else obj.title
    title_short.short_description = 'Title'


@admin.register(Place)
class PlaceAdmin(admin.ModelAdmin):
    list_display = ['label', 'historical_name', 'place_type', 'latitude',
                    'longitude', 'historical_admin_1', 'is_active']
    list_filter = ['place_type', 'is_active', 'country_code', 'historical_region',
                   'historical_admin_1']
    search_fields = ['label', 'historical_name', 'gazetteer_id']
    ordering = ['label']

    fieldsets = (
        ('Identification', {
            'fields': ('gazetteer_id', 'gazetteer_source', 'label', 'historical_name',
                       'place_type')
        }),
        ('Geocoding', {
            'fields': ('latitude', 'longitude', 'coordinate_precision',
                       'coordinate_source')
        }),
        ('Modern Administrative', {
            'fields': ('country_code', 'admin_level_1', 'admin_level_2',
                       'admin_level_3')
        }),
        ('Historical Administrative (Colonial)', {
            'fields': ('historical_admin_1', 'historical_admin_2', 'historical_region')
        }),
        ('Hierarchy & Metadata', {
            'fields': ('parent_place', 'notes', 'is_active')
        }),
    )


@admin.register(CatalogUnitPlace)
class CatalogUnitPlaceAdmin(admin.ModelAdmin):
    list_display = ['catalog_unit', 'place', 'place_role', 'sequence_number']
    list_filter = ['place_role']
    search_fields = ['catalog_unit__title', 'place__label']
    autocomplete_fields = ['catalog_unit', 'place']
    ordering = ['catalog_unit', 'sequence_number']
