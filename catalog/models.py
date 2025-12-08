"""
Fisqua Catalog Models

Core data models implementing the Fisqua database schema v0.1:
- Repository: Top-level institutions/archives
- CatalogUnit: Universal archival descriptions (ISAD(G)/MEAP/EAP compliant)
- Place: Geocoded geographic entities
- CatalogUnitPlace: Links catalog units to places
"""

from django.db import models
from django.contrib.auth.models import User
from django.core.validators import MinValueValidator, MaxValueValidator
from mptt.models import MPTTModel, TreeForeignKey


class Repository(models.Model):
    """
    Top-level institutions that own collections.
    Each repository defines its own hierarchy and standards.
    """

    class MetadataStandard(models.TextChoices):
        MEAP = 'MEAP', 'MEAP'
        EAP = 'EAP', 'EAP'
        ISADG = 'ISADG', 'ISAD(G)'
        HYBRID = 'HYBRID', 'Hybrid'
        CUSTOM = 'CUSTOM', 'Custom'

    class InstitutionType(models.TextChoices):
        ARCHIVE = 'archive', 'Archive'
        LIBRARY = 'library', 'Library'
        MUSEUM = 'museum', 'Museum'
        UNIVERSITY = 'university', 'University'
        RESEARCH_CENTER = 'research_center', 'Research Center'
        GOVERNMENT = 'government', 'Government'
        PRIVATE = 'private', 'Private Collection'
        OTHER = 'other', 'Other'

    # Basic Information
    name = models.CharField(max_length=500)
    name_translations = models.JSONField(blank=True, null=True,
        help_text='Translations: {"es": "Nombre", "en": "Name"}')
    abbreviation = models.CharField(max_length=50, blank=True)
    repository_code = models.CharField(max_length=50, unique=True, blank=True, null=True,
        help_text='ISO 15511 code if available')

    # Contact & Location
    institution_type = models.CharField(max_length=100, choices=InstitutionType.choices,
        blank=True)
    country_code = models.CharField(max_length=3, blank=True,
        help_text='ISO 3166-1 alpha-3')
    city = models.CharField(max_length=255, blank=True)
    region = models.CharField(max_length=255, blank=True)
    address = models.TextField(blank=True)
    website_url = models.URLField(blank=True)
    contact_email = models.EmailField(blank=True)
    contact_phone = models.CharField(max_length=50, blank=True)

    # Standards & Settings
    default_metadata_standard = models.CharField(max_length=20,
        choices=MetadataStandard.choices, default=MetadataStandard.MEAP)
    supports_isadg = models.BooleanField(default=False)
    supports_isaar = models.BooleanField(default=False)
    default_language_code = models.CharField(max_length=10, default='en',
        help_text='ISO 639-2 code')

    # Administrative
    is_active = models.BooleanField(default=True)
    notes = models.TextField(blank=True)

    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name_plural = 'repositories'
        ordering = ['name']

    def __str__(self):
        if self.abbreviation:
            return f"{self.name} ({self.abbreviation})"
        return self.name


class CatalogUnit(MPTTModel):
    """
    Universal table for ALL archival descriptions.

    Supports ISAD(G), EAP, MEAP, and custom structures.
    Uses MPTT for efficient hierarchical queries.
    Catalog-first model: Digital files are optional attachments.
    """

    class MetadataStandard(models.TextChoices):
        MEAP = 'MEAP', 'MEAP'
        EAP = 'EAP', 'EAP'
        ISADG = 'ISADG', 'ISAD(G)'
        HYBRID = 'HYBRID', 'Hybrid'
        CUSTOM = 'CUSTOM', 'Custom'

    class AccessCondition(models.TextChoices):
        OPEN = 'open', 'Open'
        RESTRICTED = 'restricted', 'Restricted'
        CLOSED = 'closed', 'Closed'

    class DescriptionStatus(models.TextChoices):
        DRAFT = 'draft', 'Draft'
        IN_PROGRESS = 'in_progress', 'In Progress'
        FINAL = 'final', 'Final'
        REVISED = 'revised', 'Revised'

    # =========================================================================
    # CORE IDENTITY & HIERARCHY
    # =========================================================================

    repository = models.ForeignKey(Repository, on_delete=models.CASCADE,
        related_name='catalog_units')
    parent = TreeForeignKey('self', on_delete=models.CASCADE, null=True, blank=True,
        related_name='children')

    metadata_standard = models.CharField(max_length=20,
        choices=MetadataStandard.choices, default=MetadataStandard.MEAP)

    # Hierarchy/Level Information
    level_type = models.CharField(max_length=100, blank=True,
        help_text='Repository-defined: fonds, series, collection, item, etc.')
    level_name = models.CharField(max_length=255, blank=True,
        help_text='Human-readable in local language: Fondo, Serie, Colección')

    # Reference Codes & Identifiers
    reference_code = models.CharField(max_length=500, unique=True, blank=True, null=True,
        help_text='ISAD(G) 3.1.1: CountryCode-RepoCode-LocalRef')
    local_identifier = models.CharField(max_length=255, blank=True,
        help_text='Internal repository identifier')
    ark = models.CharField(max_length=255, unique=True, blank=True, null=True,
        help_text='MEAP: Archival Resource Key')
    original_reference = models.CharField(max_length=500, blank=True,
        help_text='EAP: Pre-existing shelfmarks')

    # =========================================================================
    # ISAD(G) 3.1 IDENTITY STATEMENT AREA
    # =========================================================================

    # 3.1.2 Title (Mandatory)
    title = models.TextField()
    title_original_language = models.TextField(blank=True,
        help_text='EAP: Title in original script')
    title_transliterated = models.TextField(blank=True,
        help_text='EAP: Romanized title')
    translated_title = models.TextField(blank=True,
        help_text='MEAP: English translation')
    uniform_title = models.CharField(max_length=500, blank=True,
        help_text='MEAP: For periodicals')

    # 3.1.3 Date(s)
    date_expression = models.CharField(max_length=500, blank=True,
        help_text='Human-readable date (any format/language)')
    date_type = models.CharField(max_length=50, blank=True,
        help_text='creation | accumulation | publication | other')
    date_start = models.DateField(null=True, blank=True)
    date_end = models.DateField(null=True, blank=True)
    date_start_approximation = models.CharField(max_length=20, blank=True,
        help_text='circa | before | after | unknown')
    date_end_approximation = models.CharField(max_length=20, blank=True)
    date_bulk_start = models.DateField(null=True, blank=True)
    date_bulk_end = models.DateField(null=True, blank=True)
    date_note = models.TextField(blank=True)

    # Alternative Calendar (EAP)
    alternative_calendar = models.CharField(max_length=50, blank=True)
    alternative_calendar_dates = models.CharField(max_length=500, blank=True)

    # 3.1.4 Level of Description
    isadg_level = models.CharField(max_length=50, blank=True,
        help_text='fonds | sub-fonds | series | sub-series | file | item | part')
    eap_level = models.CharField(max_length=50, blank=True,
        help_text='Collection | Series | Sub-series | File | Item')

    # 3.1.5 Extent and Medium
    extent_expression = models.CharField(max_length=500, blank=True,
        help_text='Human-readable: "103.5 cubic feet (98 boxes)"')
    extent_quantity = models.DecimalField(max_digits=10, decimal_places=2,
        null=True, blank=True)
    extent_unit = models.CharField(max_length=100, blank=True,
        help_text='cubic_feet | linear_meters | items | pages | files')
    extent_note = models.TextField(blank=True)

    # Physical characteristics
    dimensions = models.CharField(max_length=255, blank=True)
    medium = models.CharField(max_length=255, blank=True)
    duration = models.CharField(max_length=50, blank=True,
        help_text='For audio/video: HH:MM:SS')
    condition = models.TextField(blank=True)

    # =========================================================================
    # ISAD(G) 3.2 CONTEXT AREA
    # =========================================================================

    creator_string = models.TextField(blank=True,
        help_text='Simple text if not using authority control')
    administrative_history = models.TextField(blank=True)
    biographical_history = models.TextField(blank=True)
    archival_history = models.TextField(blank=True)
    custodial_history = models.TextField(blank=True)

    acquisition_source = models.TextField(blank=True)
    acquisition_date = models.DateField(null=True, blank=True)
    acquisition_method = models.CharField(max_length=100, blank=True)
    acquisition_note = models.TextField(blank=True)

    # =========================================================================
    # ISAD(G) 3.3 CONTENT AND STRUCTURE AREA
    # =========================================================================

    description = models.TextField(blank=True)
    description_translations = models.JSONField(blank=True, null=True)
    scope_content = models.TextField(blank=True)

    resource_type = models.CharField(max_length=100, blank=True,
        help_text='still_image | text | sound | moving_image | etc.')

    appraisal_destruction = models.TextField(blank=True)
    accruals = models.TextField(blank=True)
    system_of_arrangement = models.TextField(blank=True)

    # =========================================================================
    # ISAD(G) 3.4 CONDITIONS OF ACCESS AND USE AREA
    # =========================================================================

    access_conditions = models.CharField(max_length=100,
        choices=AccessCondition.choices, blank=True)
    access_restrictions_note = models.TextField(blank=True)
    access_restriction_type = models.CharField(max_length=100, blank=True)
    access_restriction_end_date = models.DateField(null=True, blank=True)

    eap_access_status = models.CharField(max_length=50, blank=True)
    eap_restriction_reason = models.TextField(blank=True)

    contains_sensitive_data = models.BooleanField(default=False)
    sensitive_data_nature = models.TextField(blank=True)

    reproduction_conditions = models.TextField(blank=True)

    # Rights Information
    rights_copyright_status = models.CharField(max_length=100, blank=True)
    rights_publication_status = models.CharField(max_length=100, blank=True)
    rights_holder_name = models.TextField(blank=True)
    rights_holder_contact = models.TextField(blank=True)
    rights_statement = models.TextField(blank=True)
    rights_attribution = models.TextField(blank=True)
    rights_license = models.CharField(max_length=100, blank=True)
    rights_note = models.TextField(blank=True)

    # Language and Scripts
    language_codes = models.JSONField(blank=True, null=True,
        help_text='ISO 639-2 codes: ["eng", "spa", "que"]')
    language_note = models.TextField(blank=True)
    script_codes = models.JSONField(blank=True, null=True,
        help_text='ISO 15924 codes: ["Latn", "Arab"]')
    writing_system = models.CharField(max_length=50, blank=True)

    physical_characteristics = models.TextField(blank=True)
    technical_requirements = models.TextField(blank=True)

    finding_aids = models.TextField(blank=True)
    finding_aid_url = models.URLField(blank=True)

    # =========================================================================
    # ISAD(G) 3.5 ALLIED MATERIALS AREA
    # =========================================================================

    location_of_originals = models.TextField(blank=True)
    physical_location_institution = models.CharField(max_length=500, blank=True)
    physical_location_country = models.CharField(max_length=100, blank=True)
    physical_location_city = models.CharField(max_length=255, blank=True)
    physical_collection_title = models.CharField(max_length=500, blank=True)
    physical_collection_number = models.CharField(max_length=255, blank=True)
    physical_box = models.CharField(max_length=50, blank=True)
    physical_folder = models.CharField(max_length=50, blank=True)
    physical_location_note = models.TextField(blank=True)

    location_of_copies = models.TextField(blank=True)
    related_units = models.TextField(blank=True)
    publication_note = models.TextField(blank=True)

    # =========================================================================
    # ISAD(G) 3.6 NOTES AREA
    # =========================================================================

    notes = models.TextField(blank=True)
    notes_translations = models.JSONField(blank=True, null=True)
    internal_notes = models.TextField(blank=True,
        help_text='Staff-only notes, not published')

    # =========================================================================
    # ISAD(G) 3.7 DESCRIPTION CONTROL AREA
    # =========================================================================

    archivist_note = models.TextField(blank=True)
    cataloger_name = models.CharField(max_length=255, blank=True)
    rules_conventions = models.TextField(blank=True)
    description_date = models.DateField(null=True, blank=True)
    description_revision_date = models.DateField(null=True, blank=True)
    description_status = models.CharField(max_length=50,
        choices=DescriptionStatus.choices, blank=True)
    statement_of_responsibility = models.TextField(blank=True)

    # =========================================================================
    # PROVENANCE DETAILS (MEAP/EAP)
    # =========================================================================

    author = models.TextField(blank=True)
    scribe = models.TextField(blank=True)
    publisher = models.TextField(blank=True)
    publisher_location = models.CharField(max_length=255, blank=True)
    editor = models.TextField(blank=True)
    photographer = models.TextField(blank=True)
    artist = models.TextField(blank=True)
    composer = models.TextField(blank=True)
    director = models.TextField(blank=True)

    volume_number = models.CharField(max_length=50, blank=True)
    issue_number = models.CharField(max_length=50, blank=True)
    page_number = models.CharField(max_length=50, blank=True)

    # =========================================================================
    # SUBJECTS & KEYWORDS
    # =========================================================================

    subjects_topic = models.JSONField(blank=True, null=True)
    subjects_geographic = models.JSONField(blank=True, null=True)
    subjects_temporal = models.JSONField(blank=True, null=True)
    subjects_religion = models.JSONField(blank=True, null=True)
    subjects_name_string = models.JSONField(blank=True, null=True)
    related_title_of_works = models.JSONField(blank=True, null=True)

    # =========================================================================
    # DIGITAL ATTACHMENTS
    # =========================================================================

    external_url = models.URLField(blank=True)
    external_url_label = models.CharField(max_length=255, blank=True)

    iiif_manifest_url = models.URLField(blank=True)
    iiif_manifest_version = models.CharField(max_length=10, blank=True)

    digital_folder_name = models.CharField(max_length=500, blank=True)
    digital_file_count = models.IntegerField(default=0)
    digital_file_format = models.CharField(max_length=50, blank=True)
    digitization_date = models.DateField(null=True, blank=True)
    digitization_notes = models.TextField(blank=True)

    # =========================================================================
    # COMPUTED & DISPLAY
    # =========================================================================

    has_digital_files = models.BooleanField(default=False)
    has_external_link = models.BooleanField(default=False)
    descendant_count = models.IntegerField(default=0)

    sequence_number = models.IntegerField(null=True, blank=True)
    sort_key = models.CharField(max_length=500, blank=True)

    # =========================================================================
    # PUBLICATION & VISIBILITY
    # =========================================================================

    is_published = models.BooleanField(default=False)
    publication_date = models.DateField(null=True, blank=True)
    featured = models.BooleanField(default=False)

    # =========================================================================
    # METADATA
    # =========================================================================

    created_by = models.ForeignKey(User, on_delete=models.SET_NULL,
        null=True, blank=True, related_name='created_catalog_units')
    updated_by = models.ForeignKey(User, on_delete=models.SET_NULL,
        null=True, blank=True, related_name='updated_catalog_units')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class MPTTMeta:
        order_insertion_by = ['sequence_number', 'title']

    class Meta:
        verbose_name = 'catalog unit'
        verbose_name_plural = 'catalog units'
        ordering = ['tree_id', 'lft']

    def __str__(self):
        return self.title[:100] if len(self.title) > 100 else self.title

    def get_breadcrumb(self):
        """Return list of ancestors for breadcrumb navigation."""
        return self.get_ancestors(include_self=True)


class Place(models.Model):
    """
    Geocoded geographic entities (gazetteer).
    Supports historical place names and colonial administrative hierarchies.
    """

    class PlaceType(models.TextChoices):
        CITY = 'city', 'City'
        TOWN = 'town', 'Town'
        VILLAGE = 'village', 'Village'
        REGION = 'region', 'Region'
        PROVINCE = 'province', 'Province'
        DEPARTMENT = 'department', 'Department'
        COUNTRY = 'country', 'Country'
        RIVER = 'river', 'River'
        MOUNTAIN = 'mountain', 'Mountain'
        OTHER = 'other', 'Other'

    # Identification
    gazetteer_id = models.CharField(max_length=100, blank=True,
        help_text='External ID (e.g., GeoNames, custom gazetteer)')
    gazetteer_source = models.CharField(max_length=100, blank=True,
        help_text='Source gazetteer name')

    # Names
    label = models.CharField(max_length=255,
        help_text='Normalized modern name')
    historical_name = models.CharField(max_length=500, blank=True,
        help_text='Period-specific or original name')

    # Classification
    place_type = models.CharField(max_length=100, choices=PlaceType.choices, blank=True)

    # Geocoding
    latitude = models.DecimalField(max_digits=10, decimal_places=8,
        null=True, blank=True,
        validators=[MinValueValidator(-90), MaxValueValidator(90)])
    longitude = models.DecimalField(max_digits=11, decimal_places=8,
        null=True, blank=True,
        validators=[MinValueValidator(-180), MaxValueValidator(180)])
    coordinate_precision = models.CharField(max_length=50, blank=True,
        help_text='exact | approximate | centroid')
    coordinate_source = models.CharField(max_length=255, blank=True)

    # Modern administrative divisions
    country_code = models.CharField(max_length=3, blank=True,
        help_text='ISO 3166-1 alpha-3')
    admin_level_1 = models.CharField(max_length=255, blank=True,
        help_text='State/Department/Region')
    admin_level_2 = models.CharField(max_length=255, blank=True,
        help_text='Province/County')
    admin_level_3 = models.CharField(max_length=255, blank=True,
        help_text='Municipality/District')

    # Historical administrative divisions (colonial period)
    historical_admin_1 = models.CharField(max_length=255, blank=True,
        help_text='e.g., "Popayan" (Gobernación)')
    historical_admin_2 = models.CharField(max_length=255, blank=True,
        help_text='e.g., "Cali" (Partido)')
    historical_region = models.CharField(max_length=100, blank=True,
        help_text='e.g., "QUI" (Quito audiencia)')

    # Hierarchy within gazetteer
    parent_place = models.ForeignKey('self', on_delete=models.SET_NULL,
        null=True, blank=True, related_name='child_places')

    # Metadata
    notes = models.TextField(blank=True)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name_plural = 'places'
        ordering = ['label']

    def __str__(self):
        if self.historical_name and self.historical_name != self.label:
            return f"{self.label} ({self.historical_name})"
        return self.label

    @property
    def coordinates(self):
        """Return (lat, lon) tuple if both are set."""
        if self.latitude and self.longitude:
            return (float(self.latitude), float(self.longitude))
        return None


class CatalogUnitPlace(models.Model):
    """
    Links catalog units to places with role context.
    """

    class PlaceRole(models.TextChoices):
        MENTIONED = 'mentioned', 'Mentioned'
        CREATED_AT = 'created_at', 'Created At'
        SUBJECT = 'subject', 'Subject'
        SENT_FROM = 'sent_from', 'Sent From'
        SENT_TO = 'sent_to', 'Sent To'
        PUBLISHED_AT = 'published_at', 'Published At'
        DEPICTED = 'depicted', 'Depicted'
        OTHER = 'other', 'Other'

    catalog_unit = models.ForeignKey(CatalogUnit, on_delete=models.CASCADE,
        related_name='place_links')
    place = models.ForeignKey(Place, on_delete=models.CASCADE,
        related_name='catalog_unit_links')

    place_role = models.CharField(max_length=50, choices=PlaceRole.choices,
        default=PlaceRole.MENTIONED)
    role_note = models.TextField(blank=True)
    sequence_number = models.IntegerField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = 'catalog unit place'
        verbose_name_plural = 'catalog unit places'
        ordering = ['sequence_number', 'place__label']
        constraints = [
            models.UniqueConstraint(
                fields=['catalog_unit', 'place', 'place_role'],
                name='unique_catalog_unit_place_role'
            )
        ]

    def __str__(self):
        return f"{self.catalog_unit} → {self.place} ({self.get_place_role_display()})"
