"""
REST API Serializers for Fisqua Catalog.
"""

from rest_framework import serializers
from .models import Repository, CatalogUnit, Place, CatalogUnitPlace


class RepositoryListSerializer(serializers.ModelSerializer):
    """Compact serializer for repository listings."""
    catalog_unit_count = serializers.SerializerMethodField()

    class Meta:
        model = Repository
        fields = [
            'id', 'name', 'abbreviation', 'repository_code',
            'institution_type', 'city', 'region', 'country_code',
            'catalog_unit_count'
        ]

    def get_catalog_unit_count(self, obj):
        return obj.catalog_units.count()


class RepositoryDetailSerializer(serializers.ModelSerializer):
    """Full serializer for repository detail view."""
    catalog_unit_count = serializers.SerializerMethodField()
    root_units = serializers.SerializerMethodField()

    class Meta:
        model = Repository
        fields = '__all__'

    def get_catalog_unit_count(self, obj):
        return obj.catalog_units.count()

    def get_root_units(self, obj):
        """Return top-level catalog units for this repository."""
        roots = obj.catalog_units.filter(parent__isnull=True)[:20]
        return CatalogUnitListSerializer(roots, many=True).data


class PlaceSerializer(serializers.ModelSerializer):
    """Serializer for Place model."""

    class Meta:
        model = Place
        fields = [
            'id', 'label', 'historical_name', 'place_type',
            'latitude', 'longitude', 'country_code',
            'admin_level_1', 'admin_level_2', 'admin_level_3',
            'historical_admin_1', 'historical_admin_2', 'historical_region'
        ]


class CatalogUnitPlaceSerializer(serializers.ModelSerializer):
    """Serializer for place links."""
    place = PlaceSerializer(read_only=True)

    class Meta:
        model = CatalogUnitPlace
        fields = ['place', 'place_role', 'role_note']


class CatalogUnitListSerializer(serializers.ModelSerializer):
    """Compact serializer for catalog unit listings."""
    repository_name = serializers.CharField(source='repository.abbreviation', read_only=True)
    reference_code = serializers.ReadOnlyField()
    has_children = serializers.SerializerMethodField()
    child_count = serializers.SerializerMethodField()

    class Meta:
        model = CatalogUnit
        fields = [
            'id', 'repository_name', 'reference_code', 'local_identifier',
            'title', 'level_type', 'date_expression',
            'has_children', 'child_count', 'has_digital_files'
        ]

    def get_has_children(self, obj):
        return obj.get_descendant_count() > 0

    def get_child_count(self, obj):
        return obj.get_children().count()


class CatalogUnitDetailSerializer(serializers.ModelSerializer):
    """Full serializer for catalog unit detail view."""
    repository = RepositoryListSerializer(read_only=True)
    reference_code = serializers.ReadOnlyField()
    breadcrumb = serializers.SerializerMethodField()
    children = serializers.SerializerMethodField()
    places = serializers.SerializerMethodField()

    class Meta:
        model = CatalogUnit
        fields = '__all__'

    def get_breadcrumb(self, obj):
        """Return ancestor chain for navigation."""
        ancestors = obj.get_ancestors(include_self=False)
        return [
            {
                'id': a.id,
                'title': a.title[:80] + '...' if len(a.title) > 80 else a.title,
                'level_type': a.level_type
            }
            for a in ancestors
        ]

    def get_children(self, obj):
        """Return direct children."""
        children = obj.get_children()[:50]
        return CatalogUnitListSerializer(children, many=True).data

    def get_places(self, obj):
        """Return linked places."""
        links = obj.place_links.select_related('place').all()
        return CatalogUnitPlaceSerializer(links, many=True).data


class CatalogUnitTreeSerializer(serializers.ModelSerializer):
    """Serializer for tree/hierarchy view."""
    children = serializers.SerializerMethodField()
    reference_code = serializers.ReadOnlyField()

    class Meta:
        model = CatalogUnit
        fields = ['id', 'title', 'reference_code', 'level_type', 'children']

    def get_children(self, obj):
        """Recursively serialize children (with depth limit)."""
        depth = self.context.get('depth', 2)
        if depth <= 0:
            return []
        children = obj.get_children()
        return CatalogUnitTreeSerializer(
            children, many=True,
            context={'depth': depth - 1}
        ).data


class SearchResultSerializer(serializers.ModelSerializer):
    """Serializer for search results."""
    repository_name = serializers.CharField(source='repository.abbreviation', read_only=True)
    reference_code = serializers.ReadOnlyField()
    breadcrumb_text = serializers.SerializerMethodField()

    class Meta:
        model = CatalogUnit
        fields = [
            'id', 'repository_name', 'reference_code', 'local_identifier',
            'title', 'level_type', 'date_expression', 'description',
            'breadcrumb_text'
        ]

    def get_breadcrumb_text(self, obj):
        """Return breadcrumb as text string."""
        ancestors = obj.get_ancestors(include_self=False)
        if not ancestors:
            return obj.repository.abbreviation or obj.repository.name
        parts = [obj.repository.abbreviation or obj.repository.name]
        for a in ancestors[:3]:
            parts.append(a.title[:30])
        return ' > '.join(parts)
