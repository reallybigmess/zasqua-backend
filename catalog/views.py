"""
REST API Views for Fisqua Catalog.
"""

from django.db.models import Q
from rest_framework import viewsets, filters
from rest_framework.decorators import action
from rest_framework.response import Response

from .models import Repository, CatalogUnit, Place
from .serializers import (
    RepositoryListSerializer, RepositoryDetailSerializer,
    CatalogUnitListSerializer, CatalogUnitDetailSerializer,
    CatalogUnitTreeSerializer, PlaceSerializer, SearchResultSerializer
)


class RepositoryViewSet(viewsets.ReadOnlyModelViewSet):
    """
    API endpoint for repositories.

    list: Get all repositories
    retrieve: Get a single repository with its root catalog units
    """
    queryset = Repository.objects.filter(enabled=True).order_by('name')
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['name', 'abbreviation', 'city', 'region']
    ordering_fields = ['name', 'created_at']

    def get_serializer_class(self):
        if self.action == 'retrieve':
            return RepositoryDetailSerializer
        return RepositoryListSerializer


class CatalogUnitViewSet(viewsets.ReadOnlyModelViewSet):
    """
    API endpoint for catalog units.

    list: Get catalog units (filterable by repository, level, etc.)
    retrieve: Get a single catalog unit with its children and places
    tree: Get hierarchical tree structure
    search: Full-text search across catalog units
    """
    queryset = CatalogUnit.objects.select_related('repository').order_by('tree_id', 'lft')
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['title', 'description', 'local_identifier', 'creator_string']
    ordering_fields = ['title', 'date_start', 'created_at']

    def get_serializer_class(self):
        if self.action == 'retrieve':
            return CatalogUnitDetailSerializer
        if self.action == 'tree':
            return CatalogUnitTreeSerializer
        if self.action == 'search':
            return SearchResultSerializer
        return CatalogUnitListSerializer

    def get_queryset(self):
        queryset = super().get_queryset()

        # Filter by repository
        repository_id = self.request.query_params.get('repository')
        if repository_id:
            queryset = queryset.filter(repository_id=repository_id)

        # Filter by level type
        level_type = self.request.query_params.get('level_type')
        if level_type:
            queryset = queryset.filter(level_type=level_type)

        # Filter by parent (for browsing hierarchy)
        parent_id = self.request.query_params.get('parent')
        if parent_id:
            if parent_id == 'null' or parent_id == 'root':
                queryset = queryset.filter(parent__isnull=True)
            else:
                queryset = queryset.filter(parent_id=parent_id)

        # Filter to only root units
        root_only = self.request.query_params.get('root_only')
        if root_only and root_only.lower() in ('true', '1', 'yes'):
            queryset = queryset.filter(parent__isnull=True)

        return queryset

    @action(detail=False, methods=['get'])
    def search(self, request):
        """
        Full-text search across catalog units.

        Query params:
            q: Search query
            repository: Filter by repository ID
            level_type: Filter by level type
            date_from: Filter by date range start (YYYY)
            date_to: Filter by date range end (YYYY)
        """
        query = request.query_params.get('q', '').strip()
        if not query:
            return Response({'results': [], 'count': 0})

        queryset = self.get_queryset()

        # Basic text search
        queryset = queryset.filter(
            Q(title__icontains=query) |
            Q(description__icontains=query) |
            Q(local_identifier__icontains=query) |
            Q(creator_string__icontains=query) |
            Q(subjects_topic__contains=[query])
        )

        # Date range filters
        date_from = request.query_params.get('date_from')
        date_to = request.query_params.get('date_to')
        if date_from:
            try:
                year = int(date_from)
                queryset = queryset.filter(
                    Q(date_end__year__gte=year) | Q(date_end__isnull=True, date_start__year__gte=year)
                )
            except ValueError:
                pass
        if date_to:
            try:
                year = int(date_to)
                queryset = queryset.filter(
                    Q(date_start__year__lte=year) | Q(date_start__isnull=True)
                )
            except ValueError:
                pass

        # Paginate
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = SearchResultSerializer(page, many=True)
            return self.get_paginated_response(serializer.data)

        serializer = SearchResultSerializer(queryset[:100], many=True)
        return Response({'results': serializer.data, 'count': queryset.count()})

    @action(detail=True, methods=['get'])
    def tree(self, request, pk=None):
        """
        Get hierarchical tree starting from this unit.

        Query params:
            depth: How many levels to include (default 2, max 5)
        """
        unit = self.get_object()
        depth = min(int(request.query_params.get('depth', 2)), 5)
        serializer = CatalogUnitTreeSerializer(unit, context={'depth': depth})
        return Response(serializer.data)

    @action(detail=True, methods=['get'])
    def children(self, request, pk=None):
        """Get direct children of this unit."""
        unit = self.get_object()
        children = unit.get_children()
        page = self.paginate_queryset(children)
        if page is not None:
            serializer = CatalogUnitListSerializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        serializer = CatalogUnitListSerializer(children, many=True)
        return Response(serializer.data)

    @action(detail=True, methods=['get'])
    def ancestors(self, request, pk=None):
        """Get all ancestors (breadcrumb) of this unit."""
        unit = self.get_object()
        ancestors = unit.get_ancestors(include_self=False)
        serializer = CatalogUnitListSerializer(ancestors, many=True)
        return Response(serializer.data)


class PlaceViewSet(viewsets.ReadOnlyModelViewSet):
    """
    API endpoint for places (gazetteer).

    list: Get all places
    retrieve: Get a single place with linked catalog units
    """
    queryset = Place.objects.filter(is_active=True).order_by('label')
    serializer_class = PlaceSerializer
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['label', 'historical_name']
    ordering_fields = ['label', 'created_at']

    @action(detail=True, methods=['get'])
    def catalog_units(self, request, pk=None):
        """Get catalog units linked to this place."""
        place = self.get_object()
        links = place.catalog_unit_links.select_related('catalog_unit', 'catalog_unit__repository')
        units = [link.catalog_unit for link in links]
        serializer = CatalogUnitListSerializer(units, many=True)
        return Response(serializer.data)
