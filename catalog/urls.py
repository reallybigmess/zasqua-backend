"""
URL configuration for Fisqua Catalog API.
"""

from django.urls import path, include
from rest_framework.routers import DefaultRouter

from .views import RepositoryViewSet, CatalogUnitViewSet, PlaceViewSet

router = DefaultRouter()
router.register(r'repositories', RepositoryViewSet, basename='repository')
router.register(r'catalog-units', CatalogUnitViewSet, basename='catalogunit')
router.register(r'places', PlaceViewSet, basename='place')

urlpatterns = [
    path('', include(router.urls)),
]
