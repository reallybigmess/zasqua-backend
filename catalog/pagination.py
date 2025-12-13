"""
Custom pagination classes for Zasqua API.
"""

from rest_framework.pagination import PageNumberPagination


class FlexiblePageNumberPagination(PageNumberPagination):
    """
    Pagination class that allows clients to set page_size via query param.

    Usage: ?page_size=1000&page=2
    Default: 50 items per page
    Maximum: 1000 items per page
    """
    page_size = 50
    page_size_query_param = 'page_size'
    max_page_size = 1000
