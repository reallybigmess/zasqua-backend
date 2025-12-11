"""
Management command to rebuild the Meilisearch index.

Usage:
    python manage.py rebuild_search_index          # Full rebuild
    python manage.py rebuild_search_index --clear  # Clear and rebuild
    python manage.py rebuild_search_index --stats  # Show stats only
"""

import time
from django.core.management.base import BaseCommand
from catalog.models import Description
from catalog import search


class Command(BaseCommand):
    help = 'Rebuild the Meilisearch search index for descriptions'

    def add_arguments(self, parser):
        parser.add_argument(
            '--clear',
            action='store_true',
            help='Clear the index before rebuilding',
        )
        parser.add_argument(
            '--stats',
            action='store_true',
            help='Show index statistics only',
        )
        parser.add_argument(
            '--configure',
            action='store_true',
            help='Configure index settings only (no indexing)',
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=1000,
            help='Batch size for indexing (default: 1000)',
        )
        parser.add_argument(
            '--repository',
            type=str,
            help='Only index descriptions from this repository code',
        )

    def handle(self, *args, **options):
        if options['stats']:
            self.show_stats()
            return

        if options['configure']:
            self.configure_index()
            return

        if options['clear']:
            self.clear_index()

        self.configure_index()
        self.index_all(
            batch_size=options['batch_size'],
            repository=options.get('repository')
        )

    def show_stats(self):
        """Display current index statistics."""
        try:
            stats = search.get_stats()
            self.stdout.write(f"Index: {search.get_index().uid}")
            self.stdout.write(f"Documents: {stats.get('numberOfDocuments', 0):,}")
            self.stdout.write(f"Indexing: {stats.get('isIndexing', False)}")
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"Error getting stats: {e}"))

    def configure_index(self):
        """Configure index settings."""
        self.stdout.write("Configuring index settings...")
        try:
            search.configure_index()
            self.stdout.write(self.style.SUCCESS("Index configured"))
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"Error configuring index: {e}"))

    def clear_index(self):
        """Clear all documents from index."""
        self.stdout.write("Clearing index...")
        try:
            index = search.get_index()
            task = index.delete_all_documents()
            # Wait for task to complete
            client = search.get_client()
            client.wait_for_task(task.task_uid, timeout_in_ms=60000)
            self.stdout.write(self.style.SUCCESS("Index cleared"))
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"Error clearing index: {e}"))

    def index_all(self, batch_size=1000, repository=None):
        """Index all descriptions."""
        queryset = Description.objects.filter(is_published=True)

        if repository:
            queryset = queryset.filter(repository__code=repository)
            self.stdout.write(f"Filtering to repository: {repository}")

        total = queryset.count()
        self.stdout.write(f"Indexing {total:,} descriptions...")

        start_time = time.time()
        indexed, errors = search.index_descriptions(queryset, batch_size=batch_size)
        elapsed = time.time() - start_time

        rate = indexed / elapsed if elapsed > 0 else 0
        self.stdout.write(self.style.SUCCESS(
            f"Indexed {indexed:,} documents in {elapsed:.1f}s ({rate:.0f}/sec)"
        ))

        if errors:
            self.stderr.write(self.style.WARNING(f"Errors: {len(errors)}"))
            for desc_id, error in errors[:10]:
                self.stderr.write(f"  Description {desc_id}: {error}")
            if len(errors) > 10:
                self.stderr.write(f"  ... and {len(errors) - 10} more")
