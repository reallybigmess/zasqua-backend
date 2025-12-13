# Zasqua Backend

Django REST API backend for the Zasqua archival platform.

## Overview

This backend provides:
- REST API for archival descriptions and entities
- Meilisearch integration for full-text search
- MPTT-based hierarchical data model
- Management commands for data import

## Requirements

- Python 3.11+
- MySQL 8.0+
- Meilisearch 1.0+

## Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure database
cp .env.example .env
# Edit .env with your database credentials

# Run migrations
python manage.py migrate

# Start development server
python manage.py runserver
```

## Database Models

| Model | Description |
|-------|-------------|
| Repository | Archive institutions (e.g., CO.AHR, PE-BN) |
| Description | Archival descriptions with MPTT hierarchy |
| Entity | People, organizations, families |
| Place | Geographic locations |
| DescriptionEntity | Links descriptions to entities |
| DescriptionPlace | Links descriptions to places |

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `/api/repositories/` | List all repositories |
| `/api/repositories/{code}/` | Repository detail |
| `/api/descriptions/` | List descriptions with filtering |
| `/api/descriptions/{id}/` | Description detail |
| `/api/descriptions/{id}/children/` | Child descriptions |
| `/api/entities/` | List entities |
| `/api/places/` | List places |
| `/api/search/` | Meilisearch search endpoint |

## Management Commands

### Data Import
```bash
# Import AHR hierarchy from CSVs
python manage.py import_ahr_hierarchy

# With dry run
python manage.py import_ahr_hierarchy --dry-run
```

### Search Index
```bash
# Rebuild Meilisearch index
python manage.py rebuild_search_index --clear
```

### Database Utilities
```bash
# Check counts by repository
python manage.py shell -c "
from catalog.models import Description, Repository
for r in Repository.objects.all():
    print(f'{r.code}: {Description.objects.filter(repository=r).count()}')"
```

## Development

### Running Tests
```bash
python manage.py test
```

### Code Style
```bash
# Format code
black .

# Check linting
flake8
```

## Data Summary

| Repository | Records | Entities |
|------------|---------|----------|
| CO.AHR (Rionegro) | 55,353 | 88,646 links |
| CO.AHRB (Boyaca) | 7,669 | - |
| CIHJML (Popayan) | 26,000+ | - |
| PE-BN (Peru) | 14,000+ | - |
| AHJCI (Istmina) | 800+ | - |

Total: ~104,000 descriptions, ~75,000 entities
