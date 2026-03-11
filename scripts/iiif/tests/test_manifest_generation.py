"""
Tests for generate_volume_manifest.py -- JSON-based volume manifest generation.
"""

import csv
import io
import json
import sys
from pathlib import Path
import pytest

# Add scripts/iiif to path for import
sys.path.insert(0, str(Path(__file__).parent.parent))

from generate_volume_manifest import (
    INSCOPE_FONDS,
    extract_volumes_from_inventory,
    filter_aht_exclusions,
    write_manifest_csv,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def make_inventory(entries):
    """Build a minimal inventory dict from a list of path strings."""
    return {
        "collection": "Copia seguridad AHRB",
        "images": [{"path": p, "size": 100, "sha256": "abc"} for p in entries],
    }


MINIMAL_INVENTORY = make_inventory([
    # In-scope: AHRB_AHT
    "Copia seguridad AHRB/AHRB_AHT/AHRB_AHT_003/proc/recortadas/AHRB_AHT_003-img_0001.jpg",
    "Copia seguridad AHRB/AHRB_AHT/AHRB_AHT_003/proc/recortadas/AHRB_AHT_003-img_0002.jpg",
    "Copia seguridad AHRB/AHRB_AHT/AHRB_AHT_005/proc/recortadas/AHRB_AHT_005-img_0001.jpg",
    # In-scope: AHRB_N1
    "Copia seguridad AHRB/AHRB_N1/AHRB_N1_001/proc/recortadas/AHRB_N1_001-img_0001.jpg",
    "Copia seguridad AHRB/AHRB_N1/AHRB_N1_024bis/proc/recortadas/AHRB_N1_024bis-img_0001.jpg",
    "Copia seguridad AHRB/AHRB_N1/AHRB_N1_024bis/proc/recortadas/AHRB_N1_024bis-img_0002.jpg",
    # In-scope: AHRB_Cabildos
    "Copia seguridad AHRB/AHRB_Cabildos/AHRB_Cabildos_unico/proc/recortadas/AHRB_Cabildos_unico-img_0001.jpg",
    # Out-of-scope: AHRB_E
    "Copia seguridad AHRB/AHRB_E/AHRB_E_001/proc/recortadas/AHRB_E_001-img_0001.jpg",
    # Out-of-scope: Catalogos_y_miscelaneo
    "Copia seguridad AHRB/Catalogos_y_miscelaneo/Vol_001/proc/recortadas/Vol_001-img_0001.jpg",
    # Non-recortadas path (should be excluded from image count)
    "Copia seguridad AHRB/AHRB_AHT/AHRB_AHT_003/proc/otras/AHRB_AHT_003-img_0001.jpg",
])


# ---------------------------------------------------------------------------
# Test: fond filtering
# ---------------------------------------------------------------------------

class TestFondFiltering:
    def test_only_inscope_fonds_returned(self):
        """Parsing an inventory returns only in-scope fond rows."""
        volumes = extract_volumes_from_inventory(MINIMAL_INVENTORY)
        fonds = {v["fond"] for v in volumes}
        assert "AHRB_E" not in fonds
        assert "Catalogos_y_miscelaneo" not in fonds

    def test_inscope_fonds_present(self):
        """All in-scope fonds from the minimal inventory are present."""
        volumes = extract_volumes_from_inventory(MINIMAL_INVENTORY)
        fonds = {v["fond"] for v in volumes}
        assert "AHRB_AHT" in fonds
        assert "AHRB_N1" in fonds
        assert "AHRB_Cabildos" in fonds

    def test_custom_fonds_filter(self):
        """Passing a custom fonds list restricts output to those fonds."""
        volumes = extract_volumes_from_inventory(
            MINIMAL_INVENTORY, fonds=["AHRB_N1"]
        )
        fonds = {v["fond"] for v in volumes}
        assert fonds == {"AHRB_N1"}


# ---------------------------------------------------------------------------
# Test: volume directory extraction
# ---------------------------------------------------------------------------

class TestVolumeExtraction:
    def test_volume_count_per_fond(self):
        """Correct number of volumes extracted per fond."""
        volumes = extract_volumes_from_inventory(MINIMAL_INVENTORY)
        by_fond = {}
        for v in volumes:
            by_fond.setdefault(v["fond"], []).append(v)
        assert len(by_fond["AHRB_AHT"]) == 2  # 003, 005
        assert len(by_fond["AHRB_N1"]) == 2   # 001, 024bis
        assert len(by_fond["AHRB_Cabildos"]) == 1  # unico

    def test_image_dir_format(self):
        """image_dir is fond-relative, ending in proc/recortadas."""
        volumes = extract_volumes_from_inventory(MINIMAL_INVENTORY)
        aht_vols = {v["volume"]: v for v in volumes if v["fond"] == "AHRB_AHT"}
        assert aht_vols["003"]["image_dir"] == "AHRB_AHT/AHRB_AHT_003/proc/recortadas"

    def test_non_recortadas_paths_excluded_from_count(self):
        """Images not under proc/recortadas are not counted."""
        volumes = extract_volumes_from_inventory(MINIMAL_INVENTORY)
        aht_vols = {v["volume"]: v for v in volumes if v["fond"] == "AHRB_AHT"}
        # AHRB_AHT_003 has 2 recortadas + 1 otras — only 2 should be counted
        assert aht_vols["003"]["image_count"] == 2


# ---------------------------------------------------------------------------
# Test: non-numeric volume suffixes
# ---------------------------------------------------------------------------

class TestNonNumericVolumes:
    def test_cabildos_unico_volume(self):
        """AHRB_Cabildos_unico extracts volume='unico'."""
        volumes = extract_volumes_from_inventory(MINIMAL_INVENTORY)
        cab_vols = {v["volume"]: v for v in volumes if v["fond"] == "AHRB_Cabildos"}
        assert "unico" in cab_vols

    def test_cabildos_unico_image_dir(self):
        """AHRB_Cabildos_unico has correct image_dir."""
        volumes = extract_volumes_from_inventory(MINIMAL_INVENTORY)
        cab_vols = {v["volume"]: v for v in volumes if v["fond"] == "AHRB_Cabildos"}
        assert cab_vols["unico"]["image_dir"] == "AHRB_Cabildos/AHRB_Cabildos_unico/proc/recortadas"

    def test_n1_024bis_volume(self):
        """AHRB_N1_024bis extracts volume='024bis'."""
        volumes = extract_volumes_from_inventory(MINIMAL_INVENTORY)
        n1_vols = {v["volume"]: v for v in volumes if v["fond"] == "AHRB_N1"}
        assert "024bis" in n1_vols


# ---------------------------------------------------------------------------
# Test: image count column
# ---------------------------------------------------------------------------

class TestImageCount:
    def test_image_count_populated(self):
        """image_count is populated for all volumes."""
        volumes = extract_volumes_from_inventory(MINIMAL_INVENTORY)
        for v in volumes:
            assert "image_count" in v
            assert v["image_count"] > 0

    def test_image_count_correct(self):
        """image_count matches actual recortadas image count per volume."""
        volumes = extract_volumes_from_inventory(MINIMAL_INVENTORY)
        by_key = {(v["fond"], v["volume"]): v for v in volumes}
        assert by_key[("AHRB_AHT", "003")]["image_count"] == 2
        assert by_key[("AHRB_AHT", "005")]["image_count"] == 1
        assert by_key[("AHRB_N1", "024bis")]["image_count"] == 2
        assert by_key[("AHRB_Cabildos", "unico")]["image_count"] == 1


# ---------------------------------------------------------------------------
# Test: CSV output format
# ---------------------------------------------------------------------------

class TestCSVOutput:
    def test_csv_column_order(self):
        """CSV output has columns fond,volume,image_dir,image_count in order."""
        volumes = extract_volumes_from_inventory(MINIMAL_INVENTORY)
        buf = io.StringIO()
        write_manifest_csv(volumes, buf)
        buf.seek(0)
        reader = csv.reader(buf)
        headers = next(reader)
        assert headers == ["fond", "volume", "image_dir", "image_count"]

    def test_csv_rows_match_volumes(self):
        """CSV output has one row per extracted volume."""
        volumes = extract_volumes_from_inventory(MINIMAL_INVENTORY)
        buf = io.StringIO()
        write_manifest_csv(volumes, buf)
        buf.seek(0)
        reader = csv.DictReader(buf)
        rows = list(reader)
        assert len(rows) == len(volumes)


# ---------------------------------------------------------------------------
# Test: AHT exclusion
# ---------------------------------------------------------------------------

class TestAHTExclusion:
    def test_exclusion_filters_aht_volumes_new_format(self):
        """filter_aht_exclusions removes AHT volumes using new co- slug format."""
        volumes = [
            {"fond": "AHRB_AHT", "volume": "003", "image_dir": "x", "image_count": 1},
            {"fond": "AHRB_AHT", "volume": "005", "image_dir": "x", "image_count": 1},
            {"fond": "AHRB_N1",  "volume": "001", "image_dir": "x", "image_count": 1},
        ]
        # Simulate R2 dirs containing tiles for AHT 003 (new format with co-)
        r2_dirs = ["co-ahrb-aht-003-0001/", "co-ahrb-aht-003-0002/", "co-ahrb-n1-001-0001/"]
        result, excluded = filter_aht_exclusions(volumes, r2_dirs)
        remaining_aht = [v for v in result if v["fond"] == "AHRB_AHT"]
        assert len(remaining_aht) == 1
        assert remaining_aht[0]["volume"] == "005"
        assert "003" in excluded

    def test_exclusion_filters_aht_volumes_old_format(self):
        """filter_aht_exclusions removes AHT volumes using old ahrb-aht- slug format."""
        volumes = [
            {"fond": "AHRB_AHT", "volume": "003", "image_dir": "x", "image_count": 1},
            {"fond": "AHRB_AHT", "volume": "005", "image_dir": "x", "image_count": 1},
        ]
        # Old format (v0.2.0): no co- prefix, d-prefixed document number
        r2_dirs = ["ahrb-aht-003-d001/", "ahrb-aht-003-d002/"]
        result, excluded = filter_aht_exclusions(volumes, r2_dirs)
        remaining_aht = [v for v in result if v["fond"] == "AHRB_AHT"]
        assert len(remaining_aht) == 1
        assert remaining_aht[0]["volume"] == "005"
        assert "003" in excluded

    def test_exclusion_does_not_affect_non_aht(self):
        """filter_aht_exclusions does not remove non-AHT volumes."""
        volumes = [
            {"fond": "AHRB_N1", "volume": "001", "image_dir": "x", "image_count": 1},
            {"fond": "AHRB_AHT", "volume": "001", "image_dir": "x", "image_count": 1},
        ]
        r2_dirs = ["co-ahrb-aht-001-0001/"]
        result, excluded = filter_aht_exclusions(volumes, r2_dirs)
        non_aht = [v for v in result if v["fond"] == "AHRB_N1"]
        assert len(non_aht) == 1

    def test_no_tiled_dirs_means_no_exclusions(self):
        """filter_aht_exclusions with empty R2 dirs excludes nothing."""
        volumes = [
            {"fond": "AHRB_AHT", "volume": "003", "image_dir": "x", "image_count": 1},
        ]
        result, excluded = filter_aht_exclusions(volumes, [])
        assert len(result) == 1
        assert len(excluded) == 0
