"""Configuration pytest pour les tests Spark."""

import pytest
import os

# Vérifie si nous sommes dans un environnement CI (GitHub Actions)
IN_CI = os.environ.get('CI', 'false').lower() == 'true'

# Vérifie si pyspark est disponible
try:
    import pyspark
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

# Si nous sommes en CI et pyspark n'est pas disponible, désactive les tests Spark
def pytest_configure(config):
    """Configure pytest pour ignorer les tests Spark si non disponible."""
    if IN_CI and not PYSPARK_AVAILABLE:
        # Désactive tous les tests dans test_spark_pipeline.py et test_pipeline.py
        config.addinivalue_line("markers", "skip_spark: skip spark tests in CI")


def pytest_collection_modifyitems(items):
    """Modifie la collection de tests pour ignorer ceux qui nécessitent Spark."""
    for item in items:
        # Si le test est dans test_spark_pipeline.py ou test_pipeline.py
        # et que nous sommes en CI sans pyspark, ignore-le
        if IN_CI and not PYSPARK_AVAILABLE:
            if item.module.__name__.startswith('test_spark_pipeline') or \
               item.module.__name__.startswith('test_pipeline'):
                item.add_marker(pytest.mark.skip(reason="Spark tests skipped in CI without pyspark"))
