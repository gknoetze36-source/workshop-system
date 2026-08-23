import importlib.util
from pathlib import Path


def test_database_authority_is_a_package():
    spec = importlib.util.find_spec("database")
    assert spec is not None
    assert spec.submodule_search_locations is not None

    package_dir = Path(next(iter(spec.submodule_search_locations)))
    assert (package_dir / "__init__.py").exists()


def test_database_public_api_is_exposed():
    import database

    required = {
        "get_connection",
        "query_db",
        "execute_db",
        "get_session",
        "initialize_database",
    }
    assert required.issubset(set(database.__all__))
