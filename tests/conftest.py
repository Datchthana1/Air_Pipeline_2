"""Test bootstrap.

Puts dags/ on sys.path so `import function.*` works like it does inside
Airflow, and stubs the `airflow` package when it is not installed (the
local test environment has no Airflow). Also blocks all network access so
no test can reach AIR4Thai, OpenWeather, TMD or Supabase by accident.
"""
import os
import sys
import types

import pytest
import requests

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DAGS_DIR = os.path.join(REPO_ROOT, 'dags')
if DAGS_DIR not in sys.path:
    sys.path.insert(0, DAGS_DIR)


def _install_airflow_stub() -> None:
    try:
        import airflow  # noqa: F401
        return
    except ImportError:
        pass

    airflow_mod = types.ModuleType('airflow')
    exceptions_mod = types.ModuleType('airflow.exceptions')
    models_mod = types.ModuleType('airflow.models')
    param_mod = types.ModuleType('airflow.models.param')

    class AirflowException(Exception):
        pass

    class Param:
        def __init__(self, default=None, **kwargs):
            self.value = default
            self.schema = kwargs

    exceptions_mod.AirflowException = AirflowException
    param_mod.Param = Param
    models_mod.param = param_mod
    airflow_mod.exceptions = exceptions_mod
    airflow_mod.models = models_mod

    sys.modules['airflow'] = airflow_mod
    sys.modules['airflow.exceptions'] = exceptions_mod
    sys.modules['airflow.models'] = models_mod
    sys.modules['airflow.models.param'] = param_mod


_install_airflow_stub()


@pytest.fixture(autouse=True)
def _block_network(monkeypatch):
    def _no_network(*args, **kwargs):
        raise RuntimeError('Network access is disabled in tests - mock requests.get')
    # requests.get goes through Session.request, so this covers every HTTP call.
    monkeypatch.setattr(requests.sessions.Session, 'request', _no_network)


@pytest.fixture
def fake_client(monkeypatch):
    """Replace the Supabase client for both ingest modules with a MagicMock."""
    from unittest.mock import MagicMock
    import function.ingest as ingest
    import function.ingest_earthquake as eq
    client = MagicMock(name='supabase_client')
    monkeypatch.setattr(ingest, 'get_client', lambda: client)
    monkeypatch.setattr(eq, 'get_client', lambda: client)
    return client
