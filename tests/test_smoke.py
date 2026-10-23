import sys


def test_dags_dir_on_path():
    assert any(p.endswith('dags') for p in sys.path)
