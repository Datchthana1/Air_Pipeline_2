import re

from function.reload_params import reload_params, RELOAD_CONF, EARTHQUAKE_RELOAD_CONF


def test_reload_params_keys():
    assert set(reload_params().keys()) == {'reload_mode', 'reload_date', 'date_from', 'date_to', 'station_id'}
    assert 'station_id' not in reload_params(include_station_id=False)


def test_reload_mode_enum_and_default():
    params = reload_params()
    assert params['reload_mode'].schema['enum'] == ['latest', 'day', 'range', 'full']
    assert params['reload_mode'].value == 'latest'
    assert reload_params(default_mode='full')['reload_mode'].value == 'full'


def test_earthquake_conf_is_reload_conf_minus_station():
    assert EARTHQUAKE_RELOAD_CONF == {k: v for k, v in RELOAD_CONF.items() if k != 'station_id'}
    assert 'station_id' not in EARTHQUAKE_RELOAD_CONF


def test_reload_conf_templates_prefer_conf():
    pattern = re.compile(r"\{\{ dag_run\.conf\.get\('(\w+)'\)\s+or params\.\1 \}\}")
    for key, template in RELOAD_CONF.items():
        m = pattern.fullmatch(template)
        assert m is not None, template
        assert m.group(1) == key
