import math

import pytest

from function import ingest
from function.ingest import calculate_aqi_pm25, _PM25_BREAKPOINTS


@pytest.mark.parametrize('pm25, expected', [
    (None, None),
    (0, 0),
    (-5, 0),
    (7.5, 12),          # half-to-even: 12.5 -> 12
    (12.3, 21),
    ('12.3', 21),       # string input is cast with float()
    (15.0, 25),
    (15.04, 25),
    (15.05, 26),        # rounds to 15.1 first
    (15.1, 26),
    (25.0, 50),
    (25.05, 51),
    (30, 70),
    (37.5, 100),
    (37.55, 100),
    (37.6, 101),
    (75, 200),
    (150, 300),
    (250, 400),
    (250.05, 401),
    (500, 500),
    (500.04, 500),
    (500.05, 500),
    (600, 500),
    (float('nan'), None),   # FIX-2
    (float('inf'), None),   # FIX-2
], ids=[
    'none', 'zero', 'negative', 'half-even-7.5', '12.3', 'string-12.3', 'bp1-lo', 'bp1-round-down',
    'bp1-round-up', 'bp2-lo', 'bp2-hi', 'bp3-lo', 'int-30', 'bp3-hi', 'bp3-round-down', 'bp4-lo',
    'bp5-lo', 'bp6-lo', 'bp7-lo', 'bp7-round-up', 'max', 'above-max-round-down', 'above-max-round-up',
    'above-max', 'nan', 'inf',
])
def test_calculate_aqi_pm25_values(pm25, expected):
    assert calculate_aqi_pm25(pm25) == expected


def test_calculate_aqi_pm25_non_numeric_raises():
    with pytest.raises(ValueError):
        calculate_aqi_pm25('abc')


def test_calculate_aqi_pm25_nan_is_not_zero():
    # FIX-2 regression guard: NaN used to fall through to the 0 branch.
    assert calculate_aqi_pm25(float('nan')) is None
    assert not math.isnan(calculate_aqi_pm25(10.0))


def test_aqi_breakpoints_contiguous():
    for prev, nxt in zip(_PM25_BREAKPOINTS, _PM25_BREAKPOINTS[1:]):
        assert nxt[0] == round(prev[1] + 0.1, 1)
        assert nxt[2] == prev[3] + 1
    assert _PM25_BREAKPOINTS[-1][3] == 500


def test_breakpoints_module_is_ingest():
    assert ingest.calculate_aqi_pm25 is calculate_aqi_pm25
