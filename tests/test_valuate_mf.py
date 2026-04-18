"""
test_valuate_mf.py â€” Tests for pipeline/valuate_mf_nps.py
===========================================================
Tests live NAV fetching math using mocked AMFI API responses.
conftest.py ensures the project root is on sys.path.
"""
import pandas as pd
from unittest.mock import patch

from valuation.valuate_mf_nps import fetch_live_valuations


@patch('valuation.valuate_mf_nps.Path.exists', return_value=True)
@patch('valuation.valuate_mf_nps.pd.read_csv')
@patch('valuation.valuate_mf_nps.Mftool')
def test_live_valuation_math(mock_mftool_cls, mock_read_csv, mock_exists):
    """Proves the valuation engine multiplies units by NAV correctly."""
    mock_read_csv.return_value = pd.DataFrame({
        'Portfolio Owner': ['Komal', 'Pankaj'],
        'Ticker': ['120828', '149441'],
        'Asset Name': ['Quant Small Cap', 'ICICI FoF'],
        'Asset Class': ['Mutual Fund', 'Mutual Fund'],
        'Units': [10.0, 50.0],
    })

    def fake_amfi_response(amfi):
        if amfi == '120828':
            return {'nav': '250.0', 'scheme_name': 'Quant Small Cap'}
        if amfi == '149441':
            return {'nav': '10.0', 'scheme_name': 'ICICI FoF'}
        return None

    mock_mftool_cls.return_value.get_scheme_quote.side_effect = fake_amfi_response

    df_val, totals = fetch_live_valuations('data/output/mf_active_holdings.csv')

    komal_val = df_val[df_val['Portfolio Owner'] == 'Komal']['Current Value'].iloc[0]
    pankaj_val = df_val[df_val['Portfolio Owner'] == 'Pankaj']['Current Value'].iloc[0]

    assert komal_val == 2500.0, f"Math failed! Expected 2500, got {komal_val}"
    assert pankaj_val == 500.0, f"Math failed! Expected 500, got {pankaj_val}"
    assert totals['Komal'] == 2500.0
    assert totals['Pankaj'] == 500.0

