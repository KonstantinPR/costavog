import pandas as pd
import numpy as np
import pytest
from app.modules.sales_funnel_module import calculate_discount

@pytest.fixture
def sample_data():
    data = {
        'price': [10, 20, 30],
        'discount': [5, 10, 15],
        'net_cost': [8, 15, 22],
        'smooth_days': [2, 3, 4],
        'buyoutsCount': [100, 200, 300],
        'quantityFull': [50, 0, np.nan]  # Sample data for NaN case
    }
    return pd.DataFrame(data)

def test_calculate_discount_returns_dataframe(sample_data):
    result = calculate_discount(sample_data)
    assert isinstance(result, pd.DataFrame)

def test_calculate_discount_columns(sample_data):
    result = calculate_discount(sample_data)
    expected_columns = [
        'price', 'discount', 'net_cost', 'smooth_days', 'buyoutsCount', 'quantityFull',
        'price_disc', 'func_discount', 'k1', 'k2', 'k_sell/stock', 'func_delta',
        'k_net/price', '1-k_net/price', 'price_recommended', 'disc_recommended'
    ]
    assert all(column in result.columns for column in expected_columns)