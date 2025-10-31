import pandas as pd
import pytest
from src.quality_checks import (
    check_no_negative_prices,
    check_stock_integer_positive,
    check_categories_exist,
    check_sale_dates_valid
)

def create_products_df():
    return pd.DataFrame({
        'product_id': [1,2,3],
        'title': ['A','B','C'],
        'price': [10.0, 20.0, 15.0],
        'category': ['cat1','cat2','cat3']
    })

def create_sales_df():
    return pd.DataFrame({
        'sale_id': [100,101],
        'product_id': [1,2],
        'quantity': [2,1],
        'sale_date': ['2025-01-01', '2025-01-02']
    })

def create_inventory_df():
    return pd.DataFrame({
        'product_id': [1,2,3],
        'current_stock': [5, 0, 10],
        'min_stock': [2, 1, 5]
    })

def test_no_negative_prices_pass():
    df = create_products_df()
    check_no_negative_prices(df)

def test_no_negative_prices_fail():
    df = create_products_df()
    df.loc[0, 'price'] = -5
    with pytest.raises(AssertionError):
        check_no_negative_prices(df)

def test_stock_integer_positive_pass():
    inv = create_inventory_df()
    check_stock_integer_positive(inv)

def test_stock_integer_positive_fail():
    inv = create_inventory_df()
    inv.loc[0, 'current_stock'] = -1
    with pytest.raises(AssertionError):
        check_stock_integer_positive(inv)

def test_categories_exist_pass():
    products = create_products_df()
    sales = create_sales_df().merge(products[['product_id','category']], on='product_id', how='left')
    check_categories_exist(products, sales)

def test_sale_dates_valid_pass():
    sales = create_sales_df()
    check_sale_dates_valid(sales, 'sale_date')

def test_sale_dates_valid_fail():
    sales = create_sales_df()
    sales.loc[0,'sale_date'] = 'not-a-date'
    with pytest.raises(AssertionError):
        check_sale_dates_valid(sales, 'sale_date')
