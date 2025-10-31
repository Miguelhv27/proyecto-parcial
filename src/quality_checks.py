import logging
import pandas as pd

def check_no_negative_prices(df_products):
    logging.info("Check: no negative prices")
    if 'price' not in df_products.columns:
        raise AssertionError("Column 'price' missing in products")
    negatives = df_products[df_products['price'] < 0]
    if not negatives.empty:
        raise AssertionError(f"Negative prices found: {len(negatives)} rows")

def check_stock_integer_positive(inventory_df):
    logging.info("Check: stock integer positive")
    if 'current_stock' not in inventory_df.columns:
        raise AssertionError("Column 'current_stock' missing in inventory")
    series = inventory_df['current_stock'].dropna()
    if not pd.api.types.is_integer_dtype(series):
        if not all(series.apply(lambda x: float(x).is_integer())):
            raise AssertionError("current_stock must be integer-like")
    if (inventory_df['current_stock'].fillna(0) < 0).any():
        raise AssertionError("Negative current_stock values found")

def check_categories_exist(products_df, sales_df):
    logging.info("Check: categories exist")
    prod_cats = set(products_df['category'].dropna().unique())
    sale_cats = set()
    if 'category' in sales_df.columns:
        sale_cats = set(sales_df['category'].dropna().unique())
    missing = sale_cats - prod_cats
    if missing:
        raise AssertionError(f"Sales reference missing categories present in sales: {missing}")

def check_sale_dates_valid(sales_df, date_col='sale_date'):
    logging.info("Check: sale dates valid")
    if date_col not in sales_df.columns:
        raise AssertionError(f"Column '{date_col}' missing in sales")
    parsed = pd.to_datetime(sales_df[date_col], errors='coerce')
    if parsed.isna().any():
        raise AssertionError("Invalid sale_date values found")
