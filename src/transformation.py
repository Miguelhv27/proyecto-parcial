import pandas as pd
import logging

def normalize_products(df_products):
    df = df_products.copy()
    if 'id' in df.columns:
        df = df.rename(columns={'id': 'product_id'})
    for col in ['product_id', 'title', 'price', 'category']:
        if col not in df.columns:
            df[col] = None
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    return df


def merge_data(products_df, sales_df, inventory_df):
    logging.info("Merging data: products + sales + inventory")
    products = normalize_products(products_df)

    sales = sales_df.copy()
    inventory = inventory_df.copy()

    if 'product_id' not in sales.columns:
        raise KeyError("sales.csv necesita columna 'product_id'")

    sales['product_id'] = sales['product_id'].astype(int)
    inventory['product_id'] = inventory['product_id'].astype(int)

    merged = sales.merge(products, on='product_id', how='left', validate='m:1')
    merged = merged.merge(inventory, on='product_id', how='left', validate='m:1', suffixes=('', '_inv'))

    return merged


def compute_metrics(merged_df):
    logging.info("Computing business metrics")

    df = merged_df.copy()

    if 'price' not in df.columns:
        if 'price_x' in df.columns:
            df['price'] = df['price_x']
        elif 'price_y' in df.columns:
            df['price'] = df['price_y']
        else:
            raise KeyError("No se encontró ninguna columna de precio en el dataset.")

    # Convertir tipos numéricos
    df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0.0)
    df['quantity'] = pd.to_numeric(df.get('quantity', 0), errors='coerce').fillna(0).astype(int)
    df['current_stock'] = pd.to_numeric(df.get('current_stock', 0), errors='coerce').fillna(0).astype(int)
    df['min_stock'] = pd.to_numeric(df.get('min_stock', 0), errors='coerce').fillna(0).astype(int)

    # Productos con stock crítico
    df['is_critical_stock'] = df['current_stock'] < df['min_stock']

    # Ventas totales por categoría
    ventas_categoria = (
        df.groupby('category', dropna=False)
        .apply(lambda g: (g['price'] * g['quantity']).sum())
        .reset_index(name='total_sales')
    )

    # Productos más vendidos (por cantidad)
    ventas_producto = (
        df.groupby(['product_id', 'title', 'category'], dropna=False)
        .agg(total_quantity=('quantity', 'sum'), avg_price=('price', 'mean'))
        .reset_index()
        .sort_values('total_quantity', ascending=False)
    )

    # Rentabilidad estimada
    ventas_producto['estimated_revenue'] = ventas_producto['avg_price'] * ventas_producto['total_quantity']
    ventas_producto['estimated_cost'] = ventas_producto['estimated_revenue'] * 0.6
    ventas_producto['estimated_profit'] = ventas_producto['estimated_revenue'] - ventas_producto['estimated_cost']
    ventas_producto['profit_margin'] = ventas_producto.apply(
        lambda r: r['estimated_profit'] / r['estimated_revenue'] if r['estimated_revenue'] > 0 else 0.0, axis=1
    )

    # Clasificación por bandas de ventas
    ventas_producto['sales_category'] = pd.cut(
        ventas_producto['total_quantity'],
        bins=[-1, 0, 10, 50, 999999],
        labels=['sin_ventas', 'bajas', 'medias', 'altas']
    )

    return {
        'merged': df,
        'ventas_categoria': ventas_categoria,
        'ventas_producto': ventas_producto
    }
