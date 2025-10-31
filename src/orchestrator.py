import yaml
import logging
from datetime import datetime
import pandas as pd
import os

from src.ingestion import fetch_products_api, load_csv, save_parquet
from src.transformation import merge_data, compute_metrics
from src.quality_checks import (
    check_no_negative_prices,
    check_stock_integer_positive,
    check_categories_exist,
    check_sale_dates_valid
)

class EcommerceDataPipeline:
    def __init__(self, config_path):
        self.config = self.load_config(config_path)
        self.setup_logging()
        self.now = datetime.utcnow().strftime("%Y-%m-%d")
        self.raw_folder = "data/raw"
        self.processed_folder = self.config['processing']['output_path']
        self.outputs_folder = self.config['processing'].get('outputs_path', 'data/outputs')
        os.makedirs(self.raw_folder, exist_ok=True)
        os.makedirs(self.processed_folder, exist_ok=True)
        os.makedirs(self.outputs_folder, exist_ok=True)

    def load_config(self, config_path):
        with open(config_path, 'r', encoding='utf-8') as file:
            return yaml.safe_load(file)

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('pipeline_execution.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def run_pipeline(self):
        self.logger.info("Iniciando pipeline de e-commerce...")

        try:
            products_df = fetch_products_api(self.config['api']['url'], self.config['api'].get('timeout', 30))
            sales_df = load_csv(self.config['data_sources']['sales_file'])
            inventory_df = load_csv(self.config['data_sources']['inventory_file'])

            products_raw_path = os.path.join(self.raw_folder, f"products_{self.now}.parquet")
            sales_raw_path = os.path.join(self.raw_folder, f"sales_{self.now}.parquet")
            inventory_raw_path = os.path.join(self.raw_folder, f"inventory_{self.now}.parquet")

            save_parquet(products_df, products_raw_path)
            save_parquet(sales_df, sales_raw_path)
            save_parquet(inventory_df, inventory_raw_path)
        except Exception as e:
            self.logger.exception("Error en la etapa de ingesta")
            raise

        try:
            merged = merge_data(products_df, sales_df, inventory_df)
            metrics = compute_metrics(merged)
        except Exception as e:
            self.logger.exception("Error en transformación")
            raise

        try:
            check_no_negative_prices(products_df)
            check_stock_integer_positive(inventory_df)

            if 'category' not in sales_df.columns:
                try:
                    sales_df = sales_df.merge(products_df[['product_id', 'category']], on='product_id', how='left')
                except Exception:
                    pass

            check_categories_exist(products_df, sales_df)

            if 'sale_date' in sales_df.columns:
                check_sale_dates_valid(sales_df, 'sale_date')

            self.logger.info("Quality checks pasaron correctamente")
        except AssertionError as ae:
            self.logger.error(f"Quality check falló: {ae}")
            raise
        except Exception as e:
            self.logger.exception("Error en quality checks")
            raise

        try:
            merged_path = os.path.join(self.processed_folder, f"merged_{self.now}.parquet")
            ventas_categoria_path = os.path.join(self.outputs_folder, f"ventas_categoria_{self.now}.csv")
            ventas_producto_path = os.path.join(self.outputs_folder, f"ventas_producto_{self.now}.csv")

            metrics['merged'].to_parquet(merged_path, index=False)
            metrics['ventas_categoria'].to_csv(ventas_categoria_path, index=False)
            metrics['ventas_producto'].to_csv(ventas_producto_path, index=False)

            self.logger.info("Outputs generados y guardados")
        except Exception as e:
            self.logger.exception("Error guardando outputs")
            raise

        try:
            report_path = os.path.join(self.outputs_folder, f"report_{self.now}.md")
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(f"# Reporte pipeline {self.now}\n\n")
                f.write(f"- Total filas merged: {len(metrics['merged'])}\n")
                f.write(f"- Categorías detectadas: {len(metrics['ventas_categoria'])}\n\n")
                f.write("## Top 5 productos por cantidad\n")
                top5 = metrics['ventas_producto'].head(5)
                for _, row in top5.iterrows():
                    f.write(f"- {row['title']} (id:{row['product_id']}): {int(row['total_quantity'])} unidades, ingreso estimado: {row['estimated_revenue']:.2f}\n")
            self.logger.info(f"Reporte simple generado: {report_path}")
        except Exception as e:
            self.logger.exception("Error generando reporte")
            raise

        self.logger.info("Pipeline finalizado correctamente")


if __name__ == "__main__":
    pipeline = EcommerceDataPipeline('config/pipeline_config.yaml')
    pipeline.run_pipeline()
