import pandas as pd
import requests
import logging

def fetch_products_api(api_url: str, timeout: int = 30) -> pd.DataFrame:
    """
    Descarga datos de productos desde la Fake Store API.
    """
    logging.info(f"Descargando datos desde la API: {api_url}")
    response = requests.get(api_url, timeout=timeout)
    response.raise_for_status()  
    data = response.json()
    df = pd.DataFrame(data)
    logging.info(f"Datos descargados correctamente: {len(df)} productos.")
    return df


def load_csv(file_path: str) -> pd.DataFrame:
    """
    Carga un archivo CSV local y lo devuelve como DataFrame.
    """
    logging.info(f"Cargando CSV: {file_path}")
    df = pd.read_csv(file_path)
    logging.info(f"Archivo {file_path} cargado con {len(df)} filas y {len(df.columns)} columnas.")
    return df


def save_parquet(df: pd.DataFrame, output_path: str):
    """
    Guarda un DataFrame en formato Parquet.
    """
    logging.info(f"Guardando DataFrame en Parquet: {output_path}")
    df.to_parquet(output_path, index=False)
    logging.info("Archivo guardado correctamente.")
