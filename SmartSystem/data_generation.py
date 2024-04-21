import psycopg2
import joblib
import random
import logging
from datetime import datetime
from SmartSystem.config import load_database_config
from SmartSystem.database_management import get_latest_sensor_data

# Datenbankkonfiguration laden
config_file = "database_config.yaml"

# Das vortrainierte Machine-Learning-Modell laden
model = joblib.load("/Users/mudarshullar/Desktop/TelemetryData/model/model.pkl")


def generate_sensor_data():
    """
    Generiert zufällige Sensordaten.
    :return: Tuple mit Zeitstempel, Temperatur, Luftfeuchtigkeit, CO2-Werte und TVOC-Werte
    """
    try:
        temperature = round(random.uniform(10, 27), 2)
        humidity = round(random.uniform(30, 62), 2)
        co2_values = round(random.uniform(402, 600), 2)
        tvoc_values = round(random.uniform(100, 380), 2)
        timestamp = datetime.now()

        return timestamp, temperature, humidity, co2_values, tvoc_values
    except Exception as e:
        logging.error("Fehler beim Generieren von Sensordaten: %s", str(e))
        return None