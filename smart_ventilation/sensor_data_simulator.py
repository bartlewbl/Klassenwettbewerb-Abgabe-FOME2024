import pandas as pd
import time
import threading
import logging
from datetime import datetime, timedelta
import random
import os
import numpy as np

class SensorDataSimulator:
    """
    Simulator für echte Sensordaten basierend auf vorhandenen CSV-Dateien.
    Liest Daten aus den Datensätzen und simuliert Live-Datenübertragung.
    """
    
    def __init__(self, mqtt_client):
        self.mqtt_client = mqtt_client
        self.running = False
        self.data_thread = None
        
        # Lade echte Sensordaten
        self.temperature_data = self.load_temperature_data()
        self.co2_data = self.load_co2_data()
        self.humidity_data = self.load_humidity_data()
        
        # Aktuelle Position in den Daten
        self.current_index = 0
        
        # Überprüfe Datenqualität
        temp_len = len(self.temperature_data)
        co2_len = len(self.co2_data)
        humidity_len = len(self.humidity_data)
        
        logging.info(f"Geladene Sensordaten: Temperatur={temp_len}, CO2={co2_len}, Feuchtigkeit={humidity_len}")
        
        if temp_len == 0 or co2_len == 0 or humidity_len == 0:
            logging.error("WARNUNG: Nicht alle Sensordaten konnten geladen werden!")
            logging.error("Temperatur: {}, CO2: {}, Feuchtigkeit: {}".format(temp_len, co2_len, humidity_len))
            
            # Fallback: Generiere synthetische Daten
            logging.info("Generiere synthetische Sensordaten als Fallback...")
            self.generate_synthetic_data()
    
    def load_temperature_data(self):
        """Lädt Temperaturdaten aus der CSV-Datei"""
        try:
            file_path = 'datasets/10c_temp_last_30_days.csv'
            if not os.path.exists(file_path):
                logging.error(f"Temperatur-Datei nicht gefunden: {file_path}")
                return []
                
            df = pd.read_csv(file_path)
            if df.empty:
                logging.error("Temperatur-Datei ist leer")
                return []
                
            # Konvertiere Zeitstempel
            df['time'] = pd.to_datetime(df['time'])
            # Sortiere nach Zeit
            df = df.sort_values('time')
            logging.info(f"Temperaturdaten erfolgreich geladen: {len(df)} Datenpunkte")
            return df.to_dict('records')
        except Exception as e:
            logging.error(f"Fehler beim Laden der Temperaturdaten: {e}")
            return []
    
    def load_co2_data(self):
        """Lädt CO2-Daten aus der CSV-Datei"""
        try:
            file_path = 'datasets/10c_co2_last_30_days.csv'
            if not os.path.exists(file_path):
                logging.error(f"CO2-Datei nicht gefunden: {file_path}")
                return []
                
            df = pd.read_csv(file_path)
            if df.empty:
                logging.error("CO2-Datei ist leer")
                return []
                
            # Konvertiere Zeitstempel
            df['time'] = pd.to_datetime(df['time'])
            # Sortiere nach Zeit
            df = df.sort_values('time')
            logging.info(f"CO2-Daten erfolgreich geladen: {len(df)} Datenpunkte")
            return df.to_dict('records')
        except Exception as e:
            logging.error(f"Fehler beim Laden der CO2-Daten: {e}")
            return []
    
    def load_humidity_data(self):
        """Lädt Feuchtigkeitsdaten (simuliert basierend auf Temperatur)"""
        try:
            # Verwende Temperaturdaten als Basis für Feuchtigkeit
            temp_df = pd.read_csv('datasets/10c_temp_last_30_days.csv')
            temp_df['time'] = pd.to_datetime(temp_df['time'])
            temp_df = temp_df.sort_values('time')
            
            # Simuliere Feuchtigkeit basierend auf Temperatur (inverser Zusammenhang)
            humidity_data = []
            for _, row in temp_df.iterrows():
                # Feuchtigkeit zwischen 40-80%, invers zu Temperatur
                base_humidity = 60
                temp_factor = (row['temperature'] - 10) / 20  # Normalisiere Temperatur
                humidity = base_humidity - (temp_factor * 20) + random.uniform(-5, 5)
                humidity = max(40, min(80, humidity))  # Begrenze auf 40-80%
                
                humidity_data.append({
                    'time': row['time'],
                    'humidity': round(humidity, 1)
                })
            
            return humidity_data
        except Exception as e:
            logging.error(f"Fehler beim Laden der Feuchtigkeitsdaten: {e}")
            return []
    
    def start_simulation(self):
        """Startet die Datensimulation"""
        if self.running:
            return
        
        self.running = True
        self.data_thread = threading.Thread(target=self.simulate_data_stream)
        self.data_thread.daemon = True
        self.data_thread.start()
        logging.info("Sensordaten-Simulation gestartet")
    
    def stop_simulation(self):
        """Stoppt die Datensimulation"""
        self.running = False
        if self.data_thread:
            self.data_thread.join()
        logging.info("Sensordaten-Simulation gestoppt")
    
    def simulate_data_stream(self):
        """Simuliert einen kontinuierlichen Datenstrom"""
        while self.running:
            try:
                # Sicherheitscheck: Stelle sicher, dass alle Datenlisten die gleiche Länge haben
                min_length = min(
                    len(self.temperature_data),
                    len(self.co2_data),
                    len(self.humidity_data)
                )
                
                if min_length == 0:
                    logging.error("Keine Sensordaten verfügbar")
                    time.sleep(30)
                    continue
                
                if self.current_index >= min_length:
                    # Starte von vorne
                    self.current_index = 0
                    logging.info("Starte Sensordaten-Simulation von vorne")
                
                # Hole aktuelle Daten mit Sicherheitscheck
                try:
                    temp_record = self.temperature_data[self.current_index] if self.current_index < len(self.temperature_data) else None
                    co2_record = self.co2_data[self.current_index] if self.current_index < len(self.co2_data) else None
                    humidity_record = self.humidity_data[self.current_index] if self.current_index < len(self.humidity_data) else None
                except IndexError as e:
                    logging.error(f"Index-Fehler bei Datenzugriff: {e}, current_index: {self.current_index}")
                    self.current_index = 0
                    continue
                
                # Erstelle aktuellen Zeitstempel
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M")
                
                # Füge Daten zum MQTT-Client hinzu
                self.update_sensor_data(current_time, temp_record, co2_record, humidity_record)
                
                # Erhöhe Index
                self.current_index += 1
                
                # Warte 30 Sekunden bis zum nächsten Datenpunkt
                time.sleep(30)
                
            except Exception as e:
                logging.error(f"Fehler in der Datensimulation: {e}")
                logging.error(f"current_index: {self.current_index}, data_lengths: temp={len(self.temperature_data)}, co2={len(self.co2_data)}, humidity={len(self.humidity_data)}")
                time.sleep(10)
    
    def update_sensor_data(self, current_time, temp_record, co2_record, humidity_record):
        """Aktualisiert die Sensordaten im MQTT-Client"""
        try:
            # Temperatur
            if temp_record:
                temperature = temp_record['temperature']
                # Füge kleine Variation hinzu für Realismus
                temperature += random.uniform(-0.5, 0.5)
                temperature = round(temperature, 1)
            else:
                temperature = random.uniform(18, 24)
                temperature = round(temperature, 1)
            
            # CO2
            if co2_record:
                co2 = co2_record['co2']
                # Füge kleine Variation hinzu
                co2 += random.uniform(-10, 10)
                co2 = max(400, min(2000, co2))  # Begrenze auf realistische Werte
                co2 = round(co2)
            else:
                co2 = random.randint(400, 800)
            
            # Feuchtigkeit
            if humidity_record:
                humidity = humidity_record['humidity']
                # Füge kleine Variation hinzu
                humidity += random.uniform(-2, 2)
                humidity = max(40, min(80, humidity))
                humidity = round(humidity, 1)
            else:
                humidity = random.uniform(45, 65)
                humidity = round(humidity, 1)
            
            # TVOC (simuliert, basierend auf CO2)
            tvoc = max(0, (co2 - 400) / 100 + random.uniform(-0.5, 0.5))
            tvoc = round(tvoc, 1)
            
            # Außentemperatur (basierend auf Innenraum mit Abweichung)
            ambient_temp = temperature + random.uniform(-5, 2)
            ambient_temp = round(ambient_temp, 1)
            
            # Aktualisiere MQTT-Client-Daten
            with self.mqtt_client.data_lock:
                # Füge neue Zeit hinzu
                if 'time' not in self.mqtt_client.combined_data:
                    self.mqtt_client.combined_data['time'] = []
                self.mqtt_client.combined_data['time'].append(current_time)
                
                # Füge neue Sensordaten hinzu
                if 'temperature' not in self.mqtt_client.combined_data:
                    self.mqtt_client.combined_data['temperature'] = []
                self.mqtt_client.combined_data['temperature'].append(temperature)
                
                if 'humidity' not in self.mqtt_client.combined_data:
                    self.mqtt_client.combined_data['humidity'] = []
                self.mqtt_client.combined_data['humidity'].append(humidity)
                
                if 'co2' not in self.mqtt_client.combined_data:
                    self.mqtt_client.combined_data['co2'] = []
                self.mqtt_client.combined_data['co2'].append(co2)
                
                if 'tvoc' not in self.mqtt_client.combined_data:
                    self.mqtt_client.combined_data['tvoc'] = []
                self.mqtt_client.combined_data['tvoc'].append(tvoc)
                
                if 'ambient_temp' not in self.mqtt_client.combined_data:
                    self.mqtt_client.combined_data['ambient_temp'] = []
                self.mqtt_client.combined_data['ambient_temp'].append(ambient_temp)
                
                # Behalte nur die letzten 100 Datenpunkte
                max_points = 100
                for key in ['time', 'temperature', 'humidity', 'co2', 'tvoc', 'ambient_temp']:
                    if len(self.mqtt_client.combined_data.get(key, [])) > max_points:
                        self.mqtt_client.combined_data[key] = self.mqtt_client.combined_data[key][-max_points:]
                
                # Aktualisiere latest_time
                self.mqtt_client.latest_time = current_time
            
            logging.info(f"Neue Sensordaten: Temp={temperature}°C, CO2={co2}ppm, Humidity={humidity}%, TVOC={tvoc}, Ambient={ambient_temp}°C")
            
        except Exception as e:
            logging.error(f"Fehler beim Aktualisieren der Sensordaten: {e}")
    
    def get_current_data(self):
        """Gibt die aktuellen Sensordaten zurück"""
        try:
            with self.mqtt_client.data_lock:
                if not self.mqtt_client.combined_data.get('time'):
                    return None
                
                latest_index = -1
                return {
                    'time': self.mqtt_client.combined_data['time'][latest_index],
                    'temperature': self.mqtt_client.combined_data['temperature'][latest_index],
                    'humidity': self.mqtt_client.combined_data['humidity'][latest_index],
                    'co2': self.mqtt_client.combined_data['co2'][latest_index],
                    'tvoc': self.mqtt_client.combined_data['tvoc'][latest_index],
                    'ambient_temp': self.mqtt_client.combined_data['ambient_temp'][latest_index]
                }
        except Exception as e:
            logging.error(f"Fehler beim Abrufen der aktuellen Daten: {e}")
            return None
    
    def generate_synthetic_data(self):
        """Generiert synthetische Sensordaten als Fallback"""
        try:
            logging.info("Generiere 100 synthetische Datenpunkte...")
            
            # Generiere 100 Datenpunkte für die letzten 100 Minuten
            synthetic_data = []
            base_time = datetime.now() - timedelta(minutes=100)
            
            for i in range(100):
                # Zeitstempel
                timestamp = base_time + timedelta(minutes=i)
                
                # Realistische Temperatur (18-26°C mit Tagesrhythmus)
                hour = timestamp.hour
                base_temp = 22  # Mittlere Temperatur
                daily_variation = 4 * np.sin((hour - 6) * np.pi / 12)  # Tagesrhythmus
                temperature = base_temp + daily_variation + random.uniform(-1, 1)
                
                # Realistischer CO2 (400-1200 ppm, höher während Schulzeiten)
                if 8 <= hour <= 16:  # Schulzeit
                    base_co2 = 600
                    variation = random.uniform(0, 400)
                else:  # Nachts/Früh
                    base_co2 = 400
                    variation = random.uniform(0, 100)
                co2 = base_co2 + variation
                
                # Realistische Feuchtigkeit (40-70%, invers zu Temperatur)
                humidity = max(40, min(70, 60 - (temperature - 22) * 2 + random.uniform(-5, 5)))
                
                synthetic_data.append({
                    'time': timestamp,
                    'temperature': round(temperature, 1),
                    'co2': round(co2),
                    'humidity': round(humidity, 1)
                })
            
            # Aktualisiere die Datenlisten
            self.temperature_data = [{'time': d['time'], 'temperature': d['temperature']} for d in synthetic_data]
            self.co2_data = [{'time': d['time'], 'co2': d['co2']} for d in synthetic_data]
            self.humidity_data = [{'time': d['time'], 'humidity': d['humidity']} for d in synthetic_data]
            
            logging.info("Synthetische Sensordaten erfolgreich generiert")
            
        except Exception as e:
            logging.error(f"Fehler beim Generieren synthetischer Daten: {e}")
            # Notfall-Fallback: Einfache statische Daten
            self.temperature_data = [{'time': datetime.now(), 'temperature': 22.0}]
            self.co2_data = [{'time': datetime.now(), 'co2': 500}]
            self.humidity_data = [{'time': datetime.now(), 'humidity': 55.0}]
