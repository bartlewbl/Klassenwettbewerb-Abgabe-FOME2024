from flask_apscheduler import APScheduler
from flask import Flask, jsonify, render_template, request, session
import logging
import requests
from datetime import datetime, timedelta
from mqtt_client import MQTTClient
from api_config_loader import load_api_config
from datetime import datetime, timedelta
import secrets
import numpy as np
import os


logging.basicConfig(level=logging.INFO)
base_dir = os.path.abspath(os.path.dirname(__file__))
static_folder = os.path.join(base_dir, 'static')

app = Flask(__name__, static_folder=static_folder)
app.secret_key = secrets.token_hex(16)

# Pfad zu YAML-Konfigurationsdatei
config_file_path = 'smart_ventilation/api_config.yaml'

# API-Konfiguration aus YAML-Datei laden
api_config = load_api_config(config_file_path)

# API-Schlüssel und Basis-URL extrahieren
READ_API_KEY = api_config['READ_API_KEY']
POST_API_KEY = api_config['POST_API_KEY']
API_BASE_URL = api_config['API_BASE_URL']
CONTENT_TYPE = api_config["CONTENT_TYPE"]

# MQTT-Client initialisieren
mqtt_client = MQTTClient()
mqtt_client.initialize()


class Config:
    SCHEDULER_API_ENABLED = True

app.config.from_object(Config())
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()


@app.route("/", methods=["GET", "POST"])
def index():
    """
    Diese Funktion wird aufgerufen, wenn auf die Route "/" zugegriffen wird. Sie dient dazu,
    Sensordaten, die über MQTT empfangen wurden, auszulesen und in einer Webseite anzuzeigen.
    Die Funktion handhabt sowohl GET- als auch POST-Anfragen, um Daten dynamisch darzustellen.

    Die Sensordaten beinhalten typischerweise Temperatur, Feuchtigkeit, CO2, TVOC, Umgebungstemperatur
    und Vorhersagen. Falls keine Daten vorhanden sind, werden Standardwerte verwendet.

    Bei einem Fehler während der Verarbeitung wird eine benutzerfreundliche Fehlermeldung zurückgegeben,
    und es wird protokolliert, was zum Fehler geführt hat.

    :return: Ein gerendertes Template mit Sensordaten oder eine Fehlermeldung bei Problemen.
    """
    try:
        # Sicherstellen, dass combined_data initialisiert ist und alle erwarteten Schlüssel enthält
        if not hasattr(mqtt_client, 'combined_data') or not mqtt_client.combined_data:
            # Standardwerte einrichten, falls combined_data nicht verfügbar oder leer ist
            sensor_data = {}
            temperature = 0
            humidity = 0
            co2 = 0
            tvoc = "Zurzeit nicht verfügbar"
            ambient_temp = 0
            predictions = {}
        else:
            # Sensordaten aus mqtt_client.combined_data sicher extrahieren
            sensor_data = mqtt_client.combined_data
            temperature = sensor_data.get("temperature", 0)
            humidity = sensor_data.get("humidity", 0)
            co2 = sensor_data.get("co2", 0)
            tvoc = sensor_data.get("tvoc", "Zurzeit nicht verfügbar")
            ambient_temp = sensor_data.get("ambient_temp", 0)
            predictions = sensor_data.get('predictions', {})

        # Das Template index.html mit den Daten rendern
        return render_template(
            "index.html",
            sensor_data=sensor_data,
            temperature=temperature,
            humidity=humidity,
            co2=co2,
            tvoc=tvoc,
            ambient_temp=ambient_temp,
            predictions=predictions,
        )
    
    except Exception as e:
        # Detailliertes Protokollieren des Fehlers und der Sensordaten zur Fehlerbehebung
        logging.error("Ein Fehler ist in index() aufgetreten: %s. Sensordaten: %s", str(e), str(mqtt_client.combined_data))
        return "Es gab ein Problem bei der Verarbeitung Ihrer Anfrage. Bitte aktualisieren Sie die Seite.", 500


@app.route("/plots")
def plots():

    """
    Generiert und rendert Echtzeit-Datenplots basierend auf den neuesten Sensordaten, beginnend beim ersten nicht-leeren Datenpunkt und dauert eine Stunde.
    Wenn das einstündige Intervall erreicht ist, wird die Startzeit zurückgesetzt und die Anzeige beginnt erneut mit den neuen Daten.
    :return: HTML-Seite mit den Echtzeit-Datenplots oder eine Fehlermeldung, falls keine Sensordaten verfügbar sind.
    """
    
    sensor_data = mqtt_client.get_latest_sensor_data()

    if sensor_data: 

        # Listen vorbereiten, um gefilterte Daten zu sammeln
        co2_data = [data.get('co2', None) for data in sensor_data]
        temperature_data = [data.get('temperature', None) for data in sensor_data]
        humidity_data = [data.get('humidity', None) for data in sensor_data]
        tvoc_data = [data.get('tvoc', None) for data in sensor_data]
        time_data = [data.get('time', None) for data in sensor_data]

        # Datenpunkte ausrichten, um sicherzustellen, dass die Daten jedes Sensors die gleiche Länge haben
        max_length = max(len(co2_data), len(temperature_data), len(humidity_data), len(time_data) , len(tvoc_data))
        co2_data += [None] * (max_length - len(co2_data))
        temperature_data += [None] * (max_length - len(temperature_data))
        humidity_data += [None] * (max_length - len(humidity_data))
        tvoc_data += [None] * (max_length - len(tvoc_data))
        time_data += [None] * (max_length - len(time_data))

        # HTML-Seite mit Echtzeit-Datenplots rendern
        return render_template(
            "plots.html",
            co2_data=co2_data,
            temperature_data=temperature_data,
            humidity_data=humidity_data,
            tvoc_data=tvoc_data,
            time_data=time_data
        )
    
    else:
        # Leere Datensätze für die Diagramme vorbereiten
        co2_data = []
        temperature_data = []
        humidity_data = []
        tvoc_data = []
        time_data = []

        # Template mit leeren Datensätzen rendern
        return render_template(
            "plots.html",
            co2_data=co2_data,
            temperature_data=temperature_data,
            humidity_data=humidity_data,
            tvoc_data=tvoc_data,
            time_data=time_data
        )
    

def convert_to_serializable(obj):
    if isinstance(obj, (np.int64, np.int32, np.float64, np.float32)):
        return obj.item()
    raise TypeError("Unserializable object {} of type {}".format(obj, type(obj)))


@app.route('/feedback', methods=['GET', 'POST'])
def feedback():

    """
    Diese Funktion behandelt das Feedback der Benutzer bezüglich der Vorhersagen.
    
    Wenn die Methode POST ist, wird das Feedback gesendet.
    Wenn die Methode GET ist, wird das Feedback-Formular angezeigt.
    Validiert die Daten und sendet sie an die API.
    Bei Erfolg wird die Dankeseite angezeigt.
    
    :return: gerenderte HTML-Seite oder JSON-Antwort mit Fehlermeldung
    """

    if request.method == 'POST':

        try:
            # Vorhersagen vom MQTT-Client abrufen
            predictions = mqtt_client.latest_predictions
            if not predictions:
                return "Keine Vorhersagen verfügbar, um Feedback zu geben", 400
            
            combined_data = mqtt_client.combined_data
            logging.info(f"current combined_data: {combined_data}")

            features_df = mqtt_client.latest_features_df
            logging.info(f"current features_df: {features_df}")

            # Feedback-Daten erstellen
            feedback_data = {
                "temperature": float(features_df['temperature'].iloc[0]),
                "humidity": float(features_df['humidity'].iloc[0]),
                "co2": float(features_df['co2'].iloc[0]),
                "timestamp": combined_data["time"][-1],
                "outdoor_temperature": float(features_df['ambient_temp'].iloc[0]),
                "accurate_prediction": int(request.form['accurate_prediction'])
            }

            # Header für die API-Anfrage
            headers = {
                "X-Api-Key": POST_API_KEY,
                "Content-Type": CONTENT_TYPE
            }

            # Feedback-Daten an die API senden
            response = requests.post(
                API_BASE_URL,
                headers=headers,
                json=feedback_data
            )

            logging.info(f"Payload gesendet: {feedback_data}")
            logging.info(f"API Antwort: {response.json()}")

            if response.status_code == 200:
                last_prediction = mqtt_client.latest_predictions.get("Logistic Regression")
                logging.info(f"last_prediciton in feedback(): {last_prediction}")

                if isinstance(last_prediction, np.integer):
                    last_prediction = int(last_prediction)
                elif not isinstance(last_prediction, int):
                    last_prediction = int(last_prediction)

                session['last_prediction'] = last_prediction
                session.permanent = True  # Sitzung als permanent markieren

                # latest_predictions löschen, um Speicherplatz freizugeben
                mqtt_client.latest_predictions = {}
                return render_template('thank_you.html')
            else:
                return jsonify({"Meldung": "Keine Rückmeldung übermittelt", "status": response.status_code, "response": response.text}), 400

        except Exception as e:
            logging.error(f"Ein unerwarteter Fehler ist aufgetreten:: {e}")
            return jsonify({"Meldung": "Ein unerwarteter Fehler ist aufgetreten:", "Fehler:": str(e)}), 500
        
    else:
        try:
            predictions = mqtt_client.latest_predictions
            if not predictions:
                return render_template('feedback.html', error=True)
            
            predictions = mqtt_client.latest_predictions
            features_df = mqtt_client.latest_features_df

            return render_template('feedback.html', predictions=predictions, features=features_df.to_dict(orient='records')[0])

        except Exception as e:
            logging.error(f"Beim Abrufen von Vorhersagen ist ein unerwarteter Fehler aufgetreten: {e}")
            return str(e), 500


def get_data(timestamp):

    """
    Utility-Funktion zum Abrufen von Daten für einen bestimmten Zeitstempel
    """

    try:
        return mqtt_client.fetch_data(timestamp)
    except Exception as e:
        logging.error(f"Failed to fetch data: {e}")
        return {}


@app.route('/leaderboard', methods=['GET', 'POST'])
def leaderboard():
    try:
        if request.method == "POST":
            predictions = mqtt_client.latest_predictions.get("Logistic Regression")
            logging.info(f"Vorhersage im Leaderboard: {predictions}")

            # Explizite Prüfung auf None
            if predictions is None:
                last_prediction = session.get('last_prediction')
                logging.info(f"last_prediction in leaderboard {last_prediction}")

                if last_prediction is not None:
                    predictions = last_prediction  # scherstellen, dass Vorhersagen aus der Sitzung abgerufen werden

                if predictions is None:  # Nochmals explizit auf None prüfen
                    logging.error("Keine Vorhersagen verfügbar in latest_predictions und session")

                    return render_template('leaderboard.html', error=True)
            
            if 'last_prediction' not in session:

                last_prediction = mqtt_client.latest_predictions.get("Logistic Regression")

                if isinstance(last_prediction, np.integer):
                    last_prediction = int(last_prediction)
                
                elif not isinstance(last_prediction, int):
                    last_prediction = int(last_prediction)
                
                session['last_prediction'] = last_prediction
            
            if 'current_data' not in session:
                
                combined_data = mqtt_client.combined_data
                logging.info(f"combined_data in leaderboard: {combined_data}")

                latest_date = combined_data["time"][-1]
                logging.info(f"latest_data: innerhalb der Leaderboard-Funktion {latest_date}")
                
                latest_date = datetime.strptime(latest_date, "%Y-%m-%d %H:%M")

                adjusted_date = latest_date - timedelta(minutes=1)
                adjusted_date_str = adjusted_date.strftime("%Y-%m-%d %H:%M")
                
                logging.info(f"Angepasstes Datum: {adjusted_date_str}")

                current_data = get_data(adjusted_date_str)
                logging.info(f"letzte current_data in der Leaderboard-Funktion: {current_data}")
                
                formatted_current_data = [{
                    'timestamp': current_data.get('timestamp'),
                    'co2_values': float(current_data.get('co2_values')) if current_data.get('co2_values') is not None else None,
                    'temperature': float(current_data.get('temperature')) if current_data.get('temperature') is not None else None,
                    'humidity': float(current_data.get('humidity')) if current_data.get('humidity') is not None else None,
                }]

                session['latest_date'] = adjusted_date_str
                logging.info(f"session['latest_date'] {session['latest_date']}")

                session['current_data'] = formatted_current_data
                logging.info(f"session['current_data']  {session['current_data'] }")
                
                session.permanent = True

            else:

                formatted_current_data = session['current_data']
                adjusted_date_str = session['latest_date']

            if predictions == 1:
                return render_template('leaderboard2.html', current_data=formatted_current_data, future_data=None, adjusted_date_str=adjusted_date_str, error=False)
            
            else:
                return render_template('leaderboard.html', current_data=formatted_current_data, future_data=None, adjusted_date_str=adjusted_date_str, error=False)

        elif request.method == "GET":

            adjusted_date_str = session.get('latest_date')
            logging.info(f"session['latest_date'] {session['latest_date']}")
            
            current_data = session.get('current_data')
            logging.info(f"session['current_data']  {session['current_data'] }")
            
            predictions = session.get('last_prediction')  # Vorhersagen aus der Sitzung abrufen

            if not adjusted_date_str or not current_data:

                logging.error("Keine Daten in der Sitzung verfügbar, bitte erst eine Vorhersage machen")
                return "Keine Daten verfügbar, bitte führen Sie eine Vorhersage durch.", 400

            future_data_response = get_future_data(adjusted_date_str)
            if future_data_response.status_code != 200:
                return future_data_response
            
            future_data = future_data_response.get_json()
            logging.info(f"aktuellste future_data innerhalb der Leaderboard-Funktion: {future_data}")

            formatted_future_data = [{
                'timestamp': future_data.get('timestamp'),
                'co2_values': float(future_data.get('co2_values')) if future_data.get('co2_values') is not None else None,
                'temperature': float(future_data.get('temperature')) if future_data.get('temperature') is not None else None,
                'humidity': float(future_data.get('humidity')) if future_data.get('humidity') is not None else None,
            }]

            if predictions == 1:
                response = render_template('leaderboard2.html', current_data=current_data, future_data=formatted_future_data, adjusted_date_str=adjusted_date_str, error=False)
            
            else:
                response = render_template('leaderboard.html', current_data=current_data, future_data=formatted_future_data, adjusted_date_str=adjusted_date_str, error=False)

            # Sitzungsdaten nach der Verwendung löschen

            session.pop('last_prediction_id', None)
            session.pop('latest_date', None)
            session.pop('current_data', None)
            session.pop('last_prediction', None)
            
            return response

    except Exception as e:

        logging.error(f"Ein unerwarteter Fehler ist aufgetreten:: {e}")
        return jsonify({"message": "Ein unerwarteter Fehler ist aufgetreten:", "Fehler:": str(e)}), 500

    
@app.route('/future_data/<timestamp>')
def get_future_data(timestamp):

    try:

        last_prediction = session.get('last_prediction')

        if last_prediction is None:
            logging.error("Keine Vorhersage-ID in Sitzung gefunden")

            return jsonify({"Error": "Keine Vorhersage-ID in Sitzung gefunden"}), 400
        
        logging.info(f"Abruf zukünftiger Daten für Zeitstempel: {timestamp}")
        future_data = mqtt_client.fetch_future_data(timestamp)
        
        if not future_data:
            logging.info(f"Keine zukünftigen Daten für Zeitstempel verfügbar: {timestamp}")
            return jsonify({"Error": "Keine zukünftigen Daten verfügbar"}), 404
        
        formatted_future_data = {
            'timestamp': future_data.get('timestamp'),
            'co2_values': float(future_data.get('co2_values')) if future_data.get('co2_values') is not None else None,
            'temperature': float(future_data.get('temperature')) if future_data.get('temperature') is not None else None,
            'humidity': float(future_data.get('humidity')) if future_data.get('humidity') is not None else None,
        }

        logging.info(f"Future data fetched successfully: {formatted_future_data}")
        return jsonify(formatted_future_data)
    
    except Exception as e:
        logging.error(f"Fehler beim Abrufen von Zukunftsdaten: {e}")
        return jsonify({"Error": str(e)}), 500


@app.route('/clear_session')
def clear_session():

    try:

        session.clear()
        return "Die Sitzungsdaten wurden erfolgreich gelöscht", 200
    
    except Exception as e:

        logging.error(f"Fehler beim Löschen von Sitzungsdaten: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/thank_you')
def thank_you():

    """
    Diese Funktion rendert die Dankeseite nach dem Absenden des Feedbacks.
    
    :return: gerenderte HTML-Seite
    """

    return render_template('thank_you.html')


@app.route('/contact')
def contact():

    """
    Diese Funktion rendert die Kontaktseite der Anwendung.
    
    :return: gerenderte HTML-Seite
    """
    
    return render_template('contact.html')


if __name__ == "__main__":

    # Anwendung im Debug-Modus starten
    app.run(debug=True)
