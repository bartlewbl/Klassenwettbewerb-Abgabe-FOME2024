# Smart Ventilation Optimization Project

## Übersicht

Das Smart Ventilation Optimization Project ist darauf ausgelegt, Sensordaten zu sammeln, basierend auf diesen Daten Vorhersagen zu treffen und eine Webschnittstelle bereitzustellen, um die Daten zu visualisieren und mit ihnen zu interagieren. Die Anwendung ist mit Flask für die Webschnittstelle und MQTT für die Datensammlung gebaut.

## Funktionen

- Echtzeit-Datensammlung von Sensoren über MQTT.
- Datenvisualisierung über eine Webschnittstelle.
- Periodische Vorhersagen mit vortrainierten Maschinenlernmodellen.
- Sammlung von Benutzerfeedback zu den Vorhersagen.

## Anforderungen

Die Anforderungen sind in der Datei `requirements.txt` definiert. 
Um alle erforderlichen Pakete zu installieren, führen Sie den folgenden Befehl aus:

pip install -r requirements.txt

## Installation

1. Repository klonen:

    ```
    git clone <repository_url>
    cd <repository_directory>
    ```

2. Erstellen und Aktivieren eines virtuellen Umfelds:

    ```
    python -m venv venv
    source venv/bin/activate   # Unter Windows: `venv\Scripts\activate`
    ```

3. Erforderliche Pakete installieren:

    ```
    pip install -r requirements.txt
    ```

## Konfiguration

1. Stellen Sie sicher, dass Sie die Konfigurationsdatei `api_config.yaml` im Verzeichnis `smart-ventilation` mit der folgenden Struktur haben:

    ```
    READ_API_KEY: "" # Ihr API-Schlüssel für Lesezugriff
    POST_API SHOW_KEY: "" # Ihr API-Schlüssel für Schreibzugriff
    API_BASE_URL: "" # Basis-URL der API
    CONTENT_TYPE: "" # Inhaltstyp, der bei API-Anfragen verwendet wird
    CLOUD_SERVICE_URL: "" # URL Ihres Cloud-Dienstes
    USERNAME: "" # Benutzername für den Zugang zum Cloud-Dienst
    PASSWORD: "" # Passwort für den Zugang zum Cloud-Dienst
    ```

## Modelle

Im Verzeichnis `smart-ventilations/models/` befinden sich die folgenden vorbereiteten Machine Learning-Modelle im `.pkl`-Format. Diese Modelle sind serialisiert und optimiert für den Einsatz, sodass sie schnell in die Anwendung geladen und genutzt werden können:

- `Logistic_Regression.pkl` — Ein Modell basierend auf der logistischen Regression.
- `Decision_Tree.pkl` — Ein Entscheidungsbaummodell.
- `Random_Forest.pkl` — Ein Modell, das auf dem Random-Forest-Algorithmus basiert.

## Anwendung starten

1. Starten Sie den MQTT-Client und die Flask-Anwendung:

    ```
    python application_modular.py
    ```

2. Greifen Sie auf die Webschnittstelle zu, indem Sie einen Browser öffnen und zu `http://127.0.0.1:5000` navigieren.

## Endpunkte der Anwendung

- `/`: Zeigt das Hauptdashboard mit Echtzeit-Sensordaten an.
- `/plots`: Bietet Diagramme der Echtzeit-Sensordaten.
- `/feedback`: Ermöglicht es Benutzern, Feedback zu den Vorhersagen zu geben.
- `/thank_you`: Zeigt eine Dankesseite nach dem Absenden des Feedbacks an.
- `/contact`: Zeigt die Kontaktseite der Anwendung an.

## Logging

Die Anwendung protokolliert wichtige Ereignisse und Fehler in der Konsole. Stellen Sie sicher, dass das Logging im `application_modular.py`-Skript mit dem logging-Modul entsprechend konfiguriert ist.

## Hinweise

Stellen Sie sicher, dass alle Pfade in `application_modular.py` und `mqtt_client.py` korrekt gemäß Ihrer Projektstruktur gesetzt sind. Die Anwendung geht davon aus, dass Sensordaten zu bestimmten MQTT-Themen veröffentlicht werden. Passen Sie die Themen und die Datenverarbeitung in `mqtt_client.py` nach Bedarf an.

## Fehlerbehebung

Falls die Anwendung nicht startet, überprüfen Sie die Konsolenprotokolle auf Fehler bezüglich fehlender Konfigurationsdateien, Modelle oder Abhängigkeiten. Stellen Sie sicher, dass Ihr MQTT-Broker läuft und mit den richtigen Anmeldeinformationen erreichbar ist.
