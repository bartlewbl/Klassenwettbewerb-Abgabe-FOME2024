from flask import Flask, render_template
import random
import time

app = Flask(__name__)

@app.route("/team-competition")
def team_competition():
    """
    Rendert die Team-Wettbewerbsseite mit Live-Daten für zwei Teams.
    """
    # Generiere Beispiel-Daten für zwei Teams
    current_time = int(time.time())
    
    # Team A Daten
    team_a_data = {
        'energy_consumption': random.uniform(2.1, 3.5),  # kWh
        'air_quality_score': random.uniform(85, 98),     # Score 0-100
        'temperature_deviation': random.uniform(0.5, 2.0),  # Abweichung von optimaler Temperatur
        'total_score': 0
    }
    
    # Team B Daten
    team_b_data = {
        'energy_consumption': random.uniform(2.0, 3.8),  # kWh
        'air_quality_score': random.uniform(80, 95),     # Score 0-100
        'temperature_deviation': random.uniform(0.3, 2.5),  # Abweichung von optimaler Temperatur
        'total_score': 0
    }
    
    # Berechne Gesamtpunktzahl (niedrigerer Energieverbrauch = mehr Punkte, höhere Luftqualität = mehr Punkte, niedrigere Temperaturabweichung = mehr Punkte)
    def calculate_score(energy, air_quality, temp_deviation):
        energy_score = max(0, 100 - (energy - 2.0) * 50)  # 2.0 kWh = 100 Punkte, 4.0 kWh = 0 Punkte
        air_quality_score = air_quality  # Direkt die Luftqualität als Punkte
        temp_score = max(0, 100 - temp_deviation * 30)  # 0°C Abweichung = 100 Punkte, 3.3°C Abweichung = 0 Punkte
        return (energy_score + air_quality_score + temp_score) / 3
    
    team_a_data['total_score'] = round(calculate_score(
        team_a_data['energy_consumption'], 
        team_a_data['air_quality_score'], 
        team_a_data['temperature_deviation']
    ), 1)
    
    team_b_data['total_score'] = round(calculate_score(
        team_b_data['energy_consumption'], 
        team_b_data['air_quality_score'], 
        team_b_data['temperature_deviation']
    ), 1)
    
    # Bestimme den Gewinner
    winner = "Team A" if team_a_data['total_score'] > team_b_data['total_score'] else "Team B"
    if team_a_data['total_score'] == team_b_data['total_score']:
        winner = "Unentschieden"
    
    return render_template(
        "team_competition.html",
        team_a=team_a_data,
        team_b=team_b_data,
        winner=winner,
        current_time=current_time,
        version=time.time()
    )

@app.route("/")
def index():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Test Team Competition</title>
    </head>
    <body>
        <h1>Team Competition Test</h1>
        <p><a href="/team-competition">Go to Team Competition</a></p>
    </body>
    </html>
    """

if __name__ == "__main__":
    print("Starting test server...")
    print("Visit http://localhost:5000 to test the team competition")
    app.run(debug=True, port=5000) 