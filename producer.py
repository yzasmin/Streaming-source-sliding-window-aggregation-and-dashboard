import time
import json
import random
import requests
from kafka import KafkaProducer
from datetime import datetime

# --- CONFIGURATION ---
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'raw-air-quality'

# Coordonnées des villes à surveiller
CITIES = {
    "Paris": {"lat": 48.8566, "lon": 2.3522},
    "Lyon": {"lat": 45.7640, "lon": 4.8357},
    "Marseille": {"lat": 43.2965, "lon": 5.3698}
}

# URL de l'API Open-Meteo Air Quality
API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

def get_air_quality(city_name, lat, lon):
    """
    Récupère les données réelles et ajoute une légère variation
    pour simuler un capteur temps réel actif.
    """
    try:
        params = {
            "latitude": lat,
            "longitude": lon,
            "current": "pm10,pm2_5,nitrogen_dioxide,ozone", # On demande les valeurs actuelles
            "timezone": "Europe/Paris"
        }
        response = requests.get(API_URL, params=params)
        data = response.json()
        
        # Récupération des mesures 'current'
        current = data.get('current', {})
        
        # Construction du message JSON
        # Note : J'ajoute un petit random.uniform pour simuler la fluctuation du capteur
        # sinon le graphique sera plat pendant 1h.
        message = {
            "city": city_name,
            "latitude": lat,
            "longitude": lon,
            "timestamp_event": current.get('time'),
            
            # --- CORRECTION ICI : On multiplie par 1000 ---
            "timestamp_ingestion": int(time.time() * 1000), 
            # ----------------------------------------------
            
            "pm10": current.get('pm10') + random.uniform(-0.5, 0.5), 
            "pm2_5": current.get('pm2_5') + random.uniform(-0.2, 0.2),
            "no2": current.get('nitrogen_dioxide') + random.uniform(-0.5, 0.5),
            "ozone": current.get('ozone') + random.uniform(-1.0, 1.0)
        }
        return message
        
    except Exception as e:
        print(f"Erreur lors de la récupération pour {city_name}: {e}")
        return None

def run_producer():
    # Initialisation du Producer Kafka
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8') # Sérialisation auto en JSON
    )
    
    print(f" Démarrage du producer vers {KAFKA_BROKER}...")
    
    try:
        while True:
            for city_name, coords in CITIES.items():
                data = get_air_quality(city_name, coords['lat'], coords['lon'])
                
                if data:
                    # Envoi dans Redpanda
                    producer.send(TOPIC_NAME, value=data)
                    print(f" Envoyé [{city_name}] : PM2.5 = {data['pm2_5']:.2f}")
            
            # On force l'envoi immédiat
            producer.flush()
            
            # Pause de 5 secondes avant la prochaine boucle (Simulation temps réel)
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("Arrêt du producer.")
        producer.close()

if __name__ == "__main__":
    run_producer()