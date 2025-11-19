# 🌍 Air Quality Streaming Pipeline

Projet d'architecture Big Data en temps Réel.
Ce projet récupère les données de qualité de l'air (Open-Meteo), les traite en flux continu (Flink) et les visualise (Streamlit).

## 🏗 Architecture

1.  **Source** : API Open-Meteo (Simulée par `producer.py`)
2.  **Broker** : Redpanda (Compatible Kafka)
3.  **Processing** : Apache Flink (Agrégations fenêtrées)
4.  **Storage** : PostgreSQL
5.  **Viz** : Streamlit

## 🚀 Installation

### 1. Prérequis
* Docker & Docker Compose
* Python 3.8+

### 2. Installation des dépendances Python
```pip install -r requirements.txt```

### 3. Lancement de l'infrastructure
```docker-compose up -d```

### 4. Démarrage du Pipeline
## Étape 1 : Lancer le Job Flink
Le script Flink doit tourner à l'intérieur du conteneur Docker.

```docker exec -it flink-jobmanager bash```

# Une fois dans le conteneur :
```/opt/flink/bin/flink run -py /opt/flink/usrlib/processor.py```

(Vérifier sur http://localhost:8081 que le job est RUNNING)

## Étape 2 : Lancer le Producer (Données)
Ce script simule les capteurs et envoie les données vers Redpanda.

```python producer.py```

## Étape 3 : Lancer le Dashboard
```streamlit run dashboard.py```

## Accès

Streamlit Dashboard : http://localhost:8501

Flink Dashboard : http://localhost:8081

Redpanda Console : http://localhost:8080
