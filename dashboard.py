import streamlit as st
import pandas as pd
import psycopg2
import time
import plotly.express as px

# --- CONFIGURATION DE LA PAGE ---
st.set_page_config(
    page_title="Moniteur Qualité de l'Air",
    page_icon="🌍",
    layout="wide"
)

# --- CONNEXION BASE DE DONNÉES ---
def get_data():
    """Récupère les données agrégées depuis Postgres"""
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="air_quality_db",
        user="admin",
        password="password"
    )
    
    # On récupère les 100 dernières minutes de données
    query = """
    SELECT * FROM air_quality_agg 
    ORDER BY window_end DESC 
    LIMIT 200
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# --- COORDONNÉES DES VILLES (Pour la carte) ---
# Comme Flink n'a pas stocké la lat/lon dans la table d'agrégation, on les remet ici
CITIES_COORDS = {
    "Paris": {"lat": 48.8566, "lon": 2.3522},
    "Lyon": {"lat": 45.7640, "lon": 4.8357},
    "Marseille": {"lat": 43.2965, "lon": 5.3698}
}

# --- INTERFACE ---
st.title("🌍 Dashboard Qualité de l'Air Temps Réel")
st.markdown("Pipeline: **Open-Meteo ➡️ Redpanda ➡️ Flink ➡️ Postgres ➡️ Streamlit**")

# Placeholder pour le rafraîchissement auto
placeholder = st.empty()

while True:
    with placeholder.container():
        # 1. Récupération des données
        try:
            df = get_data()
            
            if df.empty:
                st.warning("⏳ En attente de données... (Le job Flink calcule les premières fenêtres)")
                time.sleep(5)
                continue
            
            # Conversion des timestamps
            df['window_end'] = pd.to_datetime(df['window_end'])
            
            # Données les plus récentes par ville
            latest_df = df.sort_values('window_end').groupby('city').tail(1)

            # --- SECTION 1 : KPIS (Indicateurs) ---
            kpi1, kpi2, kpi3 = st.columns(3)
            
            for i, (col, city) in enumerate(zip([kpi1, kpi2, kpi3], ["Paris", "Lyon", "Marseille"])):
                city_data = latest_df[latest_df['city'] == city]
                
                if not city_data.empty:
                    pm25 = city_data['avg_pm2_5'].values[0]
                    ozone = city_data['max_ozone'].values[0]
                    
                    # Couleur dynamique selon pollution
                    delta_color = "normal" if pm25 < 15 else "inverse"
                    
                    with col:
                        st.metric(
                            label=f"📍 {city}",
                            value=f"{pm25:.2f} µg/m³ (PM2.5)",
                            delta=f"Ozone Max: {ozone:.1f}",
                            delta_color=delta_color
                        )

            col_charts, col_map = st.columns([2, 1])

            # --- SECTION 2 : GRAPHIQUE ---
            with col_charts:
                st.subheader("📈 Évolution PM2.5 (Moyenne glissante)")
                fig = px.line(df, x='window_end', y='avg_pm2_5', color='city', markers=True)
                st.plotly_chart(fig, use_container_width=True)

            # --- SECTION 3 : CARTE ---
            with col_map:
                st.subheader("🗺️ Carte")
                # On fusionne les données récentes avec les coordonnées
                map_data = []
                for _, row in latest_df.iterrows():
                    coords = CITIES_COORDS.get(row['city'])
                    if coords:
                        map_data.append({
                            "lat": coords["lat"],
                            "lon": coords["lon"],
                            "size": row["avg_pm2_5"] * 10, # Taille du point selon pollution
                            "color": [255, 0, 0, 160] if row["avg_pm2_5"] > 15 else [0, 255, 0, 160]
                        })
                
                if map_data:
                    st.map(pd.DataFrame(map_data), latitude="lat", longitude="lon", size="size", color="color")

            # --- TABLEAU DE DONNÉES BRUTES ---
            with st.expander("Voir les données détaillées"):
                st.dataframe(df.sort_values('window_end', ascending=False))

        except Exception as e:
            st.error(f"Erreur de connexion : {e}")

        # Pause de 5 secondes avant le prochain rafraîchissement
        time.sleep(5)