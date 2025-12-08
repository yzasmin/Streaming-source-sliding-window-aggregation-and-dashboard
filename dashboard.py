import streamlit as st
import pandas as pd
import psycopg2
import time
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timezone

# --- 1. CONFIGURATION ET CSS ---
st.set_page_config(
    page_title="AirQ Monitor",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- LOGIQUE DARK/LIGHT MODE ---
ms = st.session_state
if "themes" not in ms: 
    ms.themes = {
        "current_theme": "light",
        "refreshed": True,
        "light": {
            "theme.base": "dark",
            "theme.backgroundColor": "black",
            "theme.primaryColor": "#c98bdb",
            "theme.secondaryBackgroundColor": "#202124",
            "theme.textColor": "white",
            "button_face": "🌜"
        },
        "dark": {
            "theme.base": "light",
            "theme.backgroundColor": "white",
            "theme.primaryColor": "#5591f5",
            "theme.secondaryBackgroundColor": "#F0F2F6",
            "theme.textColor": "#0a1464",
            "button_face": "🌞"
        },
    }

def ChangeTheme():
    previous_theme = ms.themes["current_theme"]
    tdict = ms.themes["light"] if ms.themes["current_theme"] == "light" else ms.themes["dark"]
    for vkey, vval in tdict.items(): 
        if vkey.startswith("theme"): 
            st._config.set_option(vkey, vval)
    ms.themes["refreshed"] = False
    if previous_theme == "dark": 
        ms.themes["current_theme"] = "light"
    elif previous_theme == "light": 
        ms.themes["current_theme"] = "dark"

if ms.themes["refreshed"] == False:
    ms.themes["refreshed"] = True
    st.rerun()

# --- CSS ---
st.markdown("""
    <style>
    .block-container { padding-top: 2rem; padding-bottom: 2rem; }
    div[data-testid="stMetricValue"] { font-size: 1.8rem; }
    .status-badge { padding: 5px 10px; border-radius: 4px; font-weight: bold; color: white; }
    </style>
""", unsafe_allow_html=True)

# --- CONSTANTES ---
COLORS = {
    "GOOD": "#198754", "MEDIUM": "#ffc107", "BAD": "#dc3545"
}

DB_CONFIG = {
    "host": "localhost", "port": "5432", "database": "air_quality_db",
    "user": "admin", "password": "password"
}

CITIES_COORDS = {
    "Paris": {"lat": 48.8566, "lon": 2.3522},
    "Lyon": {"lat": 45.7640, "lon": 4.8357},
    "Marseille": {"lat": 43.2965, "lon": 5.3698}
}
ALL_CITIES = list(CITIES_COORDS.keys())

MEASURE_OPTIONS = {
    # Pollution Particulaire
    "PM2.5 (Fines)": 'avg_pm2_5',
    "PM10 (Grosses)": 'avg_pm10',
    "Poussière (Dust)": 'avg_dust',
    "Opacité Atm. (AOD)": 'avg_aod',
    
    # Gaz Urbains
    "Dioxyde d'Azote (NO2)": 'avg_no2',
    "Monoxyde de Carbone (CO)": 'avg_co',
    "Dioxyde de Soufre (SO2)": 'avg_so2',
    
    # Photochimie
    "Ozone (O3)": 'max_ozone',
    "Index UV": 'avg_uv_index',
    
    # Climat
    "Dioxyde de Carbone (CO2)": 'avg_co2',
    "Méthane (CH4)": 'avg_ch4'
}

# Seuils d'alerte pour passer la carte en ROUGE
# (Valeurs approximatives basées sur normes OMS/UE)
THRESHOLDS = {
    'avg_pm2_5': 15,    # µg/m³
    'avg_pm10': 45,     # µg/m³
    'avg_no2': 25,      # µg/m³
    'max_ozone': 100,   # µg/m³
    'avg_co': 4000,     # µg/m³
    'avg_so2': 40,      # µg/m³
    'avg_co2': 1000,    # ppm 
    'avg_uv_index': 6,  # Index
    'avg_aod': 0.5,     # Sans unité
    'avg_dust': 50,     # µg/m³
    'avg_ch4': 1900     # ppb
}

# --- FONCTIONS MÉTIER ---
def calculate_aqi_status(pm25):
    if pm25 <= 15: return "Excellente", COLORS["GOOD"]
    elif pm25 <= 30: return "Moyenne", COLORS["MEDIUM"]
    else: return "Critique", COLORS["BAD"]

@st.cache_data(ttl=2)
def get_data():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        query = "SELECT * FROM air_quality_agg ORDER BY window_end DESC LIMIT 500"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()

# --- COMPOSANTS UI (HELPER FUNCTIONS) ---
def render_metric_card(label, value, delta, color_status=None, help_text=None):
    with st.container(border=True):
        st.metric(label=label, value=value, delta=delta, help=help_text)
        if color_status:
            st.markdown(f"<div style='height:4px; width:100%; background-color:{color_status}; border-radius:2px;'></div>", unsafe_allow_html=True)

def render_header_status(latency_sec, last_update):
    cols = st.columns([3, 1, 1])
    with cols[0]:
        st.title("AirQ Monitor")
        text_content = """
        <div style="text-align: justify; font-size: 14px; color: #808495; margin-bottom: 10px;">
            La surveillance de la qualité de l'air est critique pour la santé publique. 
            Les systèmes traditionnels (Batch) analysent les données avec trop de latence. 
            L'objectif de ce projet est de construire une pipeline de traitement en temps réel capable de : 
            Ingérer des flux de données simulés réalistes (API Open-Meteo). 
            Traiter ces flux pour lisser les variations via des fenêtres temporelles (Windowing). 
            Visualiser les résultats instantanément sur un dashboard interactif.
        </div>
        """
        st.markdown(text_content, unsafe_allow_html=True)
    with cols[2]:
        if latency_sec < 60:
            st.success(f"🟢 LIVE | Latence: {latency_sec:.0f}s")
        else:
            st.error(f"🔴 LAG | Latence: {latency_sec:.0f}s")
        st.caption(f"Update: {last_update}")



def render_photochemical_chart(df, city_name, template, text_color):
    """Graphique Expert : Interaction NO2 / Ozone / UV"""
    st.subheader("Cycle Photochimique")

    if city_name:
        chart_df = df[df['city'] == city_name].sort_values('window_end')
        title_suffix = f" - {city_name}"
    else:
        chart_df = df.groupby('window_end')[['avg_no2', 'max_ozone', 'avg_uv_index']].mean().reset_index().sort_values('window_end')
        title_suffix = " - Vue Globale"
    
    fig = go.Figure()
    
    # Axe Gauche : Gaz (Lignes)
    fig.add_trace(go.Scatter(x=chart_df['window_end'], y=chart_df['avg_no2'], name=f"NO2 (Trafic) {title_suffix}", line=dict(color='#ff7f0e', width=2)))
    fig.add_trace(go.Scatter(x=chart_df['window_end'], y=chart_df['max_ozone'], name=f"Ozone (O3) {title_suffix}", line=dict(color='#2ca02c', width=2)))
    
    # Axe Droite : UV (Barres)
    fig.add_trace(go.Scatter(x=chart_df['window_end'], y=chart_df['avg_uv_index'], name="UV Index", line=dict(color='#1f77b4'), yaxis='y2'))

    fig.update_layout(
        template=template,
        height=380,
        margin=dict(l=0, r=0, t=20, b=0),
        legend=dict(orientation="h", y=1.1),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color=text_color),
        
        # Configuration de l'axe Y principal (Gauche - Gaz)
        yaxis=dict(
            title="Concentration (µg/m³)"
        ),
        
        # Configuration de l'axe Y SECONDAIRE (Droite - UV)
        yaxis2=dict(
            title="Index UV",
            overlaying='y',
            side='right',
            range=[0, 12],
            showgrid=False 
        )
    )
    st.plotly_chart(fig, use_container_width=True)

def render_correlation_matrix(df, template):
    """Matrice de corrélation"""
    st.subheader("Matrice de Corrélation")
    cols_to_corr = [col for col in MEASURE_OPTIONS.values() if col in df.columns]
    
    if len(cols_to_corr) > 1:
        corr = df[cols_to_corr].corr()
        fig = px.imshow(corr, text_auto=".1f", color_continuous_scale="RdBu_r", origin='lower')
        fig.update_layout(
            template=template, height=380, margin=dict(l=0, r=0, t=0, b=0),
            plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig, use_container_width=True)

def render_co2_gauge(value, template, text_color):
    """Jauge CO2"""
    st.subheader("Taux de CO2 Global")
    fig = go.Figure(go.Indicator(
        mode = "gauge+number+delta",
        value = value,
        delta = {'reference': 420, 'increasing': {'color': COLORS['BAD']}},
        gauge = {
            'axis': {'range': [None, 600]},
            'bar': {'color': text_color},
            'steps': [{'range': [0, 420], 'color': COLORS['GOOD']}, {'range': [420, 600], 'color': COLORS['BAD']}],
            'threshold': {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': 420}
        }
    ))
    fig.update_layout(template=template, height=300, margin=dict(l=20, r=20, t=20, b=20), paper_bgcolor='rgba(0,0,0,0)', font=dict(color=text_color))
    st.plotly_chart(fig, use_container_width=True)

def render_evaluation_tab(df, template, text_color):
    """Onglet dédié à la performance technique du pipeline"""
    st.markdown("Performance du Pipeline Streaming")
    
    # 1. Calcul des métriques de Latence sur tout l'historique chargé
    now_utc = datetime.now(timezone.utc).replace(microsecond=0)
    
    # On s'assure que window_end est bien en UTC
    if df['window_end'].dt.tz is None:
        df['window_end'] = df['window_end'].dt.tz_localize(timezone.utc)
    
    # Calcul de la latence pour chaque point (en secondes)
    df['latency_seconds'] = (now_utc - df['window_end']).dt.total_seconds()
    
    # KPI Latence Actuelle
    current_latency = df['latency_seconds'].min() # Le délai le plus court (donnée la plus fraîche)
    avg_latency = df['latency_seconds'].mean()
    
    # --- LIGNE 1 : JAUGES DE SANTÉ ---
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Jauge Latence "Fraîcheur"
        fig_gauge = go.Figure(go.Indicator(
            mode = "gauge+number",
            value = current_latency,
            title = {'text': "Latence Temps Réel (s)"},
            gauge = {
                'axis': {'range': [0, 120]}, # Max 2 min
                'bar': {'color': text_color},
                'steps': [
                    {'range': [0, 60], 'color': COLORS['GOOD']},   # Vert < 1min
                    {'range': [60, 90], 'color': COLORS['MEDIUM']}, # Orange < 1m30
                    {'range': [90, 120], 'color': COLORS['BAD']}    # Rouge > 1m30
                ],
                'threshold': {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': 90}
            }
        ))
        fig_gauge.update_layout(height=250, margin=dict(l=20,r=20,t=30,b=20), paper_bgcolor='rgba(0,0,0,0)', font=dict(color=text_color))
        st.plotly_chart(fig_gauge, use_container_width=True)
        
    with col2:
        # Histogramme de stabilité du débit (Accuracy)
        avg_count = df['record_count'].mean()
        st.metric("Débit Moyen (Events/Window)", f"{avg_count:.1f}", help="Nombre de messages agrégés par fenêtre Flink")
        
        fig_vol = px.bar(df.head(50), x='window_end', y='record_count', title="Complétude des Données (Dernières fenêtres)")
        fig_vol.update_layout(template=template, height=180, margin=dict(l=0,r=0,t=30,b=0), plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font=dict(color=text_color))
        st.plotly_chart(fig_vol, use_container_width=True)

    with col3:
        st.markdown("""
        **Diagnostic Technique :**
        - **Latence :** Différence entre l'heure actuelle et la fin de la fenêtre d'agrégation.
        - **Précision (Accuracy) :** Estimée par le `record_count`. Une chute soudaine indique une perte de paquets Kafka ou un lag Flink.
        - **SLA Cible :** Latence < 60s.
        """)

    st.divider()

    # --- LIGNE 2 : GRAPHIQUES D'ANALYSE ---
    c1, c2 = st.columns(2)
    
    with c1:
        st.subheader("Historique de la Latence")
        # Est-ce que le système "dérive" (accumule du retard) ?
        fig_lat = px.line(df, x='window_end', y='latency_seconds', title="Évolution du Délai de Traitement")
        fig_lat.update_traces(line_color='#FF5733')
        fig_lat.update_layout(template=template, height=350, plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font=dict(color=text_color), yaxis_title="Secondes de retard")
        st.plotly_chart(fig_lat, use_container_width=True)
        
    with c2:
        st.subheader("Stabilité des Capteurs (Boxplot)")
        fig_box = px.box(df, y="avg_pm2_5", x="city", color="city", title="Dispersion des mesures PM2.5")
        fig_box.update_layout(template=template, height=350, plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font=dict(color=text_color))
        st.plotly_chart(fig_box, use_container_width=True)


# --- MAIN DASHBOARD ---
@st.fragment(run_every=5) 
def render_live_dashboard(selected_city, selected_measure_col, measure_label):
    
    df = get_data()
    if df.empty:
        st.warning("En attente de données dans le pipeline...")
        return

    # Configuration Graphique selon le thème sélectionné par l'utilisateur
    # "dark" dans ms.themes signifie que le thème ACTUEL est dark
    is_dark = ms.themes["current_theme"] == "dark"
    chart_template = "plotly_dark" if is_dark else "plotly_white"
    text_color = "white" if is_dark else "black"

    # Prétraitement
    df['window_end'] = pd.to_datetime(df['window_end'])
    latest_df = df.sort_values('window_end').groupby('city').tail(1).reset_index(drop=True)
    
    now_utc = datetime.now(timezone.utc).replace(microsecond=0)
    most_recent = latest_df['window_end'].max().replace(tzinfo=timezone.utc)
    latency = (now_utc - most_recent).total_seconds()
    
    render_header_status(latency, most_recent.strftime('%H:%M:%S'))
    st.divider()

    # Filtrage
    is_global = (selected_city == "Vue Globale (Toutes)")
    
    if not is_global:
        display_df = df[df['city'] == selected_city].copy()
        current_row = latest_df[latest_df['city'] == selected_city].iloc[0]
    else:
        display_df = df.copy()
        current_row = None 

    # === MISE EN PAGE PAR ONGLETS ===
    tab1, tab2, tab3, tab4 = st.tabs(["Vue Générale", "Analyse Scientifique", "Climat & GES", "Evaluation & Performance"])

    # --- ONGLET 1 : MONITORING CLASSIQUE ---
    with tab1:
        kpi_cols = st.columns(4)
        
        # KPI PM2.5
        val_pm25 = current_row['avg_pm2_5'] if not is_global else latest_df['avg_pm2_5'].mean()
        status_txt, status_col = calculate_aqi_status(val_pm25)
        with kpi_cols[0]: render_metric_card("Qualité (PM2.5)", f"{val_pm25:.1f} µg/m³", status_txt, status_col)
        
        # KPI PM10
        val_pm10 = current_row['avg_pm10'] if not is_global else latest_df['avg_pm10'].mean()
        with kpi_cols[1]: render_metric_card("PM10", f"{val_pm10:.1f} µg/m³", "Particules", None)
        
        # KPI NO2
        val_no2 = current_row['avg_no2'] if not is_global else latest_df['avg_no2'].mean()
        with kpi_cols[2]: render_metric_card("NO2 (Trafic)", f"{val_no2:.1f} µg/m³", "Azote", None)
        
        # KPI Ozone
        val_o3 = current_row['max_ozone'] if not is_global else latest_df['max_ozone'].max()
        with kpi_cols[3]: render_metric_card("Ozone (Max)", f"{val_o3:.1f} ppb", "Photochimie", None)

        ## Graphiques
        row2_col1, row2_col2 = st.columns([1, 1])

        # --- COLONNE 1 : LA CARTE DYNAMIQUE ---
        with row2_col1:
            with st.container(border=True):
                st.subheader(f"Carte : {measure_label}")
                
                # Enrichissement des données pour la carte
                latest_df['lat'] = latest_df['city'].apply(lambda x: CITIES_COORDS[x]['lat'])
                latest_df['lon'] = latest_df['city'].apply(lambda x: CITIES_COORDS[x]['lon'])
                
                # Récupération du seuil pour l'affichage
                threshold = THRESHOLDS.get(selected_measure_col, 0)
                latest_df['Seuil Limite'] = threshold  # Pour l'affichage dans le pop-up
                
                # Calcul de la taille des bulles
                val_max = latest_df[selected_measure_col].max()
                if val_max == 0 or pd.isna(val_max): val_max = 1
                latest_df['Taille'] = (latest_df[selected_measure_col] / val_max * 20).clip(lower=5)

                # Définition du Statut pour la couleur
                def get_status(val):
                    return "Critique" if val > threshold else "Normal"
                
                latest_df['Statut'] = latest_df[selected_measure_col].apply(get_status)

                # Création de la Carte Plotly
                map_style = "carto-darkmatter" if is_dark else "carto-positron"
                
                fig_map = px.scatter_mapbox(
                    latest_df,
                    lat="lat", 
                    lon="lon",
                    color="Statut", # La couleur dépend du statut
                    size="Taille",  # La taille dépend de la pollution
                    
                    # Pop-Up
                    hover_name="city",
                    hover_data={
                        selected_measure_col: ":.2f", # Valeur formatée (2 décimales)
                        "Seuil Limite": True,         # Affiche la ligne du seuil
                        "Statut": False,              # On cache les colonnes techniques
                        "Taille": False,
                        "lat": False, 
                        "lon": False
                    },
                    
                    # Couleurs personnalisées
                    color_discrete_map={
                        "Normal": "#198754",  # Vert
                        "Critique": "#dc3545" # Rouge
                    },
                    zoom=5 if is_global else 9,
                    height=380
                )

                # Finitions visuelles
                fig_map.update_layout(
                    mapbox_style=map_style,
                    margin=dict(l=0, r=0, t=0, b=0), # Carte en plein écran dans le conteneur
                    showlegend=True,
                    legend=dict(
                        yanchor="top", y=0.95, xanchor="left", x=0.02,
                        bgcolor="rgba(0,0,0,0)",
                        font=dict(color=text_color)
                    ),
                    paper_bgcolor='rgba(0,0,0,0)',
                )
                
                if not is_global and not latest_df.empty:
                    lat_center = latest_df.iloc[0]['lat']
                    lon_center = latest_df.iloc[0]['lon']
                    fig_map.update_layout(mapbox=dict(center=dict(lat=lat_center, lon=lon_center), zoom=9))

                st.plotly_chart(fig_map, use_container_width=True)

        # --- COLONNE 2 : LE GRAPHIQUE ---
        with row2_col2:
            with st.container(border=True):
                st.subheader(f"Tendance : {measure_label}")
                fig = px.area(
                    display_df, 
                    x='window_end', 
                    y=selected_measure_col,
                    color='city' if is_global else None, 
                    height=380,
                    template=chart_template,
                    color_discrete_map={"Paris": "#85C1E9", "Lyon": "#E74C3C", "Marseille": "#2ECC71"}
                )
                fig.update_layout(
                    xaxis_title=None, 
                    yaxis_title=None, 
                    margin=dict(l=0, r=0, t=10, b=0),
                    legend=dict(orientation="h", y=1.1),
                    plot_bgcolor='rgba(0,0,0,0)', 
                    paper_bgcolor='rgba(0,0,0,0)',
                    font=dict(color=text_color)
                )
                st.plotly_chart(fig, use_container_width=True)

    # --- ONGLET 2 : ANALYSE SCIENTIFIQUE ---
    with tab2:
        sc1, sc2 = st.columns(2)
        with sc1:
            city_to_plot = selected_city if not is_global else None
            # Cycle NO2 vs UV
            render_photochemical_chart(display_df, city_to_plot, chart_template, text_color)
            if is_global: st.caption("Moyenne Globale des Villes")
        with sc2:
            # Corrélation
            render_correlation_matrix(display_df, chart_template)

        st.subheader("Indicateurs Atmosphériques")
        ac1, ac2, ac3 = st.columns(3)
        val_aod = current_row['avg_aod'] if not is_global else latest_df['avg_aod'].mean()
        val_dust = current_row['avg_dust'] if not is_global else latest_df['avg_dust'].mean()
        val_uv = current_row['avg_uv_index'] if not is_global else latest_df['avg_uv_index'].mean()
        
        with ac1: render_metric_card("AOD (Opacité)", f"{val_aod:.3f}", "Optique", None)
        with ac2: render_metric_card("Poussière (Dust)", f"{val_dust:.1f} µg/m³", "Particules", None)
        with ac3: render_metric_card("Index UV", f"{val_uv:.1f}", "Solaire", None)

    # --- ONGLET 3 : CLIMAT ---
    with tab3:
        cl1, cl2 = st.columns([1, 3])
        with cl1:
            val_co2 = current_row['avg_co2'] if not is_global else latest_df['avg_co2'].mean()
            render_co2_gauge(val_co2, chart_template, text_color)
        with cl2:
            st.subheader("Gaz à Effet de Serre (CO2 vs Méthane)")
            fig_ges = px.line(display_df, x='window_end', y=['avg_co2', 'avg_ch4'], title="Evolution temporelle")
            fig_ges.update_layout(template=chart_template, height=350, plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font=dict(color=text_color))
            st.plotly_chart(fig_ges, use_container_width=True)
    
    # --- ONGLET 4 : ÉVALUATION TECHNIQUE ---
    with tab4:
        render_evaluation_tab(df, chart_template, text_color)

    # --- SECTION DE TÉLÉCHARGEMENT ---
    st.divider() # Petit séparateur visuel
    with st.expander("Inspecter & Télécharger les données"):
        
        # Préparation du CSV
        # On encode en 'utf-8-sig' pour que Excel ouvre bien les accents
        csv_data = display_df.to_csv(index=False).encode('utf-8-sig')
        
        # Génération du nom de fichier dynamique
        timestamp_str = datetime.now().strftime("%Y-%m-%d_%Hh%Mm")
        city_clean = selected_city.replace(' ', '_').replace('(', '').replace(')', '')
        file_name = f"air_quality_{city_clean}_{timestamp_str}.csv"
        
        col_dl1, col_dl2 = st.columns([1, 4])
        
        with col_dl1:
            st.download_button(
                label="Télécharger CSV",
                data=csv_data,
                file_name=file_name,
                mime="text/csv",
                help=f"Télécharger les données filtrées pour {selected_city}"
            )
        
        # Affichage du tableau
        st.dataframe(
            display_df.sort_values('window_end', ascending=False).head(50), 
            use_container_width=True,
            hide_index=True
        )


# --- MAIN ---
def main():
    with st.sidebar:
        # --- Toggle Theme ---
        btn_face = ms.themes["light"]["button_face"] if ms.themes["current_theme"] == "light" else ms.themes["dark"]["button_face"]
        is_dark_mode = ms.themes["current_theme"] == "dark"
        st.toggle(btn_face, value=is_dark_mode, on_change=ChangeTheme, help="Thème")
        # ----------------------------------------------
        
        st.header("Contrôles")
        selected_city = st.selectbox("Périmètre géographique", ["Vue Globale (Toutes)"] + ALL_CITIES)
        
        # Nouvelle liste de métriques étendue
        measure_label = st.selectbox("Métrique Principale", list(MEASURE_OPTIONS.keys()))
        selected_measure_col = MEASURE_OPTIONS[measure_label]

        st.markdown("---")
        st.caption("v2.3.0 | Master Project")

    render_live_dashboard(selected_city, selected_measure_col, measure_label)

if __name__ == "__main__":
    main()