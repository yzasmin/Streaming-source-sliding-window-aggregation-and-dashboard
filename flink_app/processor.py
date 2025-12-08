import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # --- CORRECTION CRITIQUE ---
    # On charge explicitement les JARs depuis le volume partagé (usrlib)
    # Cela garantit que le JM et le TM les trouvent tous les deux.
    CURRENT_DIR = "/opt/flink/usrlib"
    jars = [
        f"file://{CURRENT_DIR}/flink-sql-connector-kafka-3.1.0-1.18.jar",
        f"file://{CURRENT_DIR}/flink-connector-jdbc-3.1.2-1.18.jar",
        f"file://{CURRENT_DIR}/postgresql-42.7.3.jar"
    ]
    
    # On combine les chemins avec un point-virgule
    jar_config = ";".join(jars)
    
    print(f"🔧 Chargement des JARs : {jar_config}")
    t_env.get_config().get_configuration().set_string("pipeline.jars", jar_config)

    print("🚀 Initialisation du Job Flink Air Quality...")

    # 2. Source Redpanda
    t_env.execute_sql("""
    CREATE TABLE source_sensor (
        city STRING,
        pm2_5 DOUBLE,
        pm10 DOUBLE,
        ozone DOUBLE,
        timestamp_ingestion BIGINT,

        no2 DOUBLE,
        co DOUBLE,
        co2 DOUBLE,
        so2 DOUBLE,
        aod DOUBLE,
        dust DOUBLE,
        uv_index DOUBLE,
        uv_clear_sky DOUBLE,
        ch4 DOUBLE,
        
        ts AS TO_TIMESTAMP_LTZ(timestamp_ingestion, 3),
        WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'raw-air-quality',
        'properties.bootstrap.servers' = 'redpanda:19092',
        'properties.group.id' = 'flink-group',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
    )
    """)

    # 3. Destination Postgres
    t_env.execute_sql("""
    CREATE TABLE sink_db (
        city STRING,
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        avg_pm2_5 DOUBLE,
        avg_pm10 DOUBLE,
        max_ozone DOUBLE,
        
        avg_no2 DOUBLE,
        avg_co DOUBLE,
        avg_co2 DOUBLE,
        avg_so2 DOUBLE,
        avg_aod DOUBLE,
        avg_dust DOUBLE,
        avg_uv_index DOUBLE,
        avg_uv_clear_sky DOUBLE,
        avg_ch4 DOUBLE,
        
        record_count BIGINT,
        PRIMARY KEY (city, window_end) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/air_quality_db',
        'table-name' = 'air_quality_agg',
        'username' = 'admin',
        'password' = 'password'
    )
    """)

    print("🌊 Soumission du Job...")
    
    # 4. Exécution
    result = t_env.execute_sql("""
    INSERT INTO sink_db
    SELECT
        city,
        window_start,
        window_end,
        AVG(pm2_5) as avg_pm2_5,
        AVG(pm10) as avg_pm10,
        MAX(ozone) as max_ozone,
        
        AVG(no2) as avg_no2,
        AVG(co) as avg_co,
        AVG(co2) as avg_co2,
        AVG(so2) as avg_so2,
        AVG(aod) as avg_aod,
        AVG(dust) as avg_dust,
        AVG(uv_index) as avg_uv_index,
        AVG(uv_clear_sky) as avg_uv_clear_sky,
        AVG(ch4) as avg_ch4,
        
        COUNT(*) as record_count
    FROM TABLE(
        HOP(
            TABLE source_sensor, 
            DESCRIPTOR(ts), 
            INTERVAL '30' SECOND, 
            INTERVAL '2' MINUTE
        )
    )
    GROUP BY city, window_start, window_end
    """)
    
    # On attend le résultat pour éviter que le script ne quitte trop vite
    result.wait()

if __name__ == '__main__':
    main()