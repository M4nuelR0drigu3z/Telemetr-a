import requests
import pyodbc
import pandas as pd
import uuid
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()
print("DB_SERVER:", os.getenv("DB_SERVER"))
print("DB_NAME:", os.getenv("DB_NAME"))
print("DB_USER:", os.getenv("DB_USER"))
print("DB_PASSWORD:", os.getenv("DB_PASSWORD"))
print("SAMSARA_API_KEY:", os.getenv("SAMSARA_API_KEY"))

# Configuración de SQL Server
conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={os.getenv('DB_SERVER')};"
    f"DATABASE={os.getenv('DB_NAME')};"
    f"UID={os.getenv('DB_USER')};"
    f"PWD={os.getenv('DB_PASSWORD')};"
    f"TrustServerCertificate=yes;"
    f"Connection Timeout=30;"
)
cursor = conn.cursor()

# API de Samsara
base_url = "https://api.samsara.com/fleet/vehicles/stats/history"
cabecera = {
    "accept": "application/json",
    "authorization": f"Bearer {os.getenv('SAMSARA_API_KEY')}"
}

# Obtener la fecha actual y calcular fechas necesarias
hoy = datetime.utcnow()
fecha_fin = hoy - timedelta(days=1)  # Día anterior
fecha_inicio = fecha_fin - timedelta(days=1)  # Día previo a ese


data_list = []
ultimo_km_registrado = {}

try:
    fecha_actual = fecha_inicio
    unidades = {}

    while fecha_actual <= fecha_fin:
        fecha_str = fecha_actual.strftime("%Y-%m-%d")
        print(f"Obteniendo datos para {fecha_str}...")

        params = {
            "startTime": f"{fecha_str}T00:00:01Z",
            "endTime": f"{fecha_str}T23:59:59Z",
            "types": "gpsOdometerMeters,obdOdometerMeters"
        }
        
        has_next_page = True
        after_cursor = None
        registros = {}

        while has_next_page:
            if after_cursor:
                params["after"] = after_cursor
            
            response = requests.get(base_url, headers=cabecera, params=params)
            response.raise_for_status()
            data = response.json()
            
            for elemento in data.get('data', []):
                vehiculo_id = elemento['id']
                unidad = elemento.get('name', f"Vehículo {vehiculo_id}")
                unidades[vehiculo_id] = unidad
                
                obd_datos = elemento.get('obdOdometerMeters') or []
                gps_datos = elemento.get('gpsOdometerMeters') or []
                
                datos = obd_datos if obd_datos else gps_datos
                origen = "Odómetro" if obd_datos else "GPS"
                
                if datos:
                    datos.sort(key=lambda x: x['time'])
                    odometro_inicial = datos[0]['value']
                    fecha_inicio_dato = datos[0]['time']
                    odometro_final = datos[-1]['value']
                    fecha_fin_dato = datos[-1]['time']
                else:
                    odometro_inicial = odometro_final = fecha_inicio_dato = fecha_fin_dato = 0
                    origen = "Sin datos"
                
                if fecha_actual == fecha_inicio:
                    ultimo_km_registrado[vehiculo_id] = odometro_final
                
                diferencia_km = max(0, (odometro_final - odometro_inicial) / 1000)
                
                registros[vehiculo_id] = (
                    str(uuid.uuid4()), vehiculo_id, unidad, odometro_inicial, 
                    odometro_final, diferencia_km, 0, origen, fecha_inicio_dato, fecha_fin_dato, fecha_str
                )
            
            pagination = data.get("pagination", {})
            has_next_page = pagination.get("hasNextPage", False)
            after_cursor = pagination.get("endCursor")
        
        for vehiculo_id in unidades.keys():
            if vehiculo_id not in registros:
                registros[vehiculo_id] = (
                    str(uuid.uuid4()), vehiculo_id, unidades[vehiculo_id], 0, 0, 0, 0, "Sin datos", 0, 0, fecha_str
                )
        
        data_list.extend(registros.values())
        fecha_actual += timedelta(days=1)
    
    df = pd.DataFrame(data_list, columns=[
        'id', 'IdTracto', 'Unidad', 'DistanciaMetrosInicial', 'DistanciaMetrosFinal', 'DiferenciaKm', 'KmNoRegistrados', 
        'Origen', 'FechaInicio', 'FechaFin', 'Fecha'
    ])

    df['KmNoRegistrados'] = df['KmNoRegistrados'].astype(float)
    df.sort_values(by=['Unidad', 'FechaInicio'], ascending=[True, True], inplace=True)

    for index, row in df.iterrows():
        id_tracto = row['IdTracto']
        if id_tracto in ultimo_km_registrado:
            df.at[index, 'KmNoRegistrados'] = max(0, (row['DistanciaMetrosInicial'] - ultimo_km_registrado[id_tracto]) / 1000)
        
    df.fillna(0, inplace=True)

    archivo_excel = "HistoricoKmSamsara.xlsx"
    df.to_excel(archivo_excel, index=False)
    print(f"Datos guardados en {archivo_excel}")

    # for index, row in df.iterrows():
    #     cursor.execute("""
    #         INSERT INTO HistoricoKmSamsara (
    #             id, IdTracto, Unidad, DistanciaMetrosInicial, DistranciaMetrosFinal, 
    #             DiferenciaKm, KmNoRegistrados, Origen, FechaInicio, FechaFin
    #         ) 
    #         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    #     """, 
    #     row['id'], row['IdTracto'], row['Unidad'], row['DistanciaMetrosInicial'], 
    #     row['DistanciaMetrosFinal'], row['DiferenciaKm'], row['KmNoRegistrados'], 
    #     row['Origen'], row['FechaInicio'], row['FechaFin'])
    
    # conn.commit()
    # print("Datos insertados correctamente en SQL Server")

except Exception as e:
    print("Error:", e)

finally:
    cursor.close()
    conn.close()
