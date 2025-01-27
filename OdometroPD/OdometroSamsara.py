from concurrent.futures import ThreadPoolExecutor
import requests
import pandas as pd
from datetime import datetime, timedelta
import time

# Configuración API
url_base = ""

fecha_inicial = datetime(2024, 12, 24)
fecha_final = datetime.now() - timedelta(days=1)

# Procesar un vehículo
def procesar_vehiculo(vehiculo):
    vehiculo_id = vehiculo['id']
    vehiculo_nombre = vehiculo['name']
    resultados = []
    fecha_actual = fecha_inicial

    while fecha_actual <= fecha_final:
        print(f"Procesando vehículo {vehiculo_nombre} (ID: {vehiculo_id}). Fecha: {fecha_actual.strftime('%Y-%m-%d')}")
        start_time = fecha_actual.strftime("%Y-%m-%dT00:00:01Z")
        end_time = fecha_actual.strftime("%Y-%m-%dT23:59:59Z")
        params = {"startTime": start_time, "endTime": end_time, "types": "obdOdometerMeters", "vehicleIds": vehiculo_id}

        retries = 3
        while retries > 0:
            try:
                response = requests.get(url_base, headers=headers, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()

                obd_datos = data['data'][0]['obdOdometerMeters'] if data.get('data') else []
                if obd_datos:
                    obd_datos.sort(key=lambda x: x['time'])
                    odometro_inicial = obd_datos[0]['value']
                    odometro_final = max(obd_datos, key=lambda x: datetime.strptime(x['time'], "%Y-%m-%dT%H:%M:%SZ"))
                    diferencia_odometro = odometro_final['value'] - odometro_inicial
                    diferencia_odometro_km = diferencia_odometro / 1000

                    resultados.append({
                        'vehiculo_id': vehiculo_id,
                        'vehiculo_nombre': vehiculo_nombre,
                        'odometro_inicial': odometro_inicial,
                        'odometro_final': odometro_final['value'],
                        'diferencia_odometro': diferencia_odometro,
                        'diferencia_odometro_km': diferencia_odometro_km,
                        'fecha': fecha_actual.strftime("%Y-%m-%d")
                    })
                break
            except requests.exceptions.RequestException as e:
                retries -= 1
                print(f"Error: {e}. Intentos restantes: {retries}")
                time.sleep(5)
        fecha_actual += timedelta(days=1)
    return resultados

# Obtener vehículos y procesar en paralelo
try:
    response = requests.get("https://api.samsara.com/fleet/vehicles", headers=headers)
    response.raise_for_status()
    vehiculos = response.json().get('data', [])[:5]  # Limitar a 5 vehículos para pruebas

    print(f"Procesando {len(vehiculos)} vehículos del {fecha_inicial.strftime('%Y-%m-%d')} al {fecha_final.strftime('%Y-%m-%d')}.")

    with ThreadPoolExecutor(max_workers=5) as executor:
        resultados_paralelos = list(executor.map(procesar_vehiculo, vehiculos))

    todos_los_resultados = [resultado for lista in resultados_paralelos for resultado in lista]
    if todos_los_resultados:
        df = pd.DataFrame(todos_los_resultados)
        df.to_excel('resultados_odometro.xlsx', index=False)
        print("Datos exportados a resultados_odometro.xlsx")
    else:
        print("No se generaron resultados para exportar.")
except requests.exceptions.RequestException as e:
    print("Error en la solicitud inicial:", e)
