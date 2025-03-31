import requests
import pandas as pd
import pyodbc
from datetime import datetime,timedelta
from zoneinfo import ZoneInfo

# Disponible en Python 3.9+

def convert_to_mexico(time_str):
    """
    Convierte un string de fecha en formato UTC ("%Y-%m-%dT%H:%M:%SZ")
    a un string con la fecha en zona horaria de México (America/Mexico_City).
    """
    try:
        utc_dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=ZoneInfo("UTC"))
        mexico_dt = utc_dt.astimezone(ZoneInfo("America/Mexico_City"))
        return mexico_dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return time_str

hoy =  datetime.utcnow().date()
ayer = hoy- timedelta(days=1)

# Configuración de los endpoints y encabezados
headers = {
    "accept": "application/json",
    "authorization": ""
}

# Endpoint Safety Events con parámetros ajustados
safety_url = "https://api.samsara.com/fleet/safety-events"
safety_params = {
    "startTime":  f"{ayer}T06:00:01Z",
    "endTime": f"{hoy}T05:59:59Z"
}

# Endpoint Alert Incidents Stream
alerts_url = "https://api.samsara.com/alerts/incidents/stream"
alerts_params = {
    "startTime": f"{ayer}T06:00:01Z",
    "endTime": f"{hoy}T05:59:59Z",
    "configurationIds": "5b26ebdd-48e9-4e1d-b6c5-85eba2c8185f"
}

# Lista para almacenar todos los registros combinados
combined_events = []

# --- Procesar Safety Events ---
has_next = True
while has_next:
    response = requests.get(safety_url, params=safety_params, headers=headers)
    response.raise_for_status()
    data = response.json()
    
    events = data.get("data", [])
    for event in events:
        driver = event.get("driver", {})
        driver_name = driver.get("name", "Desconocido")
        
        vehicle = event.get("vehicle", {})
        vehicle_name = vehicle.get("name", "Sin vehículo")
        event_time = event.get("time", "N/A")
        if event_time != "N/A":
            event_time = convert_to_mexico(event_time)
        
        # Cada etiqueta en behaviorLabels se considera un evento individual
        for label in event.get("behaviorLabels", []):
            event_label = label.get("name", "Sin etiqueta")
            # Filtrar eventos no deseados
            if event_label.strip().lower() in ["following distance", "forward collision warning"]:
                continue
            # Traducción parcial
            if "Vehicle Speed Alert" in event_label:
                event_label = event_label.replace("Vehicle Speed Alert", "Alerta de velocidad del vehículo")
            
            combined_events.append({
                "Origen": "Safety Event",
                "nombre de conductor": driver_name,
                "unidad": vehicle_name,
                "Event Label": event_label,
                "tiempo": event_time,
                "Duración": "",  # No aplica para Safety Events
                "Proyecto Conductor": "",
                "Equipo Colaborativo Conductor": "",
                "Proyecto Vehículo": "",
                "Equipo Colaborativo Vehículo": ""
            })
    
    # Paginación para Safety Events
    pagination = data.get("pagination", {})
    has_next = pagination.get("hasNextPage", False)
    if has_next:
        safety_params["after"] = pagination.get("endCursor")

# --- Procesar Alert Incidents con paginación ---
alerts_has_next = True
while alerts_has_next:
    alerts_response = requests.get(alerts_url, params=alerts_params, headers=headers)
    alerts_response.raise_for_status()
    alerts_data = alerts_response.json()
    alerts_events = alerts_data.get("data", [])
    
    for incident in alerts_events:
        # Se utiliza "happenedAtTime" y "resolvedAtTime" para calcular la duración
        happened_time_str = incident.get("happenedAtTime", None)
        resolved_time_str = incident.get("resolvedAtTime", None)
        if not happened_time_str or not resolved_time_str:
            continue
        
        try:
            happened_time = datetime.strptime(happened_time_str, "%Y-%m-%dT%H:%M:%SZ")
            resolved_time = datetime.strptime(resolved_time_str, "%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            continue
        
        duration_seconds = (resolved_time - happened_time).total_seconds()
        duration_formatted = int(duration_seconds)
        
        for condition in incident.get("conditions", []):
            description = condition.get("description", "")
            if description == "Vehicle Speed":
                speed_details = condition.get("details", {}).get("speed", {})
                vehicle = speed_details.get("vehicle", {})
                vehicle_name = vehicle.get("name", "Sin vehículo")
                
                driver_info = speed_details.get("driver")
                if driver_info:
                    driver_name = driver_info.get("name", "").strip() or "Desconocido"
                else:
                    driver_name = "Desconocido"
                
                current_speed = speed_details.get("currentSpeedKilometersPerHour", "N/A")
                try:
                    current_speed_value = float(current_speed)
                except (ValueError, TypeError):
                    current_speed_value = None
                
                if current_speed_value is None or current_speed_value <= 105:
                    continue
                
                event_label = "Límite de Velocidad Máxima superada"
                mexico_time_str = convert_to_mexico(happened_time_str)
                
                combined_events.append({
                    "Origen": "Alert Incident",
                    "nombre de conductor": driver_name,
                    "unidad": vehicle_name,
                    "Event Label": event_label,
                    "tiempo": mexico_time_str,
                    "Duración": duration_formatted,
                    "Proyecto Conductor": "",
                    "Equipo Colaborativo Conductor": "",
                    "Proyecto Vehículo": "",
                    "Equipo Colaborativo Vehículo": ""
                })
    
    # Paginación para Alert Incidents
    alerts_pagination = alerts_data.get("pagination", {})
    alerts_has_next = alerts_pagination.get("hasNextPage", False)
    if alerts_has_next:
        alerts_params["after"] = alerts_pagination.get("endCursor")

# --- Obtener datos de tags y mapear Proyecto y Equipo Colaborativo ---
tags_response = requests.get("https://api.samsara.com/tags", headers=headers)
tags_response.raise_for_status()
tags_data = tags_response.json()

if isinstance(tags_data, dict) and "data" in tags_data:
    tags_list = tags_data["data"]
else:
    tags_list = tags_data

vehicle_mapping = {}
driver_mapping = {}
for tag in tags_list:
    proyecto = tag.get("name", "Desconocido")
    equipo = tag.get("parentTag", {}).get("name", "Desconocido")
    
    for veh in tag.get("vehicles", []):
        veh_name = veh.get("name", "").strip()
        if veh_name:
            vehicle_mapping[veh_name] = (proyecto, equipo)
            
    for drv in tag.get("drivers", []):
        drv_name = drv.get("name", "").strip()
        if drv_name:
            driver_mapping[drv_name] = (proyecto, equipo)

# Actualizar los registros con la información de Proyecto y Equipo
for event in combined_events:
    veh_name = event.get("unidad", "").strip()
    proyecto_veh, equipo_veh = vehicle_mapping.get(veh_name, ("Desconocido", "Desconocido"))
    event["Proyecto Vehículo"] = proyecto_veh
    event["Equipo Colaborativo Vehículo"] = equipo_veh

    drv_name = event.get("nombre de conductor", "").strip()
    proyecto_drv, equipo_drv = driver_mapping.get(drv_name, ("Desconocido", "Desconocido"))
    event["Proyecto Conductor"] = proyecto_drv
    event["Equipo Colaborativo Conductor"] = equipo_drv

# --- Traducir Event Labels según mapping ---
translation_map = {
    "camera obstruction": "obstrucción de la cámara",
    "crash": "choque",
    "defensive driving": "conducción defensiva",
    "drowsy": "somnolencia",
    "harsh brake": "frenada brusca",
    "harsh turn": "giro brusco",
    "inattentive driving": "conducción inatenta",
    "mobile usage": "uso del móvil"
}

for event in combined_events:
    original_label = event.get("Event Label", "")
    lower_label = original_label.lower().strip()
    if lower_label in translation_map:
        event["Event Label"] = translation_map[lower_label]

# --- Asignar id_evento basado en el Event Label ---
for event in combined_events:
    event_label = event.get("Event Label", "").lower().strip()
    if event_label == "límite de velocidad máxima superada":
        event["id_evento"] = "5b26ebdd-48e9-4e1d-b6c5-85eba2c8185f"
    elif event_label == "somnolencia":
        event["id_evento"] = "7f85857e-7f8c-41e9-bcee-cc633c152931"
    elif event_label in ["frenada brusca", "choque", "giro brusco"]:
        event["id_evento"] = "97360a6f-85ba-4c78-8f87-6ba8a45be21d"
    elif event_label == "obstrucción de la cámara":
        event["id_evento"] = "ac2d9e2f-5c8e-49d0-b534-2d9b38a2ced0"
    else:
        event["id_evento"] = ""

# Reordenar las columnas según lo solicitado (se agregó la coma faltante)
column_order = [
    "Origen",
    "nombre de conductor",
    "unidad",
    "Proyecto Conductor",
    "Equipo Colaborativo Conductor",
    "Proyecto Vehículo",
    "Equipo Colaborativo Vehículo",
    "Event Label",
    "tiempo",
    "Duración",
    "id_evento"
]

df = pd.DataFrame(combined_events, columns=column_order)

# --- Exportar a Excel ---
df.to_excel("Eventos_SemanalA.xlsx", index=False)
print("Reporte guardado en Excel")

# --- Insertar datos en la base de datos SQL ---
# Ajusta la cadena de conexión según tu entorno:
conn_str = (

)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

insert_query = """
INSERT INTO [AplicativosTDR].[dbo].[Eventos_Samsara]
       ([id],
        [origen],
        [Conductor],
        [unidad],
        [Proyecto_Conductor],
        [EC_Conductor],
        [Proyecto_Vehiculo],
        [EC_Vehiculo],
        [Tipo_Evento],
        [Tiempo],
        [Duracion],
        [id_evento])
VALUES (NEWID(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

# Recorrer el DataFrame e insertar fila por fila
for index, row in df.iterrows():
    cursor.execute(
        insert_query,
        row["Origen"],
        row["nombre de conductor"],
        row["unidad"],
        row["Proyecto Conductor"],
        row["Equipo Colaborativo Conductor"],
        row["Proyecto Vehículo"],
        row["Equipo Colaborativo Vehículo"],
        row["Event Label"],
        row["tiempo"],
        row["Duración"],
        row["id_evento"]
    )

conn.commit()
cursor.close()
conn.close()

print("Datos insertados en la base de datos exitosamente")
