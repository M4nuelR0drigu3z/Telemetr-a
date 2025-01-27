import requests
import xlsxwriter

# Paso 1: Obtener el token de acceso
url_token = ""
headers_token = {
    "Accept": "application/json"
}
body_token = {
    "username": (None, ""),
    "password": (None, ""),
    "grant_type": (None, ""),
    "client_id": (None, ""),
    "client_secret": (None, "")
}

try:
    # Solicitud para obtener el token
    response_token = requests.post(url_token, headers=headers_token, files=body_token)
    response_token.raise_for_status()
    token_data = response_token.json()
    access_token = token_data.get("access_token")

    if not access_token:
        raise ValueError("No se pudo obtener el token de acceso.")

    print("Token obtenido correctamente.")

except requests.exceptions.RequestException as e:
    print("Error al obtener el token de acceso:", e)
    raise
except ValueError as e:
    print("Error en el procesamiento de datos:", e)
    raise

# Paso 2: Utilizar el token para obtener datos del endpoint principal
url_base = "https://api.logitrack.mx/api/v2/units/last_status"
headers = {
    "Accept": "application/json",
    "Authorization": f"Bearer {access_token}"
}

try:
    # Realizar la solicitud
    response = requests.get(url_base, headers=headers)
    response.raise_for_status()

    # Procesar la respuesta en JSON
    datos = response.json()

    # Lista para almacenar resultados
    todos_los_resultados = []

    # Procesar los datos obtenidos
    for unidad in datos:
        todos_los_resultados.append({
            'unit_name': unidad.get('unit_name'),
            'lat': unidad.get('lat'),
            'lon': unidad.get('lon'),
            'address': unidad.get('address'),
            'datetime': unidad.get('datetime'),
            'macross': unidad.get('macross'),
            'speed': unidad.get('speed'),
            'engine_ign': unidad.get('engine_ign'),
            'unit_lock': unidad.get('unit_lock'),
            'geo_in': unidad.get('geo_in'),
            'vo': unidad.get('vo')
        })

    # Crear un archivo Excel para guardar los resultados
    nombre_archivo = "datos_unidades.xlsx"
    with xlsxwriter.Workbook(nombre_archivo) as workbook:
        worksheet = workbook.add_worksheet()

        # Escribir encabezados
        encabezados = [
            "Unit Name", "Lat", "Lon", "Address", "Datetime", "Macross", "Speed", "Engine Ign", "Unit Lock", "Geo In", "VO"
        ]
        for col_num, encabezado in enumerate(encabezados):
            worksheet.write(0, col_num, encabezado)

        # Escribir datos
        for row_num, resultado in enumerate(todos_los_resultados, start=1):
            worksheet.write(row_num, 0, resultado['unit_name'])
            worksheet.write(row_num, 1, resultado['lat'])
            worksheet.write(row_num, 2, resultado['lon'])
            worksheet.write(row_num, 3, resultado['address'])
            worksheet.write(row_num, 4, resultado['datetime'])
            worksheet.write(row_num, 5, resultado['macross'])
            worksheet.write(row_num, 6, resultado['speed'])
            worksheet.write(row_num, 7, resultado['engine_ign'])
            worksheet.write(row_num, 8, resultado['unit_lock'])
            worksheet.write(row_num, 9, resultado['geo_in'])
            worksheet.write(row_num, 10, resultado['vo'])

    print(f"Datos guardados en el archivo {nombre_archivo}")

except requests.exceptions.RequestException as e:
    print("Error al realizar la solicitud:", e)
except KeyError as e:
    print(f"Error al procesar los datos: Clave no encontrada - {e}")
