import requests
import xlsxwriter

# URL base de la API y headers de autenticación


# Parámetros iniciales para la primera solicitud


# Lista para almacenar todos los resultados
todos_los_resultados = []

try:
    while True:
        # Realizar la solicitud
        response = requests.get(url_base, headers=headers, params=params)
        response.raise_for_status()  # Verifica si hubo errores en la solicitud

        # Procesar la respuesta en JSON
        datos = response.json()

        # Procesar los datos de la página actual
        for elemento in datos.get('data', []):
            vehiculo_id = elemento['id']
            obd_datos = elemento.get('obdOdometerMeters', [])

            if obd_datos:
                # Ordenar los datos por tiempo
                obd_datos.sort(key=lambda x: x['time'])

                # Obtener el primer y último valor de odómetro
                odometro_inicial = obd_datos[0]['value']
                odometro_final = obd_datos[-1]['value']

                # Calcular la diferencia
                diferencia_odometro = (odometro_final - odometro_inicial) / 1000

                # Agregar el resultado a la lista
                todos_los_resultados.append({
                    'vehiculo_id': vehiculo_id,
                    'odometro_inicial': odometro_inicial,
                    'odometro_final': odometro_final,
                    'diferencia_odometro': diferencia_odometro
                })

        # Verificar si hay una próxima página
        next_page = datos.get('pagination', {}).get('endCursor')
        if not next_page:
            break

        # Actualizar los parámetros para la siguiente solicitud
        params['after'] = next_page

    # Crear un archivo Excel para guardar los resultados
    nombre_archivo = "resultados_odometro.xlsx"
    with xlsxwriter.Workbook(nombre_archivo) as workbook:
        worksheet = workbook.add_worksheet()

        # Escribir encabezados
        encabezados = ["Vehículo ID", "Odómetro Inicial", "Odómetro Final", "Diferencia Odómetro (km)"]
        for col_num, encabezado in enumerate(encabezados):
            worksheet.write(0, col_num, encabezado)

        # Escribir datos
        for row_num, resultado in enumerate(todos_los_resultados, start=1):
            worksheet.write(row_num, 0, resultado['vehiculo_id'])
            worksheet.write(row_num, 1, resultado['odometro_inicial'])
            worksheet.write(row_num, 2, resultado['odometro_final'])
            worksheet.write(row_num, 3, resultado['diferencia_odometro'])

    print(f"Datos guardados en el archivo {nombre_archivo}")

except requests.exceptions.RequestException as e:
    print("Error al realizar la solicitud:", e)
except KeyError as e:
    print(f"Error al procesar los datos: Clave no encontrada - {e}")
