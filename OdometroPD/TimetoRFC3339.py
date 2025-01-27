from datetime import datetime

def formatear_a_rfc3339(dia, hora):
    """
    Formatea la fecha y hora ingresada al formato RFC 3339.
    
    :param dia: Fecha en formato 'YYYY-MM-DD'
    :param hora: Hora en formato 'HH:MM:SS'
    :return: Fecha y hora en formato RFC 3339
    """
    try:
        # Combinar fecha y hora
        fecha_hora_str = f"{dia} {hora}"
        
        # Convertir a objeto datetime
        fecha_hora = datetime.strptime(fecha_hora_str, "%Y-%m-%d %H:%M:%S")
        
        # Formatear a RFC 3339
        fecha_hora_rfc3339 = fecha_hora.isoformat() + "Z"
        
        return fecha_hora_rfc3339
    except ValueError as e:
        return f"Error al procesar la fecha y hora: {e}"

# Ejemplo de uso
dia = input("Ingresa la fecha (YYYY-MM-DD): ")
hora = input("Ingresa la hora (HH:MM:SS): ")
resultado = formatear_a_rfc3339(dia, hora)
print("Fecha y hora en RFC 3339:", resultado)

#TOMAR EL DIA ANTERIOR
#CREAR TABLA PARA GUARDAR LOS DATOS.
#REGISTROS DE UN MES

#subir la tabla a domo: