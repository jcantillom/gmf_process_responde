from datetime import datetime
from zoneinfo import ZoneInfo


def get_current_colombia_time() -> str:
    """
    Obtiene la hora actual en la zona horaria de Colombia, formateada como 'YYYY-MM-DD HH:MM:SS.mmm'.
    """
    colombia_tz = ZoneInfo("America/Bogota")
    fecha_colombia = datetime.now(colombia_tz)
    return fecha_colombia.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
