"""Resuelve el barrio de CABA a partir de lat/lng, por point-in-polygon
contra el dataset oficial de barrios del GCBA (espacios/data/barrios_caba.geojson,
48 poligonos). Los eventos de Actividades no traen barrio (ver
MODELADO_EVENTOS_PARTE2.md #2, geolocalizacion en vez de zona/barrio), asi que
se deriva de la misma coordenada que ya viene en el evento -- nada de llamar a
un geocoder externo desde la Function."""
import json
from pathlib import Path

_RUTA_GEOJSON = Path(__file__).parent / "data" / "barrios_caba.geojson"

_barrios_cache: list[tuple[str, list]] | None = None


def _cargar_barrios() -> list[tuple[str, list]]:
    global _barrios_cache
    if _barrios_cache is None:
        data = json.loads(_RUTA_GEOJSON.read_text(encoding="utf-8"))
        _barrios_cache = [
            (f["properties"]["nombre"], f["geometry"]["coordinates"])
            for f in data["features"]
        ]
    return _barrios_cache


def _punto_en_anillo(lng: float, lat: float, anillo: list) -> bool:
    """Ray casting estandar: cuenta cruces del rayo horizontal hacia la derecha."""
    dentro = False
    n = len(anillo)
    j = n - 1
    for i in range(n):
        xi, yi = anillo[i]
        xj, yj = anillo[j]
        if (yi > lat) != (yj > lat) and lng < (xj - xi) * (lat - yi) / (yj - yi) + xi:
            dentro = not dentro
        j = i
    return dentro


def _punto_en_poligono(lng: float, lat: float, anillos: list) -> bool:
    """anillos[0] es el perimetro exterior; el resto son huecos (ej. Puerto
    Madero). Un punto esta dentro del poligono si esta dentro de una cantidad
    impar de anillos (regla even-odd, funciona para huecos sin importar el
    sentido de giro)."""
    dentro = False
    for anillo in anillos:
        if _punto_en_anillo(lng, lat, anillo):
            dentro = not dentro
    return dentro


def barrio_desde_coordenadas(lat: float | None, lng: float | None) -> str | None:
    """Nombre del barrio de CABA que contiene (lat, lng), o None si no cae
    dentro de ninguno (fuera de CABA, o coordenada faltante/invalida)."""
    if lat is None or lng is None:
        return None
    try:
        lat, lng = float(lat), float(lng)
    except (TypeError, ValueError):
        return None

    for nombre, anillos in _cargar_barrios():
        if _punto_en_poligono(lng, lat, anillos):
            return nombre
    return None
