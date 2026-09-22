"""Test 1: a que carpeta de bronze va cada evento, segun su eventType.

No toca Azure para nada: resolver_carpeta_y_tipo es una funcion pura
(texto adentro, texto afuera), no hace ninguna llamada de red.
"""
from bronze.bp_data_ingestion_bronze import resolver_carpeta_y_tipo


def test_dominio_conocido_usa_el_nombre_de_carpeta_mapeado():
    carpeta, tipo = resolver_carpeta_y_tipo("com.citypass.reclamos.ReclamoCreado")

    assert carpeta == "Reclamos/ReclamoCreado"
    assert tipo == "ReclamoCreado"


def test_dominio_desconocido_pero_bien_formado_no_se_pierde():
    # "turismo" no esta en el diccionario DOMINIOS, pero el evento tiene las
    # 4 partes esperadas -> se guarda igual, usando la clave tal cual.
    carpeta, tipo = resolver_carpeta_y_tipo("com.citypass.turismo.AlgoNuevo")

    assert carpeta == "turismo/AlgoNuevo"
    assert tipo == "AlgoNuevo"


def test_evento_type_mal_formado_cae_en_otros():
    # Le faltan partes (deberia tener 4 segmentos separados por punto).
    carpeta, tipo = resolver_carpeta_y_tipo("evento_mal_formado")

    assert carpeta == "Otros"
    assert tipo == "evento"
