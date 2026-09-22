"""Nivel 3: el trigger HTTP completo (ingesta_eventos) mockeando Azure. A
diferencia de los triggers de silver/gold, este no tenia ningun test propio
-- el reporte de cobertura marcaba el cuerpo entero de la funcion (parseo
del body, chequeo de la connection string, subida a bronze) sin cubrir."""
import json
from unittest.mock import patch

import azure.functions as func

import bronze.bp_data_ingestion_bronze as bronze_mod
from tests._fakes_azure import ContenedorBronzeEscribibleFalso, blob_service_client_falso


def _request(body_bytes: bytes) -> func.HttpRequest:
    return func.HttpRequest(
        method="POST",
        url="/api/ingesta_eventos",
        body=body_bytes,
        headers={"Content-Type": "application/json"},
    )


def test_ingesta_guarda_el_evento_en_la_carpeta_del_dominio_correspondiente(monkeypatch):
    monkeypatch.setenv("STORAGE_CONNECTION_STRING", "conexion-de-mentira")
    bronze = ContenedorBronzeEscribibleFalso()
    body = json.dumps({
        "metadata": {"eventType": "com.citypass.reclamos.ReclamoCreado"},
        "data": {"reclamoId": "R-1"},
    }).encode("utf-8")

    with patch.object(
        bronze_mod.BlobServiceClient, "from_connection_string",
        return_value=blob_service_client_falso(bronze=bronze),
    ):
        respuesta = bronze_mod.ingesta_eventos(_request(body))

    assert respuesta.status_code == 201
    assert len(bronze.subidas) == 1
    nombre, contenido = bronze.subidas[0]
    # resolver_carpeta_y_tipo ya se prueba solo en test_ingesta.py; aca lo
    # que importa es que el trigger realmente lo use para armar el nombre.
    assert nombre.startswith("Reclamos/ReclamoCreado/ReclamoCreado_")
    assert json.loads(contenido)["data"]["reclamoId"] == "R-1"


def test_ingesta_devuelve_400_si_el_body_no_es_json_valido(monkeypatch):
    monkeypatch.setenv("STORAGE_CONNECTION_STRING", "conexion-de-mentira")

    respuesta = bronze_mod.ingesta_eventos(_request(b"esto no es json"))

    assert respuesta.status_code == 400


def test_ingesta_devuelve_500_si_falta_la_connection_string(monkeypatch):
    monkeypatch.delenv("STORAGE_CONNECTION_STRING", raising=False)
    body = json.dumps({"metadata": {}, "data": {}}).encode("utf-8")

    respuesta = bronze_mod.ingesta_eventos(_request(body))

    assert respuesta.status_code == 500
